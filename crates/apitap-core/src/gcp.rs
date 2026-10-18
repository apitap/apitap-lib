//! Google Cloud service-account auth (JWT-bearer → OAuth2 access token) shared
//! by every Google API apitap talks to — the BigQuery sink and the Google Sheets
//! source use the SAME flow with different scopes. Keys are read once and never
//! logged.

use crate::error::{Error, Result};
use serde_json::Value;

#[derive(serde::Deserialize)]
pub(crate) struct ServiceAccountKey {
    pub client_email: String,
    pub private_key: String,
    #[serde(default = "default_token_uri")]
    pub token_uri: String,
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".to_string()
}

#[derive(serde::Serialize)]
struct JwtClaims {
    iss: String,
    scope: &'static str,
    aud: String,
    iat: u64,
    exp: u64,
}

/// The JWT's `iat`, a minute in the past: a client clock a few seconds ahead
/// of Google's minted a token "in the future" and got invalid_grant (P2).
fn issued_at(now: u64) -> u64 {
    now.saturating_sub(60)
}

/// POST the signed assertion, retrying a transport error, a 5xx or a 429
/// (0.5, 1, 2 s). One attempt was all there was, and the token is re-minted
/// about hourly, so one blip at refresh time aborted an hour-long load or a
/// CDC follow (P2). Other statuses are the answer.
async fn exchange(client: &reqwest::Client, uri: &str, assertion: &str) -> Result<String> {
    let mut last = String::new();
    for attempt in 0..4u32 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(500 << (attempt - 1))).await;
        }
        let sent = client
            .post(uri)
            .timeout(crate::http::control_timeout())
            .form(&[("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"), ("assertion", assertion)])
            .send()
            .await;
        let resp = match sent {
            Ok(r) => r,
            Err(e) => {
                last = format!("google token exchange: {}", e.without_url());
                continue;
            }
        };
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Transfer(format!("google token exchange: body read: {}", e.without_url())))?;
        if status.is_success() {
            return Ok(body);
        }
        last = format!("google token exchange failed ({status}): {}", body.trim());
        if !(status.is_server_error() || status.as_u16() == 429) {
            break;
        }
    }
    Err(Error::Transfer(last))
}

/// Sign a service-account JWT for `scope` and exchange it for a bearer token.
pub(crate) async fn fetch_access_token(
    client: &reqwest::Client,
    credentials_json: &str,
    scope: &'static str,
) -> Result<String> {
    let key: ServiceAccountKey = serde_json::from_str(credentials_json)
        .map_err(|e| Error::InvalidInput(format!("invalid Google service-account JSON: {e}")))?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::Transfer(format!("system clock: {e}")))?
        .as_secs();
    let claims = JwtClaims {
        iss: key.client_email,
        scope,
        aud: key.token_uri.clone(),
        iat: issued_at(now),
        exp: issued_at(now) + 3600,
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(key.private_key.as_bytes())
        .map_err(|e| {
            Error::InvalidInput(format!("invalid Google service-account private_key: {e}"))
        })?;
    let assertion = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        &encoding_key,
    )
    .map_err(|e| Error::Transfer(format!("failed to sign Google JWT: {e}")))?;
    let body = exchange(client, &key.token_uri, &assertion).await?;
    let v: Value = serde_json::from_str(&body)
        .map_err(|e| Error::Transfer(format!("google token response: {e}")))?;
    v["access_token"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| Error::Transfer("google token response missing access_token".into()))
}

/// Resolve the key file: explicit `?credentials=` beats the
/// `GOOGLE_APPLICATION_CREDENTIALS` env var; a clear error names `what` (the
/// URL scheme) when neither is present.
pub(crate) fn read_credentials(path: Option<String>, what: &str) -> Result<String> {
    let path = path
        .or_else(|| std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok())
        .ok_or_else(|| {
            Error::InvalidInput(format!(
                "{what} needs a service-account key: add ?credentials=/path/key.json \
                 to the url or set GOOGLE_APPLICATION_CREDENTIALS"
            ))
        })?;
    std::fs::read_to_string(&path)
        .map_err(|e| Error::InvalidInput(format!("can't read {what} credentials {path}: {e}")))
}

#[cfg(test)]
mod exchange_tests {
    /// P2: a server that accepts and never answers is a timeout, not a hang.
    #[test]
    fn a_silent_token_server_times_out() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = l.local_addr().unwrap().port();
            tokio::spawn(async move {
                let mut held = Vec::new();
                while let Ok((c, _)) = l.accept().await {
                    held.push(c);
                }
            });
            std::env::set_var("APITAP_HTTP_REQUEST_TIMEOUT", "1");
            let c = reqwest::Client::new();
            let r = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                super::exchange(&c, &format!("http://127.0.0.1:{port}/token"), "a"),
            )
            .await;
            std::env::remove_var("APITAP_HTTP_REQUEST_TIMEOUT");
            assert!(r.expect("the exchange must give up by itself").is_err());
        });
    }

    /// P2: the token is back-dated, and a 503 is retried; a 400 is not.
    #[test]
    fn the_token_exchange_retries_a_transient_failure() {
        assert_eq!(super::issued_at(1_000), 940);
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let serve = |answers: Vec<&'static str>| async move {
                let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let port = l.local_addr().unwrap().port();
                let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let h = hits.clone();
                tokio::spawn(async move {
                    for a in answers {
                        let (mut c, _) = l.accept().await.unwrap();
                        let mut b = [0u8; 4096];
                        let _ = c.read(&mut b).await;
                        h.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = c.write_all(a.as_bytes()).await;
                    }
                });
                (format!("http://127.0.0.1:{port}/token"), hits)
            };
            let ok = "HTTP/1.1 200 OK\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"access_token\":\"t\"}";
            let busy = "HTTP/1.1 503 Busy\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let bad = "HTTP/1.1 400 Bad\r\nContent-Length: 13\r\nConnection: close\r\n\r\ninvalid_grant";
            let c = reqwest::Client::new();
            let (uri, hits) = serve(vec![busy, ok]).await;
            assert!(super::exchange(&c, &uri, "a").await.unwrap().contains("access_token"));
            assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
            let (uri, hits) = serve(vec![bad, ok]).await;
            assert!(super::exchange(&c, &uri, "a").await.unwrap_err().to_string().contains("invalid_grant"));
            assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1, "a 400 is the answer");
        });
    }
}
