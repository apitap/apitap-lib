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
        iat: now,
        exp: now + 3600,
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
    let resp = client
        .post(&key.token_uri)
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", assertion.as_str()),
        ])
        .send()
        .await
        .map_err(|e| Error::Transfer(format!("google token exchange: {e}")))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(Error::Transfer(format!(
            "google token exchange failed ({status}): {}",
            body.trim()
        )));
    }
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
