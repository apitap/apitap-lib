//! Minimal MySQL client for the READ hot path — hand-rolled like the
//! walsender. sqlx's per-row machinery (BinaryRow allocation, async-stream
//! yields, tracing spans, BytesMut refcounts) profiled ~25% of a 0.5-core
//! read; this speaks the binary protocol straight off a buffered socket and
//! hands each row's payload to the direct-Arrow decoders in place.
//!
//! Production coverage: plain TCP and TLS (`ssl-mode=preferred/required`,
//! MySQL semantics — encrypt without certificate verification; `verify_ca`
//! and `verify_identity` ride the sqlx lane, which verifies). Auth =
//! caching_sha2_password (fast path everywhere; FULL auth over TLS sends
//! the password on the encrypted channel) and mysql_native_password, with
//! AuthSwitch both ways. Anything else fails the canary connect and the
//! read rides the sqlx lane instead — never a hard failure.
//!
//! Regular SQL (probe, control queries) stays on sqlx; this type carries
//! the span-SELECT drains — the Arrow read workers and the transfer lanes'
//! raw row pump — plus the binlog replication stream.

use crate::error::{Error, Result};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio_rustls::rustls;

/// Ceiling on one logical MySQL payload, matching the server's own maximum
/// `max_allowed_packet` (1 GB). A single packet is capped by the protocol at
/// 16 MB, but a payload larger than that is sent as a CHAIN of full packets
/// with no count in front of it, so the assembly loop below has no bound of
/// its own: a peer that keeps sending 16 MB continuations grows the buffer
/// until the process dies. This is the bound.
const MAX_PAYLOAD: usize = 1 << 30;


// Capability flags (the subset we speak).
const CLIENT_LONG_PASSWORD: u32 = 0x1;
const CLIENT_CONNECT_WITH_DB: u32 = 0x8;
const CLIENT_PROTOCOL_41: u32 = 0x200;
const CLIENT_SSL: u32 = 0x800;
const CLIENT_SECURE_CONNECTION: u32 = 0x8000;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_PLUGIN_AUTH_LENENC: u32 = 1 << 21;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

/// utf8mb4_general_ci — every 8.x and 5.7 server accepts it.
const CHARSET_UTF8MB4: u8 = 45;

/// Everything the wire rides on — TCP or TLS, behind one vtable (reads are
/// 1 MiB-buffered, so the dynamic dispatch is per syscall, not per row).
trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}
type IoBox = Box<dyn Io>;

/// MySQL's own `ssl-mode` vocabulary, and its own meanings — a URL that works
/// in the `mysql` client should mean the same thing here.
///
/// `REQUIRED` encrypts WITHOUT verifying, which is what MySQL means by it and
/// why every default install (self-signed, auto-generated certificate) works
/// with it. `VERIFY_CA` checks the chain; `VERIFY_IDENTITY` checks the chain
/// AND that the hostname matches the certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SslPref {
    Disabled,
    /// The bool records whether the URL SAID so, which decides whether a
    /// cleartext fallback is worth telling the operator about — see the
    /// walsender's note for the same reasoning.
    Preferred { explicit: bool },
    Required,
    VerifyCa,
    VerifyIdentity,
}

impl SslPref {
    /// Does this mode check the certificate at all?
    fn verifies(self) -> bool {
        matches!(self, SslPref::VerifyCa | SslPref::VerifyIdentity)
    }
}

pub(crate) struct MyWire {
    rd: BufReader<tokio::io::ReadHalf<IoBox>>,
    wr: BufWriter<tokio::io::WriteHalf<IoBox>>,
    seq: u8,
    /// Reused payload buffer — one packet at a time, continuations appended.
    buf: Vec<u8>,
    /// Negotiated: server honors DEPRECATE_EOF (OK-terminated resultsets).
    deprecate_eof: bool,
}

pub(crate) struct MyConnInfo {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db: String,
    ssl: SslPref,
}

/// Reject an ssl mode this client cannot honour, before any socket is opened.
///
/// Every MySQL entry point calls this — the bulk source pool and the CDC
/// control pool as well as the raw plane — because sqlx accepts a mode the
/// raw plane does not, and a URL that works for `mode="replace"` and fails
/// for `mode="log_based"` is a trap laid for whoever reads the URL later.
/// One answer for one string.
pub(crate) fn check_ssl_mode(url: &str) -> Result<()> {
    parse_my_url(url).map(|_| ())
}

/// The host as a CONNECTABLE string.
///
/// `url::Url::host_str()` returns an IPv6 literal in its bracketed URL form —
/// `[2001:db8::1]` — which is right for a URL and wrong for everything else:
/// `TcpStream::connect(("[2001:db8::1]", 5432))` is a DNS lookup of that
/// literal string, and it fails. It also fails as a rustls `ServerName`.
///
/// The brackets come off here, once, so the address that is dialled and the
/// name that TLS verifies are the same thing.
///
/// Note for `verify-full`/`verify_identity` against an IP: rustls turns a bare
/// address into `ServerName::IpAddress`, which requires an iPAddress SAN in
/// the certificate. Most server certificates only carry DNS names, so that
/// combination fails on purpose — connect by hostname, or use a mode that
/// does not verify.
fn connectable_host(h: &str) -> &str {
    h.strip_prefix('[')
        .and_then(|r| r.strip_suffix(']'))
        .unwrap_or(h)
}
pub(crate) fn parse_my_url(url: &str) -> Result<MyConnInfo> {
    let u = reqwest::Url::parse(url)
        .map_err(|e| crate::urlerr::bad_url("mysql url", url, e))?;
    let host = u
        .host_str()
        .map(connectable_host)
        .ok_or_else(|| Error::InvalidInput("mysql url needs a host".into()))?
        .to_string();
    let mut ssl = SslPref::Preferred { explicit: false };
    for (k, v) in u.query_pairs() {
        if k == "ssl-mode" || k == "sslmode" {
            ssl = match v.to_lowercase().replace('-', "_").as_str() {
                "disabled" => SslPref::Disabled,
                "preferred" => SslPref::Preferred { explicit: true },
                "required" => SslPref::Required,
                // rustls's standard verifier checks chain AND hostname
                // together; a chain-only verifier is a custom implementation
                // with its own failure modes. Refused by name rather than
                // silently upgraded to verify_identity (stricter than asked)
                // or downgraded to required (weaker than asked).
                "verify_ca" => {
                    return Err(Error::InvalidInput(
                        "ssl-mode=verify_ca is not implemented on the fast MySQL \
                         plane — it checks the certificate chain while skipping the \
                         hostname, which this client cannot express. Use \
                         verify_identity (chain AND hostname) or required (encrypt \
                         without verifying)."
                            .into(),
                    ))
                }
                "verify_identity" => SslPref::VerifyIdentity,
                other => {
                    return Err(Error::InvalidInput(format!(
                        "ssl-mode={other} is not a MySQL ssl mode — use disabled, \
                         preferred, required, verify_ca or verify_identity"
                    )))
                }
            };
        }
    }
    Ok(MyConnInfo {
        host,
        port: u.port().unwrap_or(3306),
        user: pct(u.username())?,
        password: pct(u.password().unwrap_or(""))?,
        db: u.path().trim_matches('/').to_string(),
        ssl,
    })
}

fn pct(s: &str) -> Result<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let h = std::str::from_utf8(&b[i + 1..i + 3])
                .ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| Error::InvalidInput("bad percent-escape in url".into()))?;
            out.push(h);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|e| Error::InvalidInput(format!("url not utf-8: {e}")))
}

fn io_err(e: std::io::Error) -> Error {
    Error::Transfer(format!("mysql wire: {e}"))
}

fn desync(what: &str) -> Error {
    Error::Transfer(format!("mysql wire: unexpected {what}"))
}

/// ERR packet body (after the 0xFF tag): 2B code, '#' + 5B sqlstate, message.
fn parse_err(body: &[u8]) -> Error {
    if body.len() < 2 {
        return desync("truncated ERR packet");
    }
    let code = u16::from_le_bytes([body[0], body[1]]);
    let mut msg = &body[2..];
    if msg.first() == Some(&b'#') && msg.len() >= 6 {
        msg = &msg[6..];
    }
    Error::Transfer(format!(
        "mysql [{code}]: {}",
        String::from_utf8_lossy(msg)
    ))
}

/// Length-encoded integer; returns (value, bytes consumed).
pub(crate) fn lenenc(b: &[u8]) -> Result<(u64, usize)> {
    let first = *b.first().ok_or_else(|| desync("empty lenenc"))?;
    Ok(match first {
        0xFB => return Err(desync("NULL lenenc in binary row")),
        0xFC => {
            if b.len() < 3 {
                return Err(desync("short lenenc2"));
            }
            (u16::from_le_bytes([b[1], b[2]]) as u64, 3)
        }
        0xFD => {
            if b.len() < 4 {
                return Err(desync("short lenenc3"));
            }
            (u32::from_le_bytes([b[1], b[2], b[3], 0]) as u64, 4)
        }
        0xFE => {
            if b.len() < 9 {
                return Err(desync("short lenenc8"));
            }
            (u64::from_le_bytes(b[1..9].try_into().unwrap()), 9)
        }
        v => (v as u64, 1),
    })
}

/// caching_sha2_password fast-path scramble:
/// XOR(SHA256(pw), SHA256(SHA256(SHA256(pw)) || nonce)).
fn scramble_sha2(password: &str, nonce: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let h1 = Sha256::digest(password.as_bytes());
    let h2 = Sha256::digest(h1);
    let mut h3 = Sha256::new();
    h3.update(h2);
    h3.update(nonce);
    let h3 = h3.finalize();
    h1.iter().zip(h3.iter()).map(|(a, b)| a ^ b).collect()
}

/// mysql_native_password scramble:
/// XOR(SHA1(pw), SHA1(nonce || SHA1(SHA1(pw)))).
fn scramble_native(password: &str, nonce: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let h1 = Sha1::digest(password.as_bytes());
    let h2 = Sha1::digest(h1);
    let mut h3 = Sha1::new();
    h3.update(nonce);
    h3.update(h2);
    let h3 = h3.finalize();
    h1.iter().zip(h3.iter()).map(|(a, b)| a ^ b).collect()
}

fn scramble(plugin: &str, password: &str, nonce: &[u8]) -> Result<Vec<u8>> {
    match plugin {
        "caching_sha2_password" => Ok(scramble_sha2(password, nonce)),
        "mysql_native_password" => Ok(scramble_native(password, nonce)),
        other => Err(Error::Transfer(format!(
            "raw mysql plane: auth plugin '{other}' not spoken — riding the \
             sqlx lane instead"
        ))),
    }
}

/// The server's initial handshake, parsed.
struct Handshake {
    caps: u32,
    nonce: Vec<u8>,
    plugin: String,
}

fn parse_handshake(p: &[u8]) -> Result<Handshake> {
    if p.first() == Some(&0xFF) {
        return Err(parse_err(&p[1..]));
    }
    if p.first() != Some(&10) {
        return Err(desync("handshake protocol version"));
    }
    let mut pos = 1 + p[1..]
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| desync("handshake server version"))?
        + 1
        + 4;
    if p.len() < pos + 8 + 1 + 2 {
        return Err(desync("short handshake"));
    }
    let mut nonce = Vec::with_capacity(20);
    nonce.extend_from_slice(&p[pos..pos + 8]);
    pos += 8 + 1; // auth-data-1 + filler
    let cap_low = u16::from_le_bytes([p[pos], p[pos + 1]]) as u32;
    pos += 2;
    let mut caps = cap_low;
    let mut plugin = String::new();
    if p.len() > pos + 1 + 2 {
        pos += 1 + 2; // charset + status
        let cap_high = u16::from_le_bytes([p[pos], p[pos + 1]]) as u32;
        caps |= cap_high << 16;
        pos += 2;
        let auth_len = p[pos] as usize;
        pos += 1 + 10; // auth-data length + reserved
        if caps & CLIENT_SECURE_CONNECTION != 0 {
            let n = auth_len.saturating_sub(8).max(13) - 1; // drop trailing NUL
            if p.len() < pos + n {
                return Err(desync("short handshake auth data"));
            }
            nonce.extend_from_slice(&p[pos..pos + n]);
            pos += n + 1;
        }
        if caps & CLIENT_PLUGIN_AUTH != 0 {
            let end = p[pos..]
                .iter()
                .position(|&b| b == 0)
                .map(|e| pos + e)
                .unwrap_or(p.len());
            plugin = String::from_utf8_lossy(&p[pos..end]).into_owned();
        }
    }
    if plugin.is_empty() {
        plugin = "mysql_native_password".into();
    }
    Ok(Handshake { caps, nonce, plugin })
}

/// Accept-anything verifier: MySQL `ssl-mode=required` semantics — the
/// channel is encrypted, the certificate is NOT verified (server certs are
/// auto-generated self-signed in the default install). Users who want
/// verification say `verify_ca`/`verify_identity` and get the sqlx lane.
#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

async fn tls_upgrade(tcp: TcpStream, host: &str, ssl: SslPref) -> Result<IoBox> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Transfer(format!("mysql tls: {e}")))?;
    let cfg = if ssl.verifies() {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        builder.with_root_certificates(roots).with_no_client_auth()
    } else {
        // MySQL's `required` means encrypt, do not verify — the same as
        // libpq's, and the reason a default install with its auto-generated
        // self-signed certificate connects at all.
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
            .with_no_client_auth()
    };
    let name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| Error::Transfer(format!("mysql tls servername: {e}")))?;
    let tls = tokio_rustls::TlsConnector::from(Arc::new(cfg))
        .connect(name, tcp)
        .await
        .map_err(io_err)?;
    Ok(Box::new(tls))
}

/// One packet read straight off the raw TCP socket (pre-TLS phase only).
/// Same door as the walsender's, for the same reason.
#[cfg(test)]
pub(super) async fn read_packet_raw_for_test(
    tcp: &mut TcpStream,
    seq: &mut u8,
) -> Result<Vec<u8>> {
    read_packet_raw(tcp, seq).await
}

async fn read_packet_raw(tcp: &mut TcpStream, seq: &mut u8) -> Result<Vec<u8>> {
    let mut head = [0u8; 4];
    tcp.read_exact(&mut head).await.map_err(io_err)?;
    let len = u32::from_le_bytes([head[0], head[1], head[2], 0]) as usize;
    if head[3] != *seq {
        return Err(desync("packet sequence"));
    }
    *seq = seq.wrapping_add(1);
    let mut body = vec![0u8; len];
    tcp.read_exact(&mut body).await.map_err(io_err)?;
    Ok(body)
}

impl MyWire {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let info = parse_my_url(url)?;
        tokio::time::timeout(std::time::Duration::from_secs(10), Self::connect_inner(&info))
            .await
            .map_err(|_| Error::Connect("mysql wire: connect timed out (10s)".into()))?
    }

    async fn connect_inner(info: &MyConnInfo) -> Result<Self> {
        let mut tcp = TcpStream::connect((info.host.as_str(), info.port))
            .await
            .map_err(io_err)?;
        tcp.set_nodelay(true).map_err(io_err)?;

        let mut seq = 0u8;
        let hs = parse_handshake(&read_packet_raw(&mut tcp, &mut seq).await?)?;

        let use_tls = match info.ssl {
            SslPref::Disabled => false,
            SslPref::Preferred { explicit } => {
                let up = hs.caps & CLIENT_SSL != 0;
                if !up && explicit {
                    // Said once per process: a URL that asked for TLS and did
                    // not get it is worth one line; a URL that never mentioned
                    // ssl has no belief to correct, and a security note on
                    // every run teaches people to skip security notes.
                    static SAID: std::sync::Once = std::sync::Once::new();
                    let (h, p) = (info.host.clone(), info.port);
                    SAID.call_once(|| {
                        crate::progress::warn(&format!(
                            "mysql {h}:{p} offers no TLS and ssl-mode=preferred permits \
                             cleartext — this connection is NOT encrypted. \
                             ssl-mode=required makes that a failure instead."
                        ));
                    });
                }
                up
            }
            SslPref::Required | SslPref::VerifyCa | SslPref::VerifyIdentity => {
                if hs.caps & CLIENT_SSL == 0 {
                    return Err(Error::Connect(
                        "mysql wire: ssl-mode requires TLS but the server offers none \
                         — check have_ssl/require_secure_transport on the server"
                            .into(),
                    ));
                }
                true
            }
        };

        let want = CLIENT_LONG_PASSWORD
            | CLIENT_CONNECT_WITH_DB
            | CLIENT_PROTOCOL_41
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH
            | CLIENT_PLUGIN_AUTH_LENENC
            | CLIENT_DEPRECATE_EOF;
        let mut use_caps = want & (hs.caps | CLIENT_LONG_PASSWORD);
        if use_caps & CLIENT_PROTOCOL_41 == 0 {
            return Err(desync("server without PROTOCOL_41"));
        }

        let io: IoBox = if use_tls {
            use_caps |= CLIENT_SSL;
            // SSLRequest: the 32-byte HandshakeResponse prefix, then the
            // socket upgrades and the real response continues INSIDE TLS.
            let mut req = vec![32, 0, 0, seq];
            seq = seq.wrapping_add(1);
            req.extend_from_slice(&use_caps.to_le_bytes());
            req.extend_from_slice(&(1u32 << 24).to_le_bytes());
            req.push(CHARSET_UTF8MB4);
            req.extend_from_slice(&[0u8; 23]);
            tcp.write_all(&req).await.map_err(io_err)?;
            tls_upgrade(tcp, &info.host, info.ssl).await?
        } else {
            Box::new(tcp)
        };

        let (r, w) = tokio::io::split(io);
        let mut me = Self {
            rd: BufReader::with_capacity(1 << 20, r),
            wr: BufWriter::new(w),
            seq,
            buf: Vec::with_capacity(64 << 10),
            deprecate_eof: use_caps & CLIENT_DEPRECATE_EOF != 0,
        };
        me.authenticate(info, &hs, use_caps, use_tls).await?;
        // TIMESTAMP columns then arrive as UTC wall time — the same session
        // pin the sqlx pool applies (delivery says utc:true; keep it true).
        me.exec("SET time_zone = '+00:00'").await?;
        Ok(me)
    }

    async fn authenticate(
        &mut self,
        info: &MyConnInfo,
        hs: &Handshake,
        use_caps: u32,
        use_tls: bool,
    ) -> Result<()> {
        let auth = scramble(&hs.plugin, &info.password, &hs.nonce)?;
        let mut hr = Vec::with_capacity(128);
        hr.extend_from_slice(&use_caps.to_le_bytes());
        hr.extend_from_slice(&(1u32 << 24).to_le_bytes()); // max packet
        hr.push(CHARSET_UTF8MB4);
        hr.extend_from_slice(&[0u8; 23]);
        hr.extend_from_slice(info.user.as_bytes());
        hr.push(0);
        hr.push(auth.len() as u8); // lenenc: our scrambles are ≤ 32 bytes
        hr.extend_from_slice(&auth);
        hr.extend_from_slice(info.db.as_bytes());
        hr.push(0);
        hr.extend_from_slice(hs.plugin.as_bytes());
        hr.push(0);
        self.write_packet(&hr).await?;

        // Post-handshake: OK / ERR / AuthSwitch (0xFE) / AuthMoreData (0x01).
        loop {
            let p = self.read_packet().await?.to_vec();
            match p.first() {
                Some(0x00) => return Ok(()),
                Some(0xFF) => return Err(parse_err(&p[1..])),
                Some(0xFE) => {
                    // AuthSwitchRequest: plugin NUL + fresh nonce.
                    let body = &p[1..];
                    let z = body
                        .iter()
                        .position(|&b| b == 0)
                        .ok_or_else(|| desync("auth switch packet"))?;
                    let plugin = String::from_utf8_lossy(&body[..z]).into_owned();
                    let mut nonce = &body[z + 1..];
                    if nonce.last() == Some(&0) {
                        nonce = &nonce[..nonce.len() - 1];
                    }
                    let auth = scramble(&plugin, &info.password, nonce)?;
                    self.write_packet(&auth).await?;
                }
                Some(0x01) => match p.get(1) {
                    // caching_sha2 fast-auth success — OK packet follows.
                    Some(0x03) => continue,
                    // Full auth: over TLS the password rides the encrypted
                    // channel (what mainline clients do). Over plaintext the
                    // RSA exchange is out of scope — sqlx lane.
                    Some(0x04) => {
                        // caching_sha2 FULL auth sends the password itself. On
                        // an UNVERIFIED channel that is a password handed to
                        // whoever answered — encryption without verification
                        // stops a passive listener, not an active one. So the
                        // channel has to have been checked, not merely
                        // encrypted.
                        if use_tls && info.ssl.verifies() {
                            let mut pw = info.password.as_bytes().to_vec();
                            pw.push(0);
                            self.write_packet(&pw).await?;
                        } else if use_tls {
                            return Err(Error::Transfer(
                                "raw mysql plane: this server wants FULL \
                                 caching_sha2 authentication, which sends the \
                                 password itself, and ssl-mode=required encrypts \
                                 WITHOUT verifying the certificate — so there is no \
                                 evidence the peer is the server. Use \
                                 ssl-mode=verify_identity, or let the connection \
                                 ride the sqlx lane (its RSA exchange does not send \
                                 the password in the clear)."
                                    .into(),
                            ));
                        } else {
                            return Err(Error::Transfer(
                                "raw mysql plane: full sha2 auth needs TLS \
                                 (cold auth cache on a plaintext connection) \
                                 — riding the sqlx lane instead"
                                    .into(),
                            ));
                        }
                    }
                    _ => return Err(desync("auth more-data")),
                },
                _ => return Err(desync("auth response")),
            }
        }
    }

    // ---- packet layer -----------------------------------------------------

    /// Read one packet payload into `self.buf` (16MB continuations appended).
    async fn read_packet(&mut self) -> Result<&[u8]> {
        self.buf.clear();
        loop {
            let mut head = [0u8; 4];
            self.rd.read_exact(&mut head).await.map_err(io_err)?;
            let len = u32::from_le_bytes([head[0], head[1], head[2], 0]) as usize;
            if head[3] != self.seq {
                return Err(desync("packet sequence"));
            }
            self.seq = self.seq.wrapping_add(1);
            let start = self.buf.len();
            if start + len > MAX_PAYLOAD {
                return Err(desync("payload past max_allowed_packet"));
            }
            self.buf.resize(start + len, 0);
            self.rd
                .read_exact(&mut self.buf[start..])
                .await
                .map_err(io_err)?;
            if len < 0xFF_FFFF {
                return Ok(&self.buf);
            }
        }
    }

    async fn write_packet(&mut self, payload: &[u8]) -> Result<()> {
        let mut rest = payload;
        loop {
            let n = rest.len().min(0xFF_FFFF);
            let mut head = [0u8; 4];
            head[..3].copy_from_slice(&(n as u32).to_le_bytes()[..3]);
            head[3] = self.seq;
            self.seq = self.seq.wrapping_add(1);
            self.wr.write_all(&head).await.map_err(io_err)?;
            self.wr.write_all(&rest[..n]).await.map_err(io_err)?;
            rest = &rest[n..];
            // A payload of exactly 16MB-1 needs an empty continuation.
            if rest.is_empty() && n < 0xFF_FFFF {
                break;
            }
            if rest.is_empty() && n == 0xFF_FFFF {
                let mut h = [0u8; 4];
                h[3] = self.seq;
                self.seq = self.seq.wrapping_add(1);
                self.wr.write_all(&h).await.map_err(io_err)?;
                break;
            }
        }
        self.wr.flush().await.map_err(io_err)
    }

    // ---- commands ---------------------------------------------------------

    /// COM_QUERY for statements with no resultset (SET ...).
    pub(crate) async fn exec(&mut self, sql: &str) -> Result<()> {
        self.seq = 0;
        let mut cmd = Vec::with_capacity(1 + sql.len());
        cmd.push(0x03);
        cmd.extend_from_slice(sql.as_bytes());
        self.write_packet(&cmd).await?;
        let p = self.read_packet().await?;
        match p.first() {
            Some(0x00) => Ok(()),
            Some(0xFF) => Err(parse_err(&p[1..])),
            _ => Err(desync("exec response")),
        }
    }

    /// COM_STMT_PREPARE → (statement id, column count).
    pub(crate) async fn prepare(&mut self, sql: &str) -> Result<(u32, usize)> {
        self.seq = 0;
        let mut cmd = Vec::with_capacity(1 + sql.len());
        cmd.push(0x16);
        cmd.extend_from_slice(sql.as_bytes());
        self.write_packet(&cmd).await?;
        let p = self.read_packet().await?;
        match p.first() {
            Some(0x00) => {}
            Some(0xFF) => return Err(parse_err(&p[1..])),
            _ => return Err(desync("prepare response")),
        }
        if p.len() < 12 {
            return Err(desync("short prepare-ok"));
        }
        let stmt_id = u32::from_le_bytes(p[1..5].try_into().unwrap());
        let ncols = u16::from_le_bytes([p[5], p[6]]) as usize;
        let nparams = u16::from_le_bytes([p[7], p[8]]) as usize;
        // Param + column definitions (span SELECTs carry no params, but be
        // exact); pre-8.0 servers add an EOF after each block.
        for _ in 0..nparams {
            self.read_packet().await?;
        }
        if nparams > 0 && !self.deprecate_eof {
            self.read_packet().await?;
        }
        for _ in 0..ncols {
            self.read_packet().await?;
        }
        if ncols > 0 && !self.deprecate_eof {
            self.read_packet().await?;
        }
        Ok((stmt_id, ncols))
    }

    /// COM_STMT_EXECUTE (no params) — consumes the column definitions and
    /// leaves the stream positioned at the first row packet.
    pub(crate) async fn execute(&mut self, stmt_id: u32) -> Result<()> {
        self.seq = 0;
        let mut cmd = [0u8; 10];
        cmd[0] = 0x17;
        cmd[1..5].copy_from_slice(&stmt_id.to_le_bytes());
        cmd[5] = 0; // CURSOR_TYPE_NO_CURSOR
        cmd[6..10].copy_from_slice(&1u32.to_le_bytes());
        self.write_packet(&cmd).await?;
        let p = self.read_packet().await?;
        match p.first() {
            Some(0xFF) => return Err(parse_err(&p[1..])),
            Some(0x00) => return Err(desync("resultset-less span SELECT")),
            _ => {}
        }
        let (ncols, _) = lenenc(p)?;
        for _ in 0..ncols {
            self.read_packet().await?;
        }
        if !self.deprecate_eof {
            self.read_packet().await?;
        }
        Ok(())
    }

    /// Next binary row payload, `None` at end of the resultset.
    pub(crate) async fn next_row(&mut self) -> Result<Option<&[u8]>> {
        let deprecate_eof = self.deprecate_eof;
        let p = self.read_packet().await?;
        match p.first() {
            Some(0xFF) => Err(parse_err(&p[1..])),
            // Terminator: OK-with-0xFE-header (8.0, DEPRECATE_EOF) or the
            // legacy EOF packet (≤ 5 bytes). A real row never starts 0xFE —
            // binary rows start 0x00.
            Some(0xFE) if deprecate_eof || p.len() <= 5 => Ok(None),
            Some(0x00) => Ok(Some(p)),
            _ => Err(desync("row packet header")),
        }
    }

    // ---- binlog replication ------------------------------------------------

    /// Become a replica: session dance + COM_BINLOG_DUMP from `file`:`pos`.
    /// TERMINAL — after this the connection only streams binlog events
    /// (control queries need their own connection). `server_id` must be
    /// nonzero and unique per attached replica; a heartbeat is requested so
    /// idle streams still wake the drain loop.
    pub(crate) async fn binlog_dump(
        &mut self,
        server_id: u32,
        file: &str,
        pos: u32,
        heartbeat_secs: u64,
    ) -> Result<()> {
        // Mirror the server's checksum setting — without this the server
        // refuses to stream to a >=5.6 replica when binlog_checksum=CRC32.
        self.exec("SET @master_binlog_checksum = @@global.binlog_checksum")
            .await?;
        let ns = heartbeat_secs.max(1) * 1_000_000_000;
        self.exec(&format!("SET @master_heartbeat_period = {ns}"))
            .await?;
        self.seq = 0;
        let mut cmd = Vec::with_capacity(11 + file.len());
        cmd.push(0x12); // COM_BINLOG_DUMP
        cmd.extend_from_slice(&pos.to_le_bytes());
        cmd.extend_from_slice(&0u16.to_le_bytes()); // flags: 0 = block/stream
        cmd.extend_from_slice(&server_id.to_le_bytes());
        cmd.extend_from_slice(file.as_bytes());
        self.write_packet(&cmd).await
        // From here the sequence id runs continuously (wrapping) for the
        // connection's lifetime — read_packet's check already handles wrap.
    }

    /// Read one packet payload into a FRESH owned buffer (16MB continuations
    /// appended), for the binlog stream only. The query path keeps the reused
    /// `self.buf` + borrowed-slice shape; the binlog drain holds events across
    /// awaits, and the borrow forced mysource to copy every event — this hands
    /// out ownership instead. `read_buf` through a `limit()` fills the spare
    /// capacity WITHOUT zeroing it first (the packet body used to be memset
    /// and then immediately overwritten), and the limit guarantees we can
    /// never consume bytes of the next packet's header.
    async fn read_packet_owned(&mut self) -> Result<bytes::Bytes> {
        use bytes::BufMut;
        let mut out = bytes::BytesMut::new();
        loop {
            let mut head = [0u8; 4];
            self.rd.read_exact(&mut head).await.map_err(io_err)?;
            let len = u32::from_le_bytes([head[0], head[1], head[2], 0]) as usize;
            if head[3] != self.seq {
                return Err(desync("packet sequence"));
            }
            self.seq = self.seq.wrapping_add(1);
            let need = out.len() + len;
            if need > MAX_PAYLOAD {
                return Err(desync("payload past max_allowed_packet"));
            }
            out.reserve(len);
            while out.len() < need {
                let want = need - out.len();
                let n = self
                    .rd
                    .read_buf(&mut (&mut out).limit(want))
                    .await
                    .map_err(io_err)?;
                if n == 0 {
                    return Err(desync("binlog stream closed mid-packet"));
                }
            }
            if len < 0xFF_FFFF {
                return Ok(out.freeze());
            }
        }
    }

    /// Next raw binlog event: the full event bytes (19-byte header + body,
    /// checksum still attached — the decoder strips it per the session's
    /// algorithm). `None` only in non-block mode; ERR packets become errors.
    /// Owned `Bytes`: the drain buffers events across windows, and handing
    /// ownership out deletes the copy-per-event it used to make.
    pub(crate) async fn next_binlog_event(&mut self) -> Result<Option<bytes::Bytes>> {
        let p = self.read_packet_owned().await?;
        match p.first() {
            Some(0x00) => Ok(Some(p.slice(1..))),
            Some(0xFF) => Err(parse_err(&p[1..])),
            Some(0xFE) if p.len() <= 9 => Ok(None),
            _ => Err(desync("binlog stream packet")),
        }
    }

    /// COM_STMT_CLOSE — fire and forget (no server response).
    pub(crate) async fn stmt_close(&mut self, stmt_id: u32) -> Result<()> {
        self.seq = 0;
        let mut cmd = [0u8; 5];
        cmd[0] = 0x19;
        cmd[1..5].copy_from_slice(&stmt_id.to_le_bytes());
        self.write_packet(&cmd).await
    }

    /// COM_QUIT — polite hangup for the canary connection.
    pub(crate) async fn quit(mut self) {
        self.seq = 0;
        let _ = self.write_packet(&[0x01]).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Live binlog-protocol probe — the layout questions the manuals leave
    /// fuzzy (checksum on the FDE and on artificial events, heartbeat
    /// framing, event-header fields) answered by a real server before
    /// mybinlog.rs commits to them:
    ///
    ///   MY_URL=mysql://root:bench@127.0.0.1:3307/bench \
    ///   cargo test -p apitap-core binlog_probe_live -- --ignored --nocapture
    #[test]
    #[ignore]
    fn binlog_probe_live() {
        let url = std::env::var("MY_URL").expect("set MY_URL");
        let name = |t: u8| match t {
            2 => "QUERY",
            4 => "ROTATE",
            15 => "FORMAT_DESCRIPTION",
            16 => "XID",
            19 => "TABLE_MAP",
            27 => "HEARTBEAT",
            30 => "WRITE_ROWS_v2",
            31 => "UPDATE_ROWS_v2",
            32 => "DELETE_ROWS_v2",
            33 => "GTID",
            34 => "ANONYMOUS_GTID",
            35 => "PREVIOUS_GTIDS",
            40 => "TRANSACTION_PAYLOAD",
            41 => "HEARTBEAT_V2",
            _ => "?",
        };
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                // Control connection: coordinates + traffic generation.
                let pool = sqlx::mysql::MySqlPoolOptions::new()
                    .max_connections(2)
                    .connect(&url)
                    .await
                    .expect("control pool");
                let row: (String, u64, String, String, String) =
                    sqlx::query_as("SHOW MASTER STATUS")
                        .fetch_one(&pool)
                        .await
                        .expect("master status");
                let (file, pos) = (row.0, row.1 as u32);
                println!("== coordinates {file}:{pos}");

                let gen = {
                    let pool = pool.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(700)).await;
                        for sql in [
                            "DROP TABLE IF EXISTS bench.cdc_probe",
                            "CREATE TABLE bench.cdc_probe (id INT PRIMARY KEY, v VARCHAR(300), d DECIMAL(12,4), ts TIMESTAMP(6) NULL, b TINYINT(1))",
                            "INSERT INTO bench.cdc_probe VALUES (1, REPEAT('x',260), 1234.5678, '2026-08-05 01:02:03.000004', 1)",
                            "UPDATE bench.cdc_probe SET v='updated', d=NULL WHERE id=1",
                            "DELETE FROM bench.cdc_probe WHERE id=1",
                        ] {
                            sqlx::query(sql).execute(&pool).await.expect(sql);
                        }
                    })
                };

                let mut w = MyWire::connect(&url).await.expect("replica connect");
                w.binlog_dump(0x6170_6901, &file, pos, 2)
                    .await
                    .expect("binlog dump");
                println!("== streaming");

                let end = std::time::Instant::now() + std::time::Duration::from_secs(15);
                let mut n = 0;
                while std::time::Instant::now() < end && n < 80 {
                    let ev = tokio::time::timeout(
                        std::time::Duration::from_secs(6),
                        w.next_binlog_event(),
                    )
                    .await;
                    let e = match ev {
                        Err(_) => {
                            println!("-- 6s idle, no heartbeat");
                            continue;
                        }
                        Ok(r) => match r.expect("event") {
                            Some(e) => e.to_vec(),
                            None => {
                                println!("== EOF");
                                break;
                            }
                        },
                    };
                    n += 1;
                    assert!(e.len() >= 19, "short event {}: {:02x?}", e.len(), e);
                    let ts = u32::from_le_bytes(e[0..4].try_into().unwrap());
                    let typ = e[4];
                    let declared = u32::from_le_bytes(e[9..13].try_into().unwrap());
                    let log_pos = u32::from_le_bytes(e[13..17].try_into().unwrap());
                    let flags = u16::from_le_bytes(e[17..19].try_into().unwrap());
                    println!(
                        "[{n:02}] {:<20} ts={ts} declared={declared} actual={} log_pos={log_pos} flags={flags:#06x} tail={:02x?}",
                        name(typ),
                        e.len(),
                        &e[e.len() - 4..]
                    );
                    if typ == 15 {
                        println!(
                            "     FDE binlog_ver={} header_len={} alg_byte@[len-5]={} (declared==actual? {})",
                            u16::from_le_bytes(e[19..21].try_into().unwrap()),
                            e[19 + 2 + 50 + 4],
                            e[e.len() - 5],
                            declared as usize == e.len()
                        );
                    }
                    if typ == 4 {
                        println!(
                            "     ROTATE artificial={} pos={} name={:?}",
                            ts == 0,
                            u64::from_le_bytes(e[19..27].try_into().unwrap()),
                            String::from_utf8_lossy(&e[27..e.len() - 4])
                        );
                    }
                }
                gen.await.expect("traffic");
                println!("== {n} events seen");
                assert!(n > 0, "no events streamed");
            });
    }

    #[test]
    fn lenenc_all_widths() {
        assert_eq!(lenenc(&[0x2a]).unwrap(), (42, 1));
        assert_eq!(lenenc(&[0xFA]).unwrap(), (250, 1));
        assert_eq!(lenenc(&[0xFC, 0x10, 0x27]).unwrap(), (10_000, 3));
        assert_eq!(lenenc(&[0xFD, 0x40, 0x42, 0x0F]).unwrap(), (1_000_000, 4));
        assert_eq!(
            lenenc(&[0xFE, 0, 0x10, 0xA5, 0xD4, 0xE8, 0, 0, 0]).unwrap(),
            (1_000_000_000_000, 9)
        );
        assert!(lenenc(&[0xFB]).is_err());
        assert!(lenenc(&[]).is_err());
    }

    #[test]
    fn scrambles_are_involutive_xors_of_the_documented_hashes() {
        // The formulas are XORs of hash chains — verify both directions
        // recover the first hash, which is what the server checks.
        let nonce: Vec<u8> = (1..=20).collect();
        let s = scramble_sha2("bench", &nonce);
        let h1 = Sha256::digest(b"bench");
        let h2 = Sha256::digest(h1);
        let mut h3 = Sha256::new();
        h3.update(h2);
        h3.update(&nonce);
        let h3 = h3.finalize();
        let recovered: Vec<u8> = s.iter().zip(h3.iter()).map(|(a, b)| a ^ b).collect();
        assert_eq!(recovered.as_slice(), h1.as_slice());
        assert_eq!(s.len(), 32);

        let s = scramble_native("bench", &nonce);
        assert_eq!(s.len(), 20);
        assert!(scramble_sha2("", &nonce).is_empty());
    }

    #[test]
    fn err_packet_parses_code_and_message() {
        let mut b = vec![0x28, 0x04]; // 1064
        b.extend_from_slice(b"#42000syntax error");
        let e = parse_err(&b);
        let msg = format!("{e}");
        assert!(msg.contains("1064"), "{msg}");
        assert!(msg.contains("syntax error"), "{msg}");
    }

    #[test]
    fn url_parse_decodes_credentials_and_gates_tls() {
        let i = parse_my_url("mysql://ro%40ot:p%23w@db.example:3307/bench?ssl-mode=disabled")
            .unwrap();
        assert_eq!(i.user, "ro@ot");
        assert_eq!(i.password, "p#w");
        assert_eq!(i.port, 3307);
        assert_eq!(i.db, "bench");
        assert!(matches!(i.ssl, SslPref::Disabled));
        // Same for the MySQL client: brackets off, or the connect is a DNS
        // lookup of a literal "[…]" string.
        let i6 = parse_my_url("mysql://u:p@[2001:db8::1]:3307/bench").unwrap();
        assert_eq!((i6.host.as_str(), i6.port), ("2001:db8::1", 3307));
        assert_eq!(parse_my_url("mysql://u:p@h/bench").unwrap().host, "h");
        // required is now spoken natively (encrypt, no verification)…
        let i = parse_my_url("mysql://u:p@h/db?ssl-mode=required").unwrap();
        assert!(matches!(i.ssl, SslPref::Required));
        // …and no ssl-mode means preferred, like the mainline client — but
        // the client knows nobody ASKED, so a cleartext fallback stays quiet.
        let i = parse_my_url("mysql://u:p@h/db").unwrap();
        assert_eq!(i.ssl, SslPref::Preferred { explicit: false });
        let i = parse_my_url("mysql://u:p@h/db?ssl-mode=preferred").unwrap();
        assert_eq!(i.ssl, SslPref::Preferred { explicit: true });
        // verify_identity is spoken natively now: chain AND hostname.
        let i = parse_my_url("mysql://u:p@h/db?ssl-mode=verify_identity").unwrap();
        assert_eq!(i.ssl, SslPref::VerifyIdentity);
        assert!(i.ssl.verifies());
        assert!(!SslPref::Required.verifies());
        // MySQL writes it with an underscore; a hyphen is the same mode.
        assert_eq!(
            parse_my_url("mysql://u:p@h/db?ssl-mode=VERIFY-IDENTITY").unwrap().ssl,
            SslPref::VerifyIdentity
        );
        // verify_ca checks the chain but NOT the hostname, which rustls cannot
        // express — refused by name rather than silently moved either way.
        assert!(parse_my_url("mysql://u:p@h/db?ssl-mode=verify_ca").is_err());
        assert!(parse_my_url("mysql://u:p@h/db?ssl-mode=whatever").is_err());
    }

    #[test]
    fn handshake_parses_caps_nonce_and_plugin() {
        // A minimal HandshakeV10: proto 10, version "8.0.0", thread id,
        // 8B nonce-1, filler, caps low (PROTOCOL_41|SSL|SECURE), charset,
        // status, caps high (PLUGIN_AUTH|DEPRECATE_EOF), auth len 21,
        // 10B reserved, 12B nonce-2 + NUL, plugin name.
        let mut p = vec![10];
        p.extend_from_slice(b"8.0.0\0");
        p.extend_from_slice(&42u32.to_le_bytes());
        p.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 0]);
        let low = (CLIENT_PROTOCOL_41 | CLIENT_SSL | CLIENT_SECURE_CONNECTION) as u16;
        p.extend_from_slice(&low.to_le_bytes());
        p.push(45);
        p.extend_from_slice(&2u16.to_le_bytes());
        let high = ((CLIENT_PLUGIN_AUTH | CLIENT_DEPRECATE_EOF) >> 16) as u16;
        p.extend_from_slice(&high.to_le_bytes());
        p.push(21);
        p.extend_from_slice(&[0u8; 10]);
        p.extend_from_slice(&[9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0]);
        p.extend_from_slice(b"caching_sha2_password\0");
        let hs = parse_handshake(&p).unwrap();
        assert_eq!(hs.nonce, (1..=20).collect::<Vec<u8>>());
        assert_eq!(hs.plugin, "caching_sha2_password");
        assert!(hs.caps & CLIENT_SSL != 0);
        assert!(hs.caps & CLIENT_DEPRECATE_EOF != 0);
    }
}
