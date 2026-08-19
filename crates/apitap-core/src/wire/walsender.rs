//! Minimal Postgres WALSENDER client — the replication connection
//! `mode="log_based"` drains. Hand-rolled like the SigV4 and Iceberg-REST
//! layers: mainline drivers can't open `replication=database` sessions, and
//! the protocol surface we need is small and frozen — startup + auth
//! (SCRAM-SHA-256 / md5 / cleartext), simple query (the replication grammar:
//! IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT, START_REPLICATION), and
//! CopyBoth framing (XLogData in, standby-status-update out).
//!
//! Regular SQL (state reads, prechecks) stays on sqlx over normal
//! connections; this type is used ONLY for the walsender session.
//!
//! TLS is terminated here, before the startup message, through the Postgres
//! SSLRequest handshake. `sslmode` means what it means in libpq: `disable`
//! never encrypts, `prefer` tries and says so out loud if the server refuses,
//! `require` encrypts WITHOUT verifying the certificate (libpq's own meaning
//! — most managed instances present a self-signed one), and `verify-full`
//! checks the chain against the bundled trust anchors and the hostname
//! against the certificate. The transport is an enum rather than a boxed
//! trait object so the cleartext read path keeps the concrete call this whole
//! module exists to preserve.

use crate::error::{Error, Result};
use crate::wire::pgoutput::lsn_to_string;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
// rustls comes through tokio-rustls so the two can never disagree on version.
use tokio_rustls::rustls;
use tokio::sync::mpsc;

/// Microseconds between the Unix and Postgres (2000-01-01) epochs.
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

/// The socket rides as SPLIT halves so the CopyBoth read side can move into
/// its own pump task (TCP is full-duplex — standby writes never contend).
/// The pump is the ape-dts daemon shape adopted batch-side: a task whose
/// only job is recv+frame, so the ~50K syscalls/s of a busy walsender run
/// on their own core while decode+collapse consume from a channel.
pub(crate) struct Walsender {
    /// Read half — `None` while the pump task owns it (CopyBoth mode).
    rd: Option<BufReader<PgRead>>,
    wr: BufWriter<PgWrite>,
    /// Set once START_REPLICATION enters CopyBoth mode.
    copying: bool,
    /// COPY OUT reached ReadyForQuery (`copy_out_next` state).
    copy_eof: bool,
    /// OWNED sliding window for the framed COPY plane: consumed prefix
    /// compacts away, refills APPEND (and grow past the target when one
    /// element is larger than the window — the property the fill_buf lease
    /// could not give, where zero-progress re-leases spun forever).
    co_win: Vec<u8>,
    co_win_pos: usize,
    /// COPY OUT frame-scan state, carried across `fill_buf` windows: a
    /// partially-read message header, and how many payload bytes of the
    /// current message remain (to copy for 'd' / to skip for the rest).
    co_head: [u8; 5],
    co_head_len: u8,
    co_tag: u8,
    co_left: usize,
    /// ErrorResponse body accumulates here ('E' payload spans windows too).
    co_err: Vec<u8>,
    pump: Option<PumpHandle>,
}

struct PumpHandle {
    frames: mpsc::Receiver<(u8, bytes::Bytes)>,
    task: tokio::task::JoinHandle<(BufReader<PgRead>, Result<()>)>,
}

/// Close the socket when this type is dropped, whatever path got us here.
///
/// The plaintext transport did this for free: `TcpStream::into_split` gives
/// an `OwnedWriteHalf` that shuts the write side down on drop. The TLS
/// transport does NOT — `tokio::io::split` halves have no `Drop` and share
/// one lock, so the fd survives until BOTH halves go, and the read half lives
/// in the pump task, parked in `read_frame` waiting for a frame that is never
/// coming.
///
/// The visible cost was a replication slot left `active` after an aborted TLS
/// run, so the NEXT run refused it as in use — a failure the plaintext path
/// did not have, which appeared the moment TLS did. Aborting the pump drops
/// the read half; the write half goes with `self`.
impl Drop for Walsender {
    fn drop(&mut self) {
        if let Some(pump) = self.pump.take() {
            pump.task.abort();
        }
    }
}

/// The pump: forward every frame until the consumer hangs up, an error
/// lands, or ReadyForQuery ends the replication conversation. No select!
/// over the read — a frame read is never cancelled mid-way (protocol
/// desync is the documented trap).
async fn pump_frames(
    mut rd: BufReader<PgRead>,
    tx: mpsc::Sender<(u8, bytes::Bytes)>,
) -> (BufReader<PgRead>, Result<()>) {
    loop {
        match read_frame(&mut rd).await {
            Ok((tag, body)) => {
                let done = tag == b'Z';
                if tx.send((tag, body)).await.is_err() {
                    return (rd, Ok(()));
                }
                if done {
                    return (rd, Ok(()));
                }
            }
            Err(e) => return (rd, Err(e)),
        }
    }
}

/// Postgres caps a protocol message at 1 GB (PQ_LARGE_MESSAGE_LIMIT), so any
/// length past it is not a message this side should try to hold. Every place
/// that allocates from a wire-supplied length checks against this.
const MAX_FRAME: usize = 1 << 30;

/// The frame reader, reachable from the torture harness in this crate.
///
/// A test-only door rather than a widened visibility: the reader's contract
/// is with this module, and the harness needs to run the real one — feeding
/// it a socket — without anything about the production path changing shape
/// to accommodate a test.
#[cfg(test)]
pub(super) async fn read_frame_for_test(
    rd: &mut BufReader<PgRead>,
) -> Result<(u8, bytes::Bytes)> {
    read_frame(rd).await
}

async fn read_frame(rd: &mut BufReader<PgRead>) -> Result<(u8, bytes::Bytes)> {
    let mut head = [0u8; 5];
    rd.read_exact(&mut head).await.map_err(io_err)?;
    let len = u32::from_be_bytes(head[1..5].try_into().unwrap()) as usize;
    if len < 4 {
        return Err(Error::Transfer("walsender: bad message length".into()));
    }
    // The length is four bytes off the wire and this line allocates it, so a
    // peer that is not the server it claims to be — a hijacked port, a proxy
    // answering with its own protocol, a corrupted frame — could ask for 4 GB
    // before anything downstream has a chance to reject the message. Postgres
    // itself caps a protocol message at 1 GB (PQ_LARGE_MESSAGE_LIMIT), so
    // anything past that is not a message this side should try to hold.
    if len > MAX_FRAME {
        return Err(Error::Transfer(format!(
            "walsender: message announces {len} bytes, past the 1 GB protocol \
             limit — the peer on this socket is not answering the Postgres \
             frontend protocol"
        )));
    }
    // BytesMut, so every text cell downstream is a refcounted slice of this
    // one allocation instead of getting its own malloc (see pgoutput::Cell).
    let mut body = bytes::BytesMut::zeroed(len - 4);
    rd.read_exact(&mut body).await.map_err(io_err)?;
    Ok((head[0], body.freeze()))
}

/// One event out of the CopyBoth stream.
#[derive(Debug)]
pub(crate) enum WalEvent {
    /// One pgoutput message payload starting at `wal_start`.
    XLogData { wal_start: u64, payload: bytes::Bytes },
    /// Primary keepalive: server's current WAL end and whether it wants an
    /// immediate reply.
    Keepalive { wal_end: u64, reply_requested: bool },
}

/// A parsed simple-query result: rows of text-format columns.
pub(crate) type Rows = Vec<Vec<Option<String>>>;

// ── the transport ───────────────────────────────────────────────────────────
//
// An ENUM, not a boxed trait object. This whole module exists because the
// generic path was too slow — `copy_out_raw` through four future layers
// profiled at ~30% of a 0.5-core budget — so the plaintext read path stays a
// direct call on a concrete `OwnedReadHalf` with one predictable branch in
// front of it. A `Box<dyn AsyncRead>` would put a vtable dispatch on every
// one of ten million reads to buy nothing on the connections that do not use
// TLS, which is most of them.

/// Read half of either transport.
pub(super) enum PgRead {
    Tcp(OwnedReadHalf),
    Tls(tokio::io::ReadHalf<tokio_rustls::client::TlsStream<TcpStream>>),
}

/// Write half of either transport.
enum PgWrite {
    Tcp(OwnedWriteHalf),
    Tls(tokio::io::WriteHalf<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl tokio::io::AsyncRead for PgRead {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            PgRead::Tcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            PgRead::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for PgWrite {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            PgWrite::Tcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            PgWrite::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            PgWrite::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            PgWrite::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            PgWrite::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            PgWrite::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Ask the server to switch the connection to TLS, and do it.
///
/// The Postgres SSLRequest is not part of the message protocol: it is eight
/// raw bytes sent before anything else — a length of 8 and the magic 80877103
/// — answered by ONE byte, `S` (proceed) or `N` (refused). There is no error
/// response and no way to negotiate; a server that answers `N` will never
/// speak TLS on this connection.
/// Returns the upgraded stream, or the ORIGINAL stream back when the server
/// answered `N`. Handing the socket back matters: after a refusal the
/// connection is still perfectly usable for a cleartext startup, which is what
/// `sslmode=prefer` means and what libpq does — opening a second connection
/// would be a wasted round trip and a second entry in the server's log.
async fn tls_upgrade(
    mut stream: TcpStream,
    host: &str,
    verify: bool,
    root_cert: Option<&str>,
) -> Result<std::result::Result<tokio_rustls::client::TlsStream<TcpStream>, TcpStream>> {
    let mut req = [0u8; 8];
    req[..4].copy_from_slice(&8u32.to_be_bytes());
    req[4..].copy_from_slice(&80_877_103u32.to_be_bytes());
    stream.write_all(&req).await.map_err(io_err)?;
    stream.flush().await.map_err(io_err)?;
    let mut answer = [0u8; 1];
    stream.read_exact(&mut answer).await.map_err(io_err)?;
    if answer[0] != b'S' {
        return Ok(Err(stream));
    }
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Transfer(format!("postgres tls: {e}")))?;
    let cfg = if verify {
        let mut roots = rustls::RootCertStore::empty();
        match root_cert {
            // libpq's meaning: naming a root REPLACES the trust store rather
            // than adding to it. A private CA is the whole point of the
            // option, and quietly keeping the public anchors alongside it
            // would verify against a wider set than the user asked for.
            Some(path) => {
                let pem = std::fs::read_to_string(path).map_err(|e| {
                    Error::InvalidInput(format!("sslrootcert {path}: {e}"))
                })?;
                let certs = pem_certs(&pem);
                if certs.is_empty() {
                    return Err(Error::InvalidInput(format!(
                        "sslrootcert {path} contains no CERTIFICATE block — an empty \
                         trust store would verify nothing, so this is refused rather \
                         than accepted"
                    )));
                }
                for c in certs {
                    roots.add(c).map_err(|e| {
                        Error::InvalidInput(format!("sslrootcert {path}: {e}"))
                    })?;
                }
            }
            None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        }
        builder.with_root_certificates(roots).with_no_client_auth()
    } else {
        builder
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(NoVerify(provider)))
            .with_no_client_auth()
    };
    let name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| Error::Transfer(format!("postgres tls servername: {e}")))?;
    let tls = tokio_rustls::TlsConnector::from(std::sync::Arc::new(cfg))
        .connect(name, stream)
        .await
        .map_err(|e| {
            Error::Transfer(format!(
                "postgres tls handshake: {e}{}",
                match (verify, root_cert) {
                    (true, Some(p)) => format!(
                        " — sslmode=verify-full checked the chain against {p} and the \
                         hostname against the certificate. If the CA is right, the \
                         name you connected to is probably not one the certificate \
                         carries."
                    ),
                    (true, None) => " — sslmode=verify-full checks the chain against \
                         the bundled trust anchors and the hostname against the \
                         certificate. For a PRIVATE CA, point sslrootcert= at its \
                         PEM file; sslmode=require encrypts without verifying, which \
                         is a different thing and worth choosing deliberately."
                        .to_string(),
                    _ => String::new(),
                }
            ))
        })?;
    Ok(Ok(tls))
}

/// Accept-anything verifier for `sslmode=require`, which is libpq's own
/// meaning: encrypt, do not verify. Never used for `verify-full`.
#[derive(Debug)]
struct NoVerify(std::sync::Arc<rustls::crypto::CryptoProvider>);

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

/// What the URL asks of the transport, in libpq's own vocabulary.
///
/// The names are libpq's and so are the meanings — a user who writes
/// `sslmode=require` in a psql connection string and in an apitap URL should
/// get the same thing from both. In particular `require` ENCRYPTS BUT DOES
/// NOT VERIFY: that is what libpq has always meant by it, and quietly making
/// it stricter would break connections to the self-signed certificates most
/// managed Postgres instances present, while quietly making it weaker would
/// be a lie. `verify-full` is the one that checks the chain and the hostname.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SslMode {
    /// No TLS at all.
    Disable,
    /// Try TLS; if the server says no, continue in cleartext. libpq's default,
    /// and apitap's — the bool says whether the URL ASKED for it or simply did
    /// not mention ssl, which decides whether a cleartext fallback is worth
    /// telling the operator about. Someone who wrote `sslmode=prefer` wrote
    /// something that reads like "please encrypt" and deserves to know it did
    /// not; someone who wrote nothing at all holds no belief to correct, and a
    /// security note on every run against a local database is noise that
    /// teaches people to ignore security notes.
    Prefer { explicit: bool },
    /// TLS required. Certificate NOT verified (libpq semantics).
    Require,
    /// TLS required, chain verified against the system roots, hostname checked.
    VerifyFull,
}

struct ConnInfo {
    host: String,
    port: u16,
    user: String,
    password: String,
    db: String,
    ssl: SslMode,
    /// `sslrootcert=` — the CA to verify against INSTEAD of the bundled trust
    /// anchors, which is libpq's meaning: naming a root replaces the store,
    /// it does not add to it.
    root_cert: Option<String>,
}

/// Certificates out of a PEM file, without a parser crate.
///
/// A PEM file is base64 between `-----BEGIN CERTIFICATE-----` markers and
/// nothing else, and this reads exactly that. It is deliberately strict: a
/// file that contains something else (a key, a mangled block) yields no
/// certificates and the caller refuses, rather than quietly verifying against
/// an empty store — which would accept everything.
fn pem_certs(pem: &str) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let mut out = Vec::new();
    let mut rest = pem;
    while let Some(i) = rest.find(BEGIN) {
        let after = &rest[i + BEGIN.len()..];
        let Some(j) = after.find(END) else { break };
        let b64: String = after[..j].chars().filter(|c| !c.is_whitespace()).collect();
        if let Some(der) = b64_decode(&b64) {
            out.push(rustls::pki_types::CertificateDer::from(der));
        }
        rest = &after[j + END.len()..];
    }
    out
}

/// Standard base64, no padding assumptions beyond `=`.
fn b64_decode(s: &str) -> Option<Vec<u8>> {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut idx = [255u8; 256];
    for (i, c) in T.iter().enumerate() {
        idx[*c as usize] = i as u8;
    }
    let (mut acc, mut bits, mut out) = (0u32, 0u32, Vec::with_capacity(s.len() * 3 / 4));
    for c in s.bytes() {
        if c == b'=' {
            break;
        }
        let v = idx[c as usize];
        if v == 255 {
            return None;
        }
        acc = (acc << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// Reject an ssl mode this client cannot honour, before any socket is opened.
///
/// Called by the CDC driver: the control pool it opens first is sqlx's, and
/// sqlx accepts modes the replication client does not, so without this the
/// failure surfaces later and describes the wrong problem.
pub(crate) fn check_ssl_mode(url: &str) -> Result<()> {
    parse_url(url).map(|_| ())
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
fn parse_url(url: &str) -> Result<ConnInfo> {
    let u = reqwest::Url::parse(url)
        .map_err(|e| Error::InvalidInput(format!("postgres url: {e}")))?;
    let host = u
        .host_str()
        .map(connectable_host)
        .ok_or_else(|| Error::InvalidInput("postgres url needs a host".into()))?
        .to_string();
    let mut ssl = SslMode::Prefer { explicit: false };
    let mut root_cert = None;
    for (k, v) in u.query_pairs() {
        if k == "sslrootcert" {
            root_cert = Some(v.to_string());
        }
        // BOTH spellings. libpq writes `sslmode`, sqlx accepts `sslmode` and
        // `ssl-mode`, and this project's own MySQL parser accepts both — so a
        // URL written with the hyphen encrypted its sqlx pool and silently
        // ran the replication socket in cleartext, with the "you asked for
        // TLS and did not get it" note suppressed because, as far as this
        // parser could tell, nobody had asked.
        if k == "sslmode" || k == "ssl-mode" {
            ssl = match v.as_ref() {
                "disable" => SslMode::Disable,
                // libpq treats `allow` as "plaintext first, TLS only if that
                // fails". Nobody deploys it deliberately and the distinction
                // buys nothing here, so it maps to the same attempt order as
                // `prefer` rather than to an unsupported-value error.
                "prefer" | "allow" => SslMode::Prefer { explicit: true },
                "require" => SslMode::Require,
                "verify-full" => SslMode::VerifyFull,
                // verify-ca means "check the chain but not the hostname".
                // rustls's standard verifier does both together, and a
                // chain-only verifier is a custom implementation with its own
                // failure modes — so it is refused by name rather than
                // silently upgraded to verify-full or downgraded to require.
                "verify-ca" => {
                    return Err(Error::InvalidInput(
                        "sslmode=verify-ca is not implemented — it verifies the \
                         certificate chain while skipping the hostname check, \
                         which this client cannot express. Use verify-full (chain \
                         AND hostname) or require (encrypt without verifying)."
                            .into(),
                    ))
                }
                other => {
                    return Err(Error::InvalidInput(format!(
                        "sslmode={other} is not a Postgres ssl mode — use disable, \
                         prefer, require or verify-full"
                    )))
                }
            };
        }
    }
    // Client certificates are a different feature (the server must also be
    // configured for them), and silently dropping them means a confusing
    // authentication failure later. Named now.
    for (k, _) in u.query_pairs() {
        if k == "sslcert" || k == "sslkey" {
            return Err(Error::InvalidInput(format!(
                "{k} is not supported on the replication connection — apitap does \
                 not do client-certificate authentication there yet. Remove it and \
                 authenticate with a password, or use mode='replace'/'append', \
                 whose lane does support it."
            )));
        }
    }
    Ok(ConnInfo {
        host,
        port: u.port().unwrap_or(5432),
        user: percent_decode(u.username())?,
        password: percent_decode(u.password().unwrap_or(""))?,
        db: u.path().trim_matches('/').to_string(),
        ssl,
        root_cert,
    })
}

fn percent_decode(s: &str) -> Result<String> {
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

impl Walsender {
    /// Open a `replication=database` session and authenticate.
    ///
    /// The first attempt rides GUCs in the startup `options` field: raising
    /// `logical_decoding_work_mem` keeps a big transaction's ReorderBuffer
    /// off pg_replslot spill files (measured 14.5s → 11.9s on a 500K-row-tx
    /// window). It is PGC_USERSET, so no server config is needed — but if
    /// the server rejects the options for any reason, retry plain rather
    /// than fail a connection that worked fine before this optimization.
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let workmem =
            std::env::var("APITAP_DECODE_WORKMEM").unwrap_or_else(|_| "1GB".to_string());
        if !matches!(workmem.as_str(), "" | "0" | "off") {
            let opts = format!("-c logical_decoding_work_mem={workmem}");
            match Self::connect_with(url, Some(&opts), true).await {
                Ok(ws) => return Ok(ws),
                Err(_) => {} // fall through to a plain startup
            }
        }
        Self::connect_with(url, None, true).await
    }

    /// Plain (non-replication) SQL session on the same hand-rolled stack.
    ///
    /// This is the COPY OUT data plane: sqlx's `copy_out_raw` yields one
    /// refcounted `Bytes` per CopyData message (≈ one per ROW) through four
    /// future layers — a 10M-row read is a 10M-poll storm that profiled at
    /// ~30% of the 0.5-core budget. Here the frames coalesce straight out of
    /// a 1 MiB read buffer into one reused Vec. Plain TCP only — callers
    /// fall back to the sqlx plane when the URL demands TLS.
    pub(crate) async fn connect_sql(url: &str) -> Result<Self> {
        Self::connect_with(url, None, false).await
    }

    async fn connect_with(url: &str, options: Option<&str>, replication: bool) -> Result<Self> {
        let ci = parse_url(url)?;
        let stream = TcpStream::connect((ci.host.as_str(), ci.port))
            .await
            .map_err(|e| Error::Transfer(format!("walsender connect {}:{}: {e}", ci.host, ci.port)))?;
        stream.set_nodelay(true).ok();
        // TLS is negotiated BEFORE the startup message, so it happens here or
        // not at all.
        let (r, w) = match ci.ssl {
            SslMode::Disable => {
                // 8 KiB (the tokio default) means a syscall every few WAL
                // messages; a drain moves millions. Same reasoning as the
                // vendored sqlx socket buffer bump.
                let (r, w) = stream.into_split();
                (PgRead::Tcp(r), PgWrite::Tcp(w))
            }
            SslMode::Prefer { explicit } => match tls_upgrade(stream, &ci.host, false, ci.root_cert.as_deref()).await? {
                Ok(tls) => {
                    let (r, w) = tokio::io::split(tls);
                    (PgRead::Tls(r), PgWrite::Tls(w))
                }
                // The server said no. `prefer` means exactly that this is
                // allowed — but it is also the case a reviewer of a URL would
                // never guess from reading it, so the run says so. Once per
                // process, not once per connection: a `slots=8` run opens
                // nine of these and the ninth copy tells nobody anything.
                Err(plain) => {
                    if explicit {
                        static SAID: std::sync::Once = std::sync::Once::new();
                        let (h, p) = (ci.host.clone(), ci.port);
                        SAID.call_once(|| {
                            crate::progress::warn(&format!(
                                "postgres {h}:{p} refused TLS and sslmode=prefer permits \
                                 cleartext — this connection is NOT encrypted. \
                                 sslmode=require makes that a failure instead."
                            ));
                        });
                    }
                    let (r, w) = plain.into_split();
                    (PgRead::Tcp(r), PgWrite::Tcp(w))
                }
            },
            SslMode::Require | SslMode::VerifyFull => {
                let verify = ci.ssl == SslMode::VerifyFull;
                match tls_upgrade(stream, &ci.host, verify, ci.root_cert.as_deref()).await? {
                    Ok(tls) => {
                        let (r, w) = tokio::io::split(tls);
                        (PgRead::Tls(r), PgWrite::Tls(w))
                    }
                    Err(_) => {
                        return Err(Error::Transfer(format!(
                            "postgres {}:{} answered the SSLRequest with 'N': it is \
                             not built with TLS support, or ssl is off in \
                             postgresql.conf. sslmode={} does not permit a cleartext \
                             fallback.",
                            ci.host,
                            ci.port,
                            if verify { "verify-full" } else { "require" }
                        )));
                    }
                }
            }
        };
        let mut ws = Self {
            rd: Some(BufReader::with_capacity(1 << 20, r)),
            wr: BufWriter::with_capacity(64 << 10, w),
            copying: false,
            copy_eof: false,
            co_win: Vec::new(),
            co_win_pos: 0,
            co_head: [0; 5],
            co_head_len: 0,
            co_tag: 0,
            co_left: 0,
            co_err: Vec::new(),
            pump: None,
        };
        ws.startup(&ci, options, replication).await?;
        Ok(ws)
    }

    async fn startup(
        &mut self,
        ci: &ConnInfo,
        options: Option<&str>,
        replication: bool,
    ) -> Result<()> {
        // StartupMessage: no tag; length + protocol 3.0 + kv pairs.
        let mut body = Vec::with_capacity(128);
        body.extend_from_slice(&196_608u32.to_be_bytes()); // protocol 3.0
        let mut params: Vec<(&str, &str)> = vec![
            ("user", ci.user.as_str()),
            ("database", ci.db.as_str()),
            ("client_encoding", "UTF8"),
            // Pin the session timezone: pgoutput renders timestamptz TEXT in
            // the session's zone, and the apply paths (MySQL especially)
            // depend on the offset being a fixed +00. The SQL plane pins it
            // too — the sqlx pool it replaces does the same in after_connect.
            ("TimeZone", "UTC"),
        ];
        if replication {
            params.push(("replication", "database"));
        }
        if let Some(o) = options {
            params.push(("options", o));
        }
        for (k, v) in params {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let len = (body.len() + 4) as u32;
        self.wr.write_all(&len.to_be_bytes()).await.map_err(io_err)?;
        self.wr.write_all(&body).await.map_err(io_err)?;
        self.wr.flush().await.map_err(io_err)?;

        // Auth conversation, then drain parameters until ReadyForQuery.
        let mut scram: Option<ScramState> = None;
        loop {
            let (tag, msg) = self.read_message().await?;
            match tag {
                b'R' => {
                    // `read_frame` guarantees the length field is at least 4,
                    // which is a length of ZERO body bytes — so these slices
                    // are the peer's word, not a fact. A hijacked port, a
                    // proxy answering with its own protocol, or one corrupt
                    // frame aborted the whole host process here, and this is a
                    // library: the process it aborts is the caller's.
                    let short =
                        || Error::Transfer("walsender: truncated authentication request".into());
                    let code = u32::from_be_bytes(
                        msg.get(0..4).ok_or_else(short)?.try_into().unwrap(),
                    );
                    match code {
                        0 => {} // AuthenticationOk
                        3 => self.send_password(&ci.password).await?, // cleartext
                        5 => {
                            let salt: [u8; 4] =
                                msg.get(4..8).ok_or_else(short)?.try_into().unwrap();
                            self.send_password(&md5_password(&ci.user, &ci.password, salt))
                                .await?;
                        }
                        10 => {
                            // SASL: mechanisms as cstr list. We speak
                            // SCRAM-SHA-256 (no channel binding on plain TCP).
                            let mechs = msg
                                .get(4..)
                                .and_then(|b| std::str::from_utf8(b).ok())
                                .unwrap_or("");
                            if !mechs.contains("SCRAM-SHA-256") {
                                return Err(Error::Transfer(format!(
                                    "walsender: server offers SASL {mechs:?}, only \
                                     SCRAM-SHA-256 is supported"
                                )));
                            }
                            let st = ScramState::start();
                            let first = st.client_first();
                            let mut b = Vec::new();
                            b.extend_from_slice(b"SCRAM-SHA-256\0");
                            b.extend_from_slice(&(first.len() as u32).to_be_bytes());
                            b.extend_from_slice(first.as_bytes());
                            self.send_msg(b'p', &b).await?;
                            scram = Some(st);
                        }
                        11 => {
                            let server_first = std::str::from_utf8(
                                msg.get(4..).ok_or_else(bad_scram)?,
                            )
                                .map_err(|_| bad_scram())?;
                            let st = scram.as_mut().ok_or_else(bad_scram)?;
                            let fin = st.client_final(server_first, &ci.password)?;
                            self.send_msg(b'p', fin.as_bytes()).await?;
                        }
                        12 => {
                            let server_final =
                                std::str::from_utf8(msg.get(4..).ok_or_else(bad_scram)?)
                                    .map_err(|_| bad_scram())?;
                            let st = scram.as_ref().ok_or_else(bad_scram)?;
                            st.verify_server(server_final)?;
                        }
                        other => {
                            return Err(Error::Transfer(format!(
                                "walsender: unsupported auth method {other}"
                            )))
                        }
                    }
                }
                b'S' | b'K' | b'N' => {} // ParameterStatus / BackendKeyData / Notice
                b'Z' => return Ok(()),   // ReadyForQuery
                b'E' => return Err(parse_error(&msg)),
                other => {
                    return Err(Error::Transfer(format!(
                        "walsender startup: unexpected message {:?}",
                        other as char
                    )))
                }
            }
        }
    }

    async fn send_password(&mut self, pw: &str) -> Result<()> {
        let mut b = pw.as_bytes().to_vec();
        b.push(0);
        self.send_msg(b'p', &b).await
    }

    async fn send_msg(&mut self, tag: u8, body: &[u8]) -> Result<()> {
        self.wr.write_all(&[tag]).await.map_err(io_err)?;
        self.wr
            .write_all(&((body.len() + 4) as u32).to_be_bytes())
            .await
            .map_err(io_err)?;
        self.wr.write_all(body).await.map_err(io_err)?;
        self.wr.flush().await.map_err(io_err)?;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<(u8, bytes::Bytes)> {
        let rd = self
            .rd
            .as_mut()
            .expect("direct read while the pump owns the read half");
        read_frame(rd).await
    }

    /// Run one simple-protocol query (the replication grammar included) and
    /// collect text rows. Must not be called while in CopyBoth mode.
    pub(crate) async fn simple_query(&mut self, sql: &str) -> Result<Rows> {
        assert!(!self.copying, "simple_query during CopyBoth");
        let mut b = sql.as_bytes().to_vec();
        b.push(0);
        self.send_msg(b'Q', &b).await?;
        let mut rows = Vec::new();
        let mut err: Option<Error> = None;
        loop {
            let (tag, msg) = self.read_message().await?;
            match tag {
                b'T' => {} // RowDescription — text rows, we index positionally
                b'D' => {
                    // Field count and every field length are the peer's
                    // numbers. Slicing on them directly panics on a truncated
                    // frame, which in a library means the caller's process
                    // dies instead of seeing an error it could handle.
                    let short = || Error::Transfer("walsender: truncated DataRow".into());
                    let n = u16::from_be_bytes(
                        msg.get(0..2).ok_or_else(short)?.try_into().unwrap(),
                    ) as usize;
                    // Capacity is bounded by what the frame could possibly
                    // hold: two bytes per field at the absolute minimum.
                    let mut row = Vec::with_capacity(n.min(msg.len() / 2 + 1));
                    let mut o = 2usize;
                    for _ in 0..n {
                        let l = i32::from_be_bytes(
                            msg.get(o..o + 4).ok_or_else(short)?.try_into().unwrap(),
                        );
                        o += 4;
                        if l < 0 {
                            row.push(None);
                        } else {
                            let l = l as usize;
                            let cell = msg.get(o..o + l).ok_or_else(short)?;
                            row.push(Some(String::from_utf8_lossy(cell).into_owned()));
                            o += l;
                        }
                    }
                    rows.push(row);
                }
                b'C' | b'I' | b'S' | b'N' => {} // CommandComplete/EmptyQuery/ParameterStatus/Notice
                b'E' => err = Some(parse_error(&msg)),
                b'Z' => break,
                b'W' => {
                    // CopyBothResponse — START_REPLICATION accepted.
                    self.copying = true;
                    return Ok(rows);
                }
                other => {
                    return Err(Error::Transfer(format!(
                        "walsender query: unexpected message {:?}",
                        other as char
                    )))
                }
            }
        }
        match err {
            Some(e) => Err(e),
            None => Ok(rows),
        }
    }

    /// Send `COPY ... TO STDOUT` and consume up to the CopyOutResponse.
    /// On a server error the conversation is drained back to ReadyForQuery
    /// so the connection stays usable for the next span.
    pub(crate) async fn copy_out_start(&mut self, sql: &str) -> Result<()> {
        assert!(!self.copying, "copy_out during CopyBoth");
        self.copy_eof = false;
        self.co_head_len = 0;
        self.co_tag = 0;
        self.co_left = 0;
        self.co_err.clear();
        let mut b = sql.as_bytes().to_vec();
        b.push(0);
        self.send_msg(b'Q', &b).await?;
        loop {
            let (tag, msg) = self.read_message().await?;
            match tag {
                b'H' => return Ok(()), // CopyOutResponse
                b'S' | b'N' => {}      // ParameterStatus / Notice
                b'E' => {
                    let e = parse_error(&msg);
                    self.drain_to_ready().await?;
                    return Err(e);
                }
                other => {
                    return Err(Error::Transfer(format!(
                        "copy_out: unexpected message {:?}",
                        other as char
                    )))
                }
            }
        }
    }

    /// Append CopyData payloads into `buf` (cleared first) until it holds at
    /// least `target` bytes or the COPY ends. Returns `false` exactly once:
    /// when the stream is consumed through ReadyForQuery and `buf` is empty.
    ///
    /// The frame scan is SYNCHRONOUS over `fill_buf` windows: one await
    /// refills ~1 MiB, then message headers parse and payload runs copy in
    /// a plain loop. The previous shape (two async `read_exact` per
    /// message ≈ per row) profiled at 23.7% of a 0.5-core read — 20M
    /// future polls per 10M rows was the whole cost.
    pub(crate) async fn copy_out_next(&mut self, buf: &mut Vec<u8>, target: usize) -> Result<bool> {
        buf.clear();
        if self.copy_eof {
            return Ok(false);
        }
        loop {
            let rd = self
                .rd
                .as_mut()
                .expect("copy_out while the pump owns the read half");
            // ONE await per window; everything below is a sync scan.
            let avail = rd.fill_buf().await.map_err(io_err)?;
            if avail.is_empty() {
                return Err(Error::Transfer(
                    "copy_out: connection closed mid-stream".into(),
                ));
            }
            let n = avail.len();
            let mut i = 0usize;
            let mut done = false;
            let mut err: Option<Error> = None;
            // Local copies keep the scan in registers; write back after.
            let mut co_tag = self.co_tag;
            let mut co_left = self.co_left;
            while i < n {
                if co_left > 0 {
                    // Inside a message payload: one run, bounded by the window.
                    let take = (n - i).min(co_left);
                    match co_tag {
                        b'd' => buf.extend_from_slice(&avail[i..i + take]),
                        b'E' => self.co_err.extend_from_slice(&avail[i..i + take]),
                        _ => {} // Z status byte / CopyDone / CommandComplete / …
                    }
                    co_left -= take;
                    i += take;
                    if co_left == 0 {
                        match co_tag {
                            b'Z' => {
                                done = true;
                                break;
                            }
                            b'E' => {
                                err = Some(parse_error(&std::mem::take(&mut self.co_err)));
                                break;
                            }
                            // A filled piece returns; the tail of the window
                            // stays buffered for the next call.
                            b'd' if buf.len() >= target => break,
                            _ => {}
                        }
                    }
                    continue;
                }
                let (tag, len) = if self.co_head_len == 0 && n - i >= 5 {
                    // Common case: the whole header sits in the window.
                    let tag = avail[i];
                    let len =
                        u32::from_be_bytes(avail[i + 1..i + 5].try_into().unwrap()) as usize;
                    i += 5;
                    (tag, len)
                } else {
                    // Straddling header: accumulate across windows.
                    let have = self.co_head_len as usize;
                    let take = (n - i).min(5 - have);
                    self.co_head[have..have + take].copy_from_slice(&avail[i..i + take]);
                    self.co_head_len += take as u8;
                    i += take;
                    if self.co_head_len < 5 {
                        break; // window exhausted mid-header
                    }
                    self.co_head_len = 0;
                    (
                        self.co_head[0],
                        u32::from_be_bytes(self.co_head[1..5].try_into().unwrap()) as usize,
                    )
                };
                if len < 4 {
                    err = Some(Error::Transfer("copy_out: bad message length".into()));
                    break;
                }
                match tag {
                    b'd' | b'Z' | b'E' | b'c' | b'C' | b'S' | b'N' | b'A' => {}
                    other => {
                        err = Some(Error::Transfer(format!(
                            "copy_out: unexpected message {:?}",
                            other as char
                        )));
                        break;
                    }
                }
                co_tag = tag;
                co_left = len - 4;
                // Z carries a status byte and E a body in practice, but a
                // zero-payload one must still terminate, not no-op.
                if co_left == 0 && matches!(tag, b'Z' | b'E') {
                    if tag == b'Z' {
                        done = true;
                    } else {
                        err = Some(parse_error(&[]));
                    }
                    break;
                }
            }
            self.co_tag = co_tag;
            self.co_left = co_left;
            rd.consume(i);
            if let Some(e) = err {
                self.copy_eof = true;
                self.drain_to_ready().await?;
                return Err(e);
            }
            if done {
                self.copy_eof = true;
                return Ok(!buf.is_empty());
            }
            if buf.len() >= target {
                return Ok(true);
            }
        }
    }

    /// The unconsumed owned window.
    pub(crate) fn co_window(&self) -> &[u8] {
        &self.co_win[self.co_win_pos..]
    }

    pub(crate) fn co_advance(&mut self, n: usize) {
        self.co_win_pos += n;
    }

    /// Compact the consumed prefix, then APPEND fresh bytes: first any
    /// leftovers the BufReader holds, else a direct socket read. Always
    /// adds at least one byte or errors — the zero-progress guarantee the
    /// framed consumer relies on.
    pub(crate) async fn co_refill(&mut self) -> Result<()> {
        if self.co_win_pos > 0 {
            let len = self.co_win.len();
            self.co_win.copy_within(self.co_win_pos.., 0);
            self.co_win.truncate(len - self.co_win_pos);
            self.co_win_pos = 0;
        }
        let rd = self
            .rd
            .as_mut()
            .expect("copy_out while the pump owns the read half");
        let buffered_len = {
            let b = rd.buffer();
            if !b.is_empty() {
                self.co_win.extend_from_slice(b);
                b.len()
            } else {
                0
            }
        };
        if buffered_len > 0 {
            rd.consume(buffered_len);
            return Ok(());
        }
        self.co_win.reserve(256 << 10);
        let n = rd
            .get_mut()
            .read_buf(&mut self.co_win)
            .await
            .map_err(io_err)?;
        if n == 0 {
            return Err(Error::Transfer(
                "copy_out: connection closed mid-stream".into(),
            ));
        }
        Ok(())
    }

    /// Handle ONE control message at the head of the owned window (the
    /// framed consumer stopped on a non-'d' header). Returns true when the
    /// COPY conversation is DONE (ReadyForQuery consumed).
    pub(crate) async fn co_control(&mut self) -> Result<bool> {
        loop {
            while self.co_window().len() < 5 {
                self.co_refill().await?;
            }
            let w = self.co_window();
            let tag = w[0];
            let len = u32::from_be_bytes(w[1..5].try_into().unwrap()) as usize;
            if len < 4 {
                return Err(Error::Transfer("copy_out: bad message length".into()));
            }
            // Same ceiling as `read_frame`, and for the same reason: this loop
            // refills until the window holds `len` bytes, so a peer answering
            // with 0xFFFFFFFF grows `co_win` to four gigabytes before anything
            // downstream gets a chance to reject the message.
            if len > MAX_FRAME {
                return Err(Error::Transfer(format!(
                    "copy_out: message announces {len} bytes, past the 1 GB \
                     protocol limit — the peer on this socket is not answering \
                     the Postgres frontend protocol"
                )));
            }
            while self.co_window().len() < 5 + (len - 4) {
                self.co_refill().await?;
            }
            let body_len = len - 4;
            match tag {
                b'Z' => {
                    self.co_advance(5 + body_len);
                    self.copy_eof = true;
                    return Ok(true);
                }
                b'E' => {
                    let body = self.co_window()[5..5 + body_len].to_vec();
                    self.co_advance(5 + body_len);
                    let e = parse_error(&body);
                    self.copy_eof = true;
                    // Drain to ReadyForQuery through the window.
                    loop {
                        while self.co_window().len() < 5 {
                            self.co_refill().await?;
                        }
                        let w = self.co_window();
                        let t = w[0];
                        let l = u32::from_be_bytes(w[1..5].try_into().unwrap()) as usize;
                        // Draining after an error is still the peer's protocol,
                        // and a peer that just sent us an error is exactly the
                        // one worth not trusting with an allocation. The length
                        // also has to be at least 4 or the advance below would
                        // walk backwards through the window.
                        if !(4..=MAX_FRAME).contains(&l) {
                            return Err(e);
                        }
                        while self.co_window().len() < 1 + l {
                            self.co_refill().await?;
                        }
                        self.co_advance(1 + l);
                        if t == b'Z' {
                            return Err(e);
                        }
                    }
                }
                // CopyDone / CommandComplete / ParameterStatus / Notice / …
                b'c' | b'C' | b'S' | b'N' | b'A' => {
                    self.co_advance(5 + body_len);
                    return Ok(false);
                }
                other => {
                    return Err(Error::Transfer(format!(
                        "copy_out: unexpected message {:?}",
                        other as char
                    )))
                }
            }
        }
    }

    async fn drain_to_ready(&mut self) -> Result<()> {
        loop {
            let (tag, _) = self.read_message().await?;
            if tag == b'Z' {
                return Ok(());
            }
        }
    }

    /// `START_REPLICATION SLOT ... LOGICAL ...` — enters CopyBoth mode.
    ///
    /// Tries pgoutput proto v2 with `streaming` first: the server then ships
    /// a big transaction WHILE decoding it (blocks flush every
    /// `logical_decoding_work_mem`), so the client consumes concurrently
    /// with the server's own WAL scan instead of waiting for the full
    /// decode. The threshold is deliberately kept LOW on this path (stream
    /// early = pipeline long). If the server refuses v2 (pre-14), fall back
    /// to v1 with a big work_mem so nothing spills to pg_replslot files.
    ///
    /// `APITAP_PG_BINARY=1` adds `binary 'true'` (PG14+) to the first
    /// attempt: the walsender then ships `send`-format tuples instead of
    /// running every column through its text output function. Those output
    /// functions execute inside the ONE pegged walsender process that is the
    /// measured per-slot ceiling (benchmarks/gcp-cdc-100tables.md Part 7);
    /// with binary on, apitap renders the text itself (wire::pgbindec) on a
    /// core that idles 60%+ in every receipt. Opt-in while the renderer's
    /// type coverage is the common scalar set — an unsupported OID fails the
    /// drain loudly, before anything is applied or confirmed.
    pub(crate) async fn start_replication(
        &mut self,
        slot: &str,
        start_lsn: u64,
        publication: &str,
    ) -> Result<()> {
        let binary = matches!(
            std::env::var("APITAP_PG_BINARY").as_deref(),
            Ok("1") | Ok("true") | Ok("on")
        );
        // The connection already carries this as a startup option, and this
        // SET used to overwrite it with a hard-coded 64MB — so
        // APITAP_DECODE_WORKMEM was inert on the streaming path, while the
        // savepoint refusal told operators to reach for it. Same source of
        // truth in both places now.
        //
        // It is re-issued rather than trusted because the startup `options`
        // attempt falls back to a plain connection when the server rejects
        // it, and a run that quietly lost the setting is how a large
        // transaction starts spilling to pg_replslot.
        let workmem =
            std::env::var("APITAP_DECODE_WORKMEM").unwrap_or_else(|_| "1GB".to_string());
        if !matches!(workmem.as_str(), "" | "0" | "off") {
            self.simple_query(&format!(
                "SET logical_decoding_work_mem = '{}'",
                workmem.replace('\'', "")
            ))
            .await
            .ok();
        }
        if binary {
            let v2b = format!(
                "START_REPLICATION SLOT \"{slot}\" LOGICAL {} (\"proto_version\" '2', \
                 \"publication_names\" '{publication}', \"streaming\" 'true', \
                 \"binary\" 'true')",
                lsn_to_string(start_lsn)
            );
            // A refusal (pre-14 server) falls through to the text attempts —
            // simple_query drains to ReadyForQuery, the session stays usable.
            let _ = self.simple_query(&v2b).await;
        }
        if !self.copying {
            let v2 = format!(
                "START_REPLICATION SLOT \"{slot}\" LOGICAL {} (\"proto_version\" '2', \
                 \"publication_names\" '{publication}', \"streaming\" 'true')",
                lsn_to_string(start_lsn)
            );
            if self.simple_query(&v2).await.is_err() || !self.copying {
                // (already set above from APITAP_DECODE_WORKMEM)
                let v1 = format!(
                    "START_REPLICATION SLOT \"{slot}\" LOGICAL {} (\"proto_version\" '1', \
                     \"publication_names\" '{publication}')",
                    lsn_to_string(start_lsn)
                );
                self.simple_query(&v1).await?;
            }
        }
        if !self.copying {
            return Err(Error::Transfer(
                "walsender: START_REPLICATION did not enter copy mode".into(),
            ));
        }
        // Hand the read half to the pump: from here until stop_replication,
        // frames arrive through the channel while this task decodes.
        let rd = self.rd.take().expect("read half present at copy start");
        let (tx, rx) = mpsc::channel(8192);
        let task = tokio::spawn(pump_frames(rd, tx));
        self.pump = Some(PumpHandle { frames: rx, task });
        Ok(())
    }

    /// Take the pump down and reclaim the read half, surfacing its error.
    async fn join_pump(&mut self, pump: PumpHandle) -> Result<()> {
        let (rd, res) = pump
            .task
            .await
            .map_err(|e| Error::Transfer(format!("walsender pump join: {e}")))?;
        self.rd = Some(rd);
        res
    }

    /// Next CopyBoth event. `None` when the server ended the stream.
    pub(crate) async fn next_event(&mut self) -> Result<Option<WalEvent>> {
        assert!(self.copying, "next_event outside CopyBoth");
        loop {
            let (tag, msg) = match self.pump.as_mut() {
                Some(p) => match p.frames.recv().await {
                    Some(f) => f,
                    None => {
                        // Pump exited without a server CopyDone: an error or
                        // a hangup — join it and tell the truth.
                        let pump = self.pump.take().expect("pump present");
                        self.copying = false;
                        self.join_pump(pump).await?;
                        return Err(Error::Transfer(
                            "walsender: stream ended unexpectedly".into(),
                        ));
                    }
                },
                None => self.read_message().await?,
            };
            match tag {
                b'd' => {
                    // XLogData carries a 25-byte header; a keepalive 18. A
                    // frame shorter than the header it claims is a protocol
                    // error, not a reason to panic on a slice.
                    let short = || Error::Transfer("walsender: truncated CopyData frame".into());
                    match msg.first() {
                        Some(b'w') => {
                            if msg.len() < 25 {
                                return Err(short());
                            }
                            let wal_start =
                                u64::from_be_bytes(msg[1..9].try_into().unwrap());
                            // bytes 9..17 wal_end, 17..25 server clock — unused.
                            // A Bytes slice, so dropping the header is a
                            // pointer bump — the old `drain(..25)` memmoved
                            // every payload at ~1M events/window.
                            let payload = msg.slice(25..);
                            return Ok(Some(WalEvent::XLogData { wal_start, payload }));
                        }
                        Some(b'k') => {
                            if msg.len() < 9 {
                                return Err(short());
                            }
                            let wal_end = u64::from_be_bytes(msg[1..9].try_into().unwrap());
                            let reply_requested = msg.get(17).copied().unwrap_or(0) == 1;
                            return Ok(Some(WalEvent::Keepalive { wal_end, reply_requested }));
                        }
                        _ => {
                            return Err(Error::Transfer(
                                "walsender: unknown CopyData frame".into(),
                            ))
                        }
                    }
                }
                b'c' => {
                    // Server CopyDone: acknowledge and fall out of copy mode.
                    self.copying = false;
                    return Ok(None);
                }
                b'E' => return Err(parse_error(&msg)),
                b'N' => {} // NoticeResponse
                other => {
                    return Err(Error::Transfer(format!(
                        "walsender copy: unexpected message {:?}",
                        other as char
                    )))
                }
            }
        }
    }

    /// Standby status update — reports `lsn` as written/flushed/applied.
    /// This is the ONLY thing that lets Postgres discard slot WAL; callers
    /// send it exactly once per run, after the destination commit.
    pub(crate) async fn standby_status(&mut self, lsn: u64, request_reply: bool) -> Result<()> {
        let now_us = chrono::Utc::now().timestamp_micros() - PG_EPOCH_OFFSET_US;
        let mut b = Vec::with_capacity(35);
        b.push(b'r');
        for _ in 0..3 {
            b.extend_from_slice(&lsn.to_be_bytes());
        }
        b.extend_from_slice(&now_us.to_be_bytes());
        b.push(request_reply as u8);
        self.send_msg(b'd', &b).await
    }

    /// Leave CopyBoth mode cleanly (CopyDone handshake) so the session can
    /// run further simple queries or close gracefully.
    pub(crate) async fn stop_replication(&mut self) -> Result<()> {
        if let Some(mut pump) = self.pump.take() {
            if self.copying {
                self.send_msg(b'c', &[]).await?;
                self.copying = false;
            }
            // Bounded. This waits for the server to finish the CopyBoth
            // conversation, and a path that is half-dead — a NAT or load
            // balancer that reaped the flow without sending an RST — never
            // sends the ReadyForQuery this loop is waiting for. Unbounded, a
            // run that had already failed then parked here indefinitely
            // instead of returning the error it was carrying. The drain is
            // over either way; the only question is how long to be polite
            // about it.
            const DRAIN_SECS: u64 = 30;
            let mut err: Option<Error> = None;
            let drained = tokio::time::timeout(
                std::time::Duration::from_secs(DRAIN_SECS),
                async {
                    loop {
                        match pump.frames.recv().await {
                            Some((b'Z', _)) | None => break,
                            Some((b'E', msg)) => err = Some(parse_error(&msg)),
                            Some(_) => {} // drain in-flight frames
                        }
                    }
                },
            )
            .await;
            if drained.is_err() {
                // Nothing left to salvage from this connection: abort the
                // pump, which drops the read half, and let the socket close.
                pump.task.abort();
                self.copying = false;
                return match err {
                    Some(e) => Err(e),
                    None => Err(Error::Transfer(format!(
                        "walsender: the server did not finish the replication \
                         conversation within {DRAIN_SECS}s — the connection is \
                         half-open (a firewall or load balancer reaped it). The \
                         window's data is unaffected; re-run to continue from the \
                         watermark."
                    ))),
                };
            }
            let res = self.join_pump(pump).await;
            return match err {
                Some(e) => Err(e),
                None => res,
            };
        }
        if !self.copying {
            return Ok(());
        }
        self.send_msg(b'c', &[]).await?;
        loop {
            let (tag, msg) = self.read_message().await?;
            match tag {
                b'd' | b'N' | b'C' | b'c' => {} // drain in-flight data
                b'E' => return Err(parse_error(&msg)),
                b'Z' => {
                    self.copying = false;
                    return Ok(());
                }
                other => {
                    return Err(Error::Transfer(format!(
                        "walsender stop: unexpected message {:?}",
                        other as char
                    )))
                }
            }
        }
    }
}

fn io_err(e: std::io::Error) -> Error {
    // This socket serves BOTH the replication connection and the bulk raw-COPY
    // plane (connect_sql), so the message must not name a subsystem the caller
    // may not be using: a bulk transfer that died here used to report
    // "walsender io: early eof", sending users to read up on replication
    // settings that had nothing to do with it.
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        return Error::Transfer(
            "postgres closed the connection mid-stream — a server restart, a              pg_terminate_backend, an idle/statement timeout, or a dropped              network path. Nothing was committed at the destination (bulk loads              swap only at the end, and a CDC watermark advances only with its              data), so re-running is safe and is the recovery."
                .into(),
        );
    }
    Error::Transfer(format!("postgres wire io: {e}"))
}

fn bad_scram() -> Error {
    Error::Transfer("walsender: malformed SCRAM exchange".into())
}

fn parse_error(msg: &[u8]) -> Error {
    // ErrorResponse: fields of (type u8, cstr) until 0.
    let (mut code, mut text) = (String::new(), String::new());
    let mut i = 0;
    while i < msg.len() && msg[i] != 0 {
        let t = msg[i];
        let end = msg[i + 1..].iter().position(|&c| c == 0).map(|p| i + 1 + p);
        let Some(end) = end else { break };
        let v = String::from_utf8_lossy(&msg[i + 1..end]).into_owned();
        match t {
            b'C' => code = v,
            b'M' => text = v,
            _ => {}
        }
        i = end + 1;
    }
    Error::Transfer(format!("walsender: server error {code}: {text}"))
}

fn md5_password(user: &str, password: &str, salt: [u8; 4]) -> String {
    use md5::Md5;
    let inner = hex::encode(Md5::digest(format!("{password}{user}")));
    let outer = hex::encode(Md5::digest([inner.as_bytes(), &salt].concat()));
    format!("md5{outer}")
}

// ============================================================================
// SCRAM-SHA-256 (RFC 5802/7677), no channel binding (gs2 header "n,,").
// ============================================================================

struct ScramState {
    nonce: String,
    client_first_bare: String,
    /// Set by client_final for verify_server.
    server_signature: std::cell::RefCell<Option<Vec<u8>>>,
}

impl ScramState {
    fn start() -> Self {
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let client_first_bare = format!("n=,r={nonce}");
        Self { nonce, client_first_bare, server_signature: Default::default() }
    }

    fn client_first(&self) -> String {
        format!("n,,{}", self.client_first_bare)
    }

    fn client_final(&self, server_first: &str, password: &str) -> Result<String> {
        let mut r = None;
        let mut s = None;
        let mut i = None;
        for part in server_first.split(',') {
            match part.split_once('=') {
                Some(("r", v)) => r = Some(v.to_string()),
                Some(("s", v)) => s = Some(v.to_string()),
                Some(("i", v)) => i = v.parse::<u32>().ok(),
                _ => {}
            }
        }
        let (r, s, i) = (
            r.ok_or_else(bad_scram)?,
            s.ok_or_else(bad_scram)?,
            i.ok_or_else(bad_scram)?,
        );
        if !r.starts_with(&self.nonce) {
            return Err(Error::Transfer("walsender: SCRAM nonce mismatch".into()));
        }
        let salt = B64.decode(&s).map_err(|_| bad_scram())?;
        let salted = hi(password.as_bytes(), &salt, i);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = Sha256::digest(&client_key);
        let without_proof = format!("c={},r={r}", B64.encode(b"n,,"));
        let auth_message =
            format!("{},{server_first},{without_proof}", self.client_first_bare);
        let client_sig = hmac_sha256(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_sig.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        let server_key = hmac_sha256(&salted, b"Server Key");
        *self.server_signature.borrow_mut() =
            Some(hmac_sha256(&server_key, auth_message.as_bytes()));
        Ok(format!("{without_proof},p={}", B64.encode(proof)))
    }

    fn verify_server(&self, server_final: &str) -> Result<()> {
        let v = server_final
            .strip_prefix("v=")
            .ok_or_else(bad_scram)?
            .trim_end_matches(['\0', '\n']);
        let got = B64.decode(v).map_err(|_| bad_scram())?;
        let want = self.server_signature.borrow();
        if want.as_deref() == Some(got.as_slice()) {
            Ok(())
        } else {
            Err(Error::Transfer(
                "walsender: SCRAM server signature mismatch — wrong server?".into(),
            ))
        }
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = Hmac::<Sha256>::new_from_slice(key).expect("any key length");
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

/// PBKDF2-HMAC-SHA256 with dkLen = one block (RFC 2898 `Hi` from RFC 5802).
fn hi(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut u = hmac_sha256(password, &[salt, &1u32.to_be_bytes()].concat());
    let mut out = u.clone();
    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for (o, b) in out.iter_mut().zip(u.iter()) {
            *o ^= b;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677's published SCRAM-SHA-256 test vector.
    #[test]
    fn scram_matches_the_rfc7677_vector() {
        let st = ScramState {
            nonce: "rOprNGfwEbeRWgbNEkqO".into(),
            client_first_bare: "n=user,r=rOprNGfwEbeRWgbNEkqO".into(),
            server_signature: Default::default(),
        };
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                            s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let fin = st.client_final(server_first, "pencil").unwrap();
        assert_eq!(
            fin,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
             p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        st.verify_server("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .unwrap();
    }

    #[test]
    fn md5_password_matches_postgres_formula() {
        // Known-answer computed with PG's own algorithm.
        assert_eq!(
            md5_password("u", "p", [1, 2, 3, 4]),
            format!("md5{}", {
                use md5::{Digest, Md5};
                let inner = hex::encode(Md5::digest("pu"));
                hex::encode(Md5::digest([inner.as_bytes(), &[1, 2, 3, 4][..]].concat()))
            })
        );
    }

    #[test]
    fn error_response_parses_code_and_message() {
        let mut msg = Vec::new();
        msg.extend_from_slice(b"C42601\0");
        msg.extend_from_slice(b"Msyntax error\0");
        msg.push(0);
        let e = parse_error(&msg);
        let s = format!("{e}");
        assert!(s.contains("42601") && s.contains("syntax error"));
    }

    #[test]
    fn url_parse_reads_sslmode_and_decodes_credentials() {
        let ci = parse_url("postgres://u%40x:p%3Aw@h:5433/db").unwrap();
        assert_eq!((ci.user.as_str(), ci.password.as_str()), ("u@x", "p:w"));
        assert_eq!((ci.host.as_str(), ci.port, ci.db.as_str()), ("h", 5433, "db"));
        // libpq's default, and so this client's.
        assert_eq!(ci.ssl, SslMode::Prefer { explicit: false });
        // An IPv6 literal must come out DIALLABLE, not URL-shaped: with the
        // brackets left on, connect() looks up "[::1]" in DNS.
        let ci6 = parse_url("postgres://u:p@[2001:db8::1]:5433/db").unwrap();
        assert_eq!((ci6.host.as_str(), ci6.port), ("2001:db8::1", 5433));
        assert_eq!(parse_url("postgres://u:p@[::1]/db").unwrap().host, "::1");
        // A name is left exactly as it was.
        assert_eq!(parse_url("postgres://u:p@db.example/x").unwrap().host, "db.example");
        // Both spellings, because a URL that works for the pool must not
        // mean something weaker for the replication socket.
        assert_eq!(
            parse_url("postgres://u:p@h/db?ssl-mode=require").unwrap().ssl,
            SslMode::Require
        );
        for (q, want) in [
            ("disable", SslMode::Disable),
            ("prefer", SslMode::Prefer { explicit: true }),
            ("allow", SslMode::Prefer { explicit: true }),
            ("require", SslMode::Require),
            ("verify-full", SslMode::VerifyFull),
        ] {
            let ci = parse_url(&format!("postgres://u:p@h/db?sslmode={q}")).unwrap();
            assert_eq!(ci.ssl, want, "sslmode={q}");
        }
        // Refused by name rather than silently mapped to a neighbouring mode:
        // one of them would be weaker than asked for, the other stricter.
        assert!(parse_url("postgres://u:p@h/db?sslmode=verify-ca").is_err());
        assert!(parse_url("postgres://u:p@h/db?sslmode=yes-please").is_err());
    }

    /// LIVE smoke against a real Postgres (`wal_level=logical`):
    ///
    ///   WAL_URL=postgres://user:pass@host/db \
    ///   cargo test -p apitap-core walsender_live -- --ignored --nocapture
    ///
    /// Creates a TEMPORARY slot (auto-dropped on disconnect), starts
    /// replication, generates a little traffic via a second walsender
    /// session's simple-query support, and decodes the events end-to-end.
    #[test]
    #[ignore]
    fn walsender_live() {
        use crate::wire::pgoutput::{self, PgoMessage};
        let url = std::env::var("WAL_URL").expect("set WAL_URL");
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut ws = Walsender::connect(&url).await.expect("connect");
                let sys = ws.simple_query("IDENTIFY_SYSTEM").await.expect("identify");
                println!("IDENTIFY_SYSTEM: {sys:?}");

                // A second session drives DDL/DML (walsender database mode
                // runs plain SQL too — same trick ape-dts leans on).
                let mut sql = Walsender::connect(&url).await.expect("sql conn");
                sql.simple_query(
                    "DROP TABLE IF EXISTS walsmoke; \
                     CREATE TABLE walsmoke(id int primary key, v text); \
                     DROP PUBLICATION IF EXISTS walsmoke_pub; \
                     CREATE PUBLICATION walsmoke_pub FOR TABLE walsmoke",
                )
                .await
                .expect("ddl");

                let _ = sql
                    .simple_query("SELECT pg_drop_replication_slot('apitap_walsmoke')")
                    .await; // leftover from an aborted run — best-effort
                let rows = ws
                    .simple_query(
                        "CREATE_REPLICATION_SLOT apitap_walsmoke TEMPORARY LOGICAL \
                         pgoutput EXPORT_SNAPSHOT",
                    )
                    .await
                    .expect("create slot");
                let consistent_point = rows[0][1].clone().expect("consistent_point");
                let snapshot = rows[0][2].clone();
                println!("slot at {consistent_point}, snapshot {snapshot:?}");
                assert!(snapshot.is_some(), "EXPORT_SNAPSHOT must yield a name");

                // Two separate simple-query batches = two source transactions,
                // so the drain sees two Commits.
                sql.simple_query(
                    "INSERT INTO walsmoke VALUES (1,'a'),(2,''); \
                     UPDATE walsmoke SET v='b' WHERE id=1; \
                     DELETE FROM walsmoke WHERE id=2",
                )
                .await
                .expect("dml");
                sql.simple_query("TRUNCATE walsmoke").await.expect("truncate");

                let lsn = pgoutput::lsn_from_string(&consistent_point).unwrap();
                ws.start_replication("apitap_walsmoke", lsn, "walsmoke_pub")
                    .await
                    .expect("start replication");

                let (mut ins, mut upd, mut del, mut trunc, mut commits) = (0, 0, 0, 0, 0);
                let mut empty_string_seen = false;
                while commits < 2 {
                    match ws.next_event().await.expect("event") {
                        Some(WalEvent::XLogData { payload, .. }) => {
                            match pgoutput::decode(&payload, false, &Default::default()).map(|(m, _)| m)
                                .expect("decode")
                            {
                                PgoMessage::Insert { new, .. } => {
                                    ins += 1;
                                    if new.views().any(|c| {
                                        matches!(c, pgoutput::Cellv::Text(t) if t.is_empty())
                                    }) {
                                        empty_string_seen = true;
                                    }
                                }
                                PgoMessage::Update { .. } => upd += 1,
                                PgoMessage::Delete { .. } => del += 1,
                                PgoMessage::Truncate { .. } => trunc += 1,
                                PgoMessage::Commit { .. } => commits += 1,
                                _ => {}
                            }
                        }
                        Some(WalEvent::Keepalive { reply_requested, .. }) => {
                            if reply_requested {
                                ws.standby_status(lsn, false).await.unwrap();
                            }
                        }
                        None => break,
                    }
                }
                assert_eq!((ins, upd, del, trunc), (2, 1, 1, 1), "full op coverage");
                assert!(empty_string_seen, "empty string must arrive as Text(\"\"), not Null");
                ws.stop_replication().await.expect("clean stop");
                sql.simple_query("DROP TABLE walsmoke; DROP PUBLICATION walsmoke_pub")
                    .await
                    .expect("cleanup");
                println!("walsender live smoke: ALL OPS CAPTURED, clean stop");
            });
    }
}
