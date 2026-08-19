//! Saying what is wrong with a URL without saying the password.
//!
//! Nearly every URL apitap is handed fails to parse for one reason: a password
//! with a reserved character in it. `p@ssw0rd` puts a second `@` in the
//! authority, `a/b` ends the authority early, `?` starts a query string — and
//! what the parser reports is the *consequence*, several characters later:
//!
//! ```text
//! mysql url: invalid port number
//! ```
//!
//! That names neither the password nor even the fact that a password is
//! involved, and the obvious next move — print the URL so the user can see it
//! — is the one thing that must never happen, because the URL is the
//! credential. So the URL is re-read by hand, far enough to show the shape the
//! parser saw with the secret struck out, and the fix is named outright.
//!
//! The hand-reading is deliberately crude, because it is reading a string the
//! real parser has already rejected. Split off the scheme, then split at the
//! LAST `@` in what remains — not the first, which is inside the password, and
//! not "the authority up to the first `/`", which is inside the password too
//! the moment the password holds a slash. (That was the first version, and its
//! own test caught it: `postgres://u:a/b@h:5432/db` came out as
//! `postgres://u:a` — the host gone, the `@` never seen, and no advice offered
//! on precisely the URL that needed it.) Everything before that `@` is
//! userinfo, everything after it up to the first `/?#` is host and port, and
//! user splits from secret at the first `:`.
//!
//! The one shape this misreads is an `@` in the PATH of a URL with no
//! credentials. It cannot come up here: such a URL parses, and this code only
//! runs on one that did not.
//!
//! It is a diagnostic, not a parser; nothing downstream ever sees its output.

use crate::Error;
use std::fmt::Display;

/// The authority with the password replaced, plus whatever we can say about
/// why it might not have parsed. Never contains the secret.
/// Split what follows `scheme://` into (userinfo, host:port). See the module
/// docs for why the last `@` is the right pivot and the first `/` is not.
fn split_authority(rest: &str) -> (Option<&str>, &str) {
    match rest.rsplit_once('@') {
        Some((u, h)) => (Some(u), h.split(['/', '?', '#']).next().unwrap_or(h)),
        None => (None, rest.split(['/', '?', '#']).next().unwrap_or(rest)),
    }
}

pub(crate) fn sanitized(url: &str) -> String {
    let (scheme, rest) = match url.split_once("://") {
        Some((s, r)) => (s, r),
        // No `://` at all — the shape is wrong long before the password is.
        None => {
            let end = url
                .char_indices()
                .map(|(i, c)| i + c.len_utf8())
                .take(12)
                .last()
                .unwrap_or(0);
            return format!("{}…", &url[..end]);
        }
    };
    let (userinfo, hostport) = split_authority(rest);
    let host = if hostport.is_empty() { "<empty>" } else { hostport };
    match userinfo {
        None => format!("{scheme}://{host}"),
        Some(ui) => {
            let (user, secret) = match ui.split_once(':') {
                Some((u, s)) => (u, Some(s)),
                None => (ui, None),
            };
            let user = if user.is_empty() { "<empty>" } else { user };
            match secret {
                None => format!("{scheme}://{user}@{host}"),
                Some(s) => format!("{scheme}://{user}:<{} chars, hidden>@{host}", s.chars().count()),
            }
        }
    }
}

/// Characters that end or redirect the authority when they are not
/// percent-encoded. A password holding any of them is the usual cause.
const RESERVED: [char; 7] = [':', '/', '?', '#', '[', ']', '@'];

/// Build the error for a URL apitap could not parse.
///
/// `what` names the URL's role ("mysql url", "clickhouse url", …) so the
/// message is useful in a multi-table run where several URLs are in play.
pub(crate) fn bad_url(what: &str, url: &str, e: impl Display) -> Error {
    let shown = sanitized(url);
    let mut msg = format!("{what}: {e}\n  read as: {shown}");
    // Only offer the percent-encoding fix when there is a userinfo section for
    // it to apply to — on a URL with no credentials it would just be noise
    // pointing at a part that is not there.
    if let Some((_, rest)) = url.split_once("://") {
        if let (Some(userinfo), _) = split_authority(rest) {
            let after_user = userinfo.split_once(':').map(|(_, s)| s).unwrap_or("");
            let offenders: Vec<String> = RESERVED
                .iter()
                .filter(|c| after_user.contains(**c))
                .map(|c| format!("'{c}'"))
                .collect();
            if !offenders.is_empty() {
                msg.push_str(&format!(
                    "\n  the password contains {} — those are URL syntax, not text, \
                     so they end the authority early and the parser reports the \
                     damage further along",
                    offenders.join(" and ")
                ));
            }
            msg.push_str(
                "\n  percent-encode the password before it goes in the URL:\n\
                 \x20     from urllib.parse import quote\n\
                 \x20     url = f\"scheme://{user}:{quote(password, safe='')}@{host}:{port}/{db}\"",
            );
        }
    }
    Error::InvalidInput(msg)
}

/// Wrap a CONNECT failure, adding the URL diagnosis when the URL looks like the
/// reason.
///
/// The parse-time path above only fires when a URL is malformed enough for the
/// parser to give up. The common case is worse than that: a password with a
/// reserved character produces a URL that parses *fine* and means something
/// else. `postgres://alice:p@ssw0rd@db.internal/app` is read with `ssw0rd` as
/// the host, and what the user sees is
///
/// ```text
/// connect: error communicating with database: failed to lookup address
/// information: Name or service not known
/// ```
///
/// — a DNS error naming a host they never typed, on a database they can ping.
/// This is also the path that matters most, because every bulk transfer takes
/// it: those connect through sqlx, which parses the URL itself and never
/// reaches `bad_url`.
///
/// The hint is only added when the userinfo actually holds a reserved
/// character. A wrong password, a firewall, a server that is down — all reach
/// here too, and telling those users to check their percent-encoding would send
/// them down a road with nothing at the end of it.
pub(crate) fn connect_err(what: &str, url: &str, e: impl Display) -> Error {
    let plain = format!("{what}: {e}");
    let Some((_, rest)) = url.split_once("://") else {
        return Error::Connect(plain);
    };
    let (Some(userinfo), _host) = split_authority(rest) else {
        return Error::Connect(plain);
    };
    let secret = match userinfo.split_once(':') {
        Some((_, s)) => s,
        None => return Error::Connect(plain),
    };
    let offenders: Vec<String> = RESERVED
        .iter()
        .filter(|c| secret.contains(**c))
        .map(|c| format!("'{c}'"))
        .collect();
    if offenders.is_empty() {
        return Error::Connect(plain);
    }
    Error::Connect(format!(
        "{plain}\n  the password in this URL contains {} — unencoded, those are \
         URL syntax, so the host the client dialled is probably not the host you \
         wrote (this URL reads as: {})\n  percent-encode the password:\n\
         \x20     from urllib.parse import quote\n\
         \x20     url = f\"scheme://{{user}}:{{quote(password, safe='')}}@{{host}}:{{port}}/{{db}}\"",
        offenders.join(" and "),
        sanitized(url),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(url: &str) -> String {
        match bad_url("mysql url", url, "invalid port number") {
            Error::InvalidInput(s) => s,
            other => panic!("wrong variant: {other:?}"),
        }
    }

    /// The one thing this module must never do.
    #[test]
    fn the_password_never_appears_anywhere_in_the_message() {
        for url in [
            "mysql://root:hunter2@host:3307/db",
            "mysql://root:p@ssw0rd@host:3307/db",
            "postgres://u:a/b?c#d@host/db",
            "mysql://root:@host/db",
            "clickhouse://default:s3cr3t@127.0.0.1:8123",
        ] {
            let m = err(url);
            for secret in ["hunter2", "ssw0rd", "s3cr3t", "a/b?c#d"] {
                assert!(
                    !m.contains(secret),
                    "leaked {secret:?} from {url:?}:\n{m}"
                );
            }
        }
    }

    /// The point of the message: the host survives the mangling that a bare
    /// '@' in the password does to the authority, so the user can see the
    /// parser landed somewhere sensible.
    #[test]
    fn the_host_is_read_past_an_unencoded_at_sign() {
        assert_eq!(
            sanitized("mysql://root:p@ssw0rd@db.internal:3307/bench"),
            "mysql://root:<8 chars, hidden>@db.internal:3307"
        );
    }

    /// …and it names the character that did it.
    #[test]
    fn the_offending_character_is_named() {
        let m = err("mysql://root:p@ssw0rd@db.internal:3307/bench");
        assert!(m.contains("'@'"), "{m}");
        assert!(m.contains("quote(password, safe='')"), "{m}");
    }

    /// A URL with no credentials must not be told to encode a password it
    /// does not have.
    #[test]
    fn a_url_without_credentials_gets_no_password_advice() {
        let m = err("clickhouse://127.0.0.1:8123/default");
        assert!(!m.contains("quote(password"), "{m}");
        assert!(m.contains("clickhouse://127.0.0.1:8123"), "{m}");
    }

    /// A password with reserved characters but no '@' still gets the fix,
    /// because '/' and '?' break the authority just as thoroughly.
    #[test]
    fn slash_and_question_mark_are_reported_too() {
        let m = err("postgres://u:a/b@h:5432/db");
        assert!(m.contains("'/'"), "{m}");
        let m = err("postgres://u:a?b@h:5432/db");
        assert!(m.contains("'?'"), "{m}");
    }

    /// The case that killed the first version of `split_authority`: a password
    /// with a slash in it. Cutting the authority at the first `/` landed
    /// INSIDE the password, so the host vanished and no advice was given — on
    /// exactly the URL the advice exists for.
    #[test]
    fn a_slash_in_the_password_does_not_swallow_the_host() {
        assert_eq!(
            sanitized("postgres://u:a/b@h.example:5432/db"),
            "postgres://u:<3 chars, hidden>@h.example:5432"
        );
        assert_eq!(
            sanitized("postgres://u:a?b@h.example:5432/db"),
            "postgres://u:<3 chars, hidden>@h.example:5432"
        );
    }

    fn cerr(url: &str) -> String {
        match connect_err("connect", url, "failed to lookup address information") {
            Error::Connect(s) => s,
            other => panic!("wrong variant: {other:?}"),
        }
    }

    /// The case the e2e leg exposed: a password with '@' produces a URL that
    /// PARSES, so the parse-time diagnostic never fires and the user gets a
    /// DNS error naming a host they never typed.
    #[test]
    fn a_connect_failure_explains_a_reserved_character_in_the_password() {
        let m = cerr("postgres://alice:p@ssw0rd@db.internal:5432/app");
        assert!(m.contains("'@'"), "{m}");
        assert!(m.contains("db.internal"), "the real host is shown: {m}");
        assert!(m.contains("quote(password, safe='')"), "{m}");
        assert!(!m.contains("ssw0rd"), "leaked: {m}");
    }

    /// A wrong password, a firewall, a server that is down — all land here too,
    /// and must NOT be sent chasing percent-encoding.
    #[test]
    fn an_ordinary_connect_failure_gets_no_url_advice() {
        for url in [
            "postgres://alice:plainpassword@db.internal:5432/app",
            "postgres://db.internal:5432/app",
            "not even a url",
        ] {
            let m = cerr(url);
            assert!(!m.contains("percent-encode"), "{url}: {m}");
        }
    }

    /// Garbage in must not panic — this runs on a string the real parser has
    /// already given up on.
    #[test]
    fn nothing_here_panics_on_a_string_that_is_not_a_url() {
        for s in ["", "://", "@", "mysql://", "mysql://@", "mysql://:@", "no-scheme-at-all",
                  "mysql://u:p@", "🙂://🙂:🙂@🙂"] {
            let _ = sanitized(s);
            let _ = err(s);
        }
    }
}
