//! ORDER BY on text under a collation whose order is not byte order
//! (W1 round P): a Postgres column under a glibc or ICU locale (`en_US`,
//! `und-x-icu`; `citext` too), a MySQL column under any collation but
//! `_bin`/`binary`. apitap sorts text by its bytes (a MySQL `_ci`/`_ai`
//! column by its case- and accent-folded bytes). Where the source would order
//! two values otherwise, `ORDER BY s LIMIT n` returned other rows than the
//! source, with no error. apitap does not carry the collations' tables, so
//! the sort stays as it is and is CHECKED: every pair of rows next to each
//! other in the answer whose order the text key decides must be ordered the
//! same by the source's collation as by bytes, or the statement is refused by
//! name.
//!
//! ## Why checking adjacent pairs is enough
//!
//! The answer is sorted by bytes (strictly: by the keys, the text key by its
//! compared form). If for every adjacent pair (x, y) the collation also puts
//! x no later than y, then by transitivity the collation's order of the whole
//! list is the same (ties aside: rows equal under the collation may come in
//! another order, which SQL leaves open anyway). So the check is sound iff
//! [`pair_ok`] never says "same order" for a pair the collation orders the
//! other way. It must see the whole sorted list: a top-N selection or a
//! per-pass LIMIT prune by bytes can drop a row the collation would have
//! kept, which no check of the kept rows can see. Under a guarded key the
//! engine therefore sorts every candidate row and cuts the LIMIT after the
//! check (`order_rows`, `retain`, `order_limit`).
//!
//! ## The pair rule ([`pair_ok`]), conservative by construction
//!
//! For the collations this applies to (glibc locales, ICU/CLDR, MySQL UCA
//! 0900 and the older `_ci` ones), level 1 weighs ASCII digits below ASCII
//! letters, digits in numeric order, letters A..Z in alphabet order with
//! case ignored, and the decision falls at the first position whose level-1
//! weights differ. Given a pair a < b by bytes, with i the first differing
//! byte and the prefix a[..i] = b[..i]:
//!
//! - equal bytes: a tie, fine.
//! - the prefix must be ASCII: identical ASCII bytes give identical level-1
//!   weights (no contraction among ASCII characters in an untailored
//!   collation), so they cannot decide the pair. Not ASCII: refused.
//! - a is a prefix of b ('ab' vs 'abc', 'ab' vs 'ab-'): bytes say a < b. The
//!   collation agrees when b's rest has a level-1 weight of its own, which an
//!   ASCII letter or digit has; a rest of punctuation or spaces only may be
//!   ignorable at level 1 (glibc) and the pair then turns on later levels:
//!   refused. Under a PAD SPACE collation a rest of spaces only is equal
//!   (a tie), but the compared form already has trailing spaces trimmed.
//! - a[i] and b[i] both ASCII digits: numeric order = byte order.
//! - a digit and a letter: digits weigh below letters at level 1, and their
//!   bytes are below letters too: the same order.
//! - two letters: the collation compares them case-folded; bytes compare
//!   them as they are. Same order iff the folded letters order as the bytes
//!   do ('a' < 'B' folded, 'a' > 'B' by bytes: refused). The same letter in
//!   two cases ('a' vs 'A'): level 1 ties here and a later position or level
//!   decides: refused.
//! - anything else at i (punctuation, space, control, a non-ASCII byte):
//!   refused. glibc ignores punctuation at level 1 ('AC' < 'A-D' there, not
//!   by bytes); UCA weighs it below digits; either way bytes cannot tell.
//! - a collation tailored for its language that reorders or contracts ASCII
//!   letters (Danish/Norwegian `aa`, Czech/Slovak `ch`, Lithuanian `y`,
//!   Turkish dotless i, Welsh digraphs, MySQL `roman`, …, see [`tailored`]):
//!   two differing letters are refused there; digits keep their rule.
//!
//! When unsure, refuse: a refusal names the column, the collation, the first
//! pair (each value cut to 16 characters: they may be client data) and the
//! remedy.

/// Whether two adjacent values, in the form the engine compared them, are
/// ordered the same by the column's collation as by their bytes (or tie).
/// See the module doc for why each case is safe.
pub(super) fn pair_ok(a: &[u8], b: &[u8], tailored: bool) -> bool {
    if a == b {
        return true;
    }
    let i = a.iter().zip(b).take_while(|(x, y)| x == y).count();
    if !a[..i].is_ascii() {
        return false;
    }
    if i == a.len() || i == b.len() {
        let rest = if i == a.len() { &b[i..] } else { &a[i..] };
        return rest.is_ascii() && rest.iter().any(u8::is_ascii_alphanumeric);
    }
    let (x, y) = (a[i], b[i]);
    match (x.is_ascii_digit(), y.is_ascii_digit(), x.is_ascii_alphabetic(), y.is_ascii_alphabetic()) {
        (true, true, _, _) => true,
        (true, false, _, true) | (false, true, true, _) => true,
        (false, false, true, true) => {
            let (fx, fy) = (x.to_ascii_lowercase(), y.to_ascii_lowercase());
            !tailored && fx != fy && fx.cmp(&fy) == x.cmp(&y)
        }
        _ => false,
    }
}

/// Whether a collation (a Postgres libc locale or ICU locale, a MySQL
/// collation name) is tailored for a language that reorders or contracts
/// ASCII letters. Unknown languages count as untailored only when they are
/// not in this list: the list holds every CLDR/glibc/MySQL tailoring of
/// ASCII letters known to us.
pub(super) fn tailored(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    if n.contains("trad") || n.contains("phonebk") || n.contains("phonebook") {
        return true;
    }
    // MySQL: charset_language_..., the language as a word.
    const MY: [&str; 13] = [
        "danish", "czech", "slovak", "hungarian", "lithuanian", "latvian", "estonian", "croatian", "turkish", "spanish2", "roman",
        "sinhala", "vietnamese",
    ];
    if n.split('_').any(|w| MY.contains(&w)) {
        return true;
    }
    // ICU / libc / MySQL 0900: the language code first (`da_DK.utf8`,
    // `da-x-icu`, `utf8mb4_da_0900_ai_ci`).
    const LANG: [&str; 23] = [
        "da", "nb", "nn", "no", "cs", "sk", "hu", "lt", "lv", "et", "cy", "hr", "bs", "sr", "sh", "tr", "az", "kl", "se", "fil", "haw",
        "to", "fo",
    ];
    let lang = |s: &str| s.split(['_', '-', '.', '@']).next().unwrap_or("").to_string();
    let first = lang(&n);
    if LANG.contains(&first.as_str()) {
        return true;
    }
    // A MySQL name's second word (`utf8mb4_da_0900_ai_ci`).
    if let Some(w) = n.split('_').nth(1) {
        if n.starts_with("utf8") || n.starts_with("latin") || n.starts_with("ucs2") || n.starts_with("utf16") || n.starts_with("utf32") {
            return LANG.contains(&w);
        }
    }
    false
}

/// A column's collation as `ArrowField::locale` carries it: the name a
/// refusal shows and what the comparison oracle ([`verify`]) asks the
/// server under. MySQL: the collation name. Postgres: `pg` + the
/// collation to name (empty: the column's default) + citext, as fields.
pub(super) enum Locale {
    Pg { coll: String, citext: bool, shown: String },
    My { coll: String },
}

impl Locale {
    pub(super) fn encode(&self) -> String {
        match self {
            Locale::Pg { coll, citext, shown } => format!("pg\u{1f}{coll}\u{1f}{}\u{1f}{shown}", u8::from(*citext)),
            Locale::My { coll } => coll.clone(),
        }
    }

    pub(super) fn parse(s: &str) -> Locale {
        let f: Vec<&str> = s.split('\u{1f}').collect();
        match f.as_slice() {
            ["pg", coll, ci, shown] => Locale::Pg { coll: coll.to_string(), citext: *ci == "1", shown: shown.to_string() },
            _ => Locale::My { coll: s.to_string() },
        }
    }

    /// Whether the collation is tailored for a language ([`tailored`]).
    pub(super) fn tailored(s: &str) -> bool {
        match Locale::parse(s) {
            Locale::Pg { coll, shown, .. } => {
                let inner = shown.rsplit_once('(').map_or("", |x| x.1).trim_end_matches(')');
                tailored(&coll) || tailored(inner)
            }
            Locale::My { coll } => tailored(&coll),
        }
    }

    /// The name a refusal shows.
    pub(super) fn shown(s: &str) -> String {
        match Locale::parse(s) {
            Locale::Pg { shown, .. } => shown,
            Locale::My { coll } => coll,
        }
    }
}

/// Pairs the check could not vouch for, kept for the server to order
/// ([`verify`]): (column, collation as carried, source URL, the values as
/// the source holds them, first then second in apitap's order).
#[derive(Default)]
pub(super) struct Pending {
    pub(super) pairs: Vec<(String, String, String, Vec<u8>, Vec<u8>)>,
    bytes: usize,
}

/// At most this many pairs, and this many bytes of them, go to a server
/// per statement; past either the statement is refused as before.
const ORACLE_PAIRS: usize = 4096;
const ORACLE_BYTES: usize = 1 << 20;

impl Pending {
    /// Keep a pair for the server; `false` when past the cap.
    pub(super) fn push(&mut self, col: &str, coll: &str, url: &str, a: &[u8], b: &[u8]) -> bool {
        if self.pairs.len() >= ORACLE_PAIRS || self.bytes + a.len() + b.len() > ORACLE_BYTES {
            return false;
        }
        self.bytes += a.len() + b.len();
        self.pairs.push((col.to_string(), coll.to_string(), url.to_string(), a.to_vec(), b.to_vec()));
        true
    }
}

tokio::task_local! {
    /// The statement's pairs for the server (`order_rows` and `order_limit`
    /// fill it, `run_engine` and the parts path ask the server after).
    pub(super) static PENDING: std::cell::RefCell<Pending>;
}

/// Ask each source whether its collation orders every kept pair as apitap
/// did (first no later than second): one statement per column, the values
/// sent as parameters (a comparison of values apitap holds, not a query of
/// the table). The first pair the server orders the other way refuses the
/// statement by name, as the check did before (W1 round P, p4: the check
/// alone refused names with spaces or punctuation that the server, e.g. a
/// musl `en_US`, orders by bytes after all).
pub(super) async fn verify(p: Pending) -> crate::Result<()> {
    use std::collections::BTreeMap;
    let mut by: BTreeMap<(String, String, String), Vec<(Vec<u8>, Vec<u8>)>> = BTreeMap::new();
    for (col, coll, url, a, b) in p.pairs {
        by.entry((col, coll, url)).or_default().push((a, b));
    }
    for ((col, coll, url), pairs) in by {
        let a: Vec<String> = pairs.iter().map(|x| String::from_utf8_lossy(&x.0).into_owned()).collect();
        let b: Vec<String> = pairs.iter().map(|x| String::from_utf8_lossy(&x.1).into_owned()).collect();
        let bad_at: Option<i64> = match Locale::parse(&coll) {
            Locale::Pg { coll: c, citext, .. } => {
                let (x, y) = if citext {
                    ("a::citext".to_string(), "b::citext".to_string())
                } else if c.is_empty() {
                    ("a".to_string(), "b".to_string())
                } else {
                    let q = format!("COLLATE \"{}\"", c.replace('"', "\"\""));
                    (format!("a {q}"), format!("b {q}"))
                };
                let src = crate::source::postgres::PgSource::connect(&url, 1).await?;
                sqlx::query_scalar(&format!(
                    "SELECT i FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS t(a, b, i) WHERE {x} > {y} ORDER BY i LIMIT 1"
                ))
                .bind(&a)
                .bind(&b)
                .fetch_optional(src.pool())
                .await
                .map_err(|e| crate::Error::Transfer(format!("sql: asking the server how {col} orders: {e}")))?
            }
            Locale::My { coll: c } => {
                let cs = c.split('_').next().unwrap_or("utf8mb4").to_string();
                let ok = |s: &str| s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
                if !ok(&c) || !ok(&cs) {
                    return Err(refuse(&col, &c, &pairs[0].0, &pairs[0].1));
                }
                // A UNION ALL of the pairs as parameters: every MySQL and
                // MariaDB version has it (JSON_TABLE is MySQL 8.0 / MariaDB
                // 10.6 on). At most ORACLE_PAIRS rows, 3 parameters each.
                let src = crate::source::mysql::MySqlSource::connect(&url, 1).await?;
                let rows = (0..a.len()).map(|k| if k == 0 { "SELECT ? AS i, ? AS a, ? AS b".to_string() } else { "SELECT ?, ?, ?".to_string() }).collect::<Vec<_>>().join(" UNION ALL ");
                let sql = format!(
                    "SELECT t.i FROM ({rows}) t WHERE STRCMP(CONVERT(t.a USING {cs}) COLLATE {c}, CONVERT(t.b USING {cs}) COLLATE {c}) > 0 \
                     ORDER BY t.i LIMIT 1"
                );
                let mut q = sqlx::query_scalar::<_, i64>(&sql);
                for (k, (x, y)) in a.iter().zip(&b).enumerate() {
                    q = q.bind(k as i64 + 1).bind(x.as_str()).bind(y.as_str());
                }
                let v: Option<i64> = q
                    .fetch_optional(src.pool())
                    .await
                    .map_err(|e| crate::Error::Transfer(format!("sql: asking the server how {col} orders: {e}")))?;
                v
            }
        };
        if let Some(i) = bad_at {
            let (x, y) = &pairs[(i - 1).max(0) as usize];
            return Err(refuse(&col, &Locale::shown(&coll), x, y));
        }
    }
    Ok(())
}

/// A value for a refusal: at most 16 characters (it may be client data).
pub(super) fn shown(v: &[u8]) -> String {
    let s = String::from_utf8_lossy(v);
    let mut out: String = s.chars().take(16).collect();
    if s.chars().count() > 16 {
        out.push('…');
    }
    format!("{out:?}")
}

/// The refusal for a pair the check cannot vouch for.
pub(super) fn refuse(col: &str, coll: &str, a: &[u8], b: &[u8]) -> crate::Error {
    crate::Error::InvalidInput(format!(
        "sql: ORDER BY {col}: the column's collation {coll} may order {} and {} otherwise than apitap's byte order \
         (case, punctuation, accents or the language's rules), so the answer could differ from the source's. \
         Order by it under a byte collation in the source (Postgres: ORDER BY {col} COLLATE \"C\" in a view, or a \
         column with COLLATE \"C\"; MySQL: a _bin collation or BINARY), or order by a column that is not text",
        shown(a),
        shown(b)
    ))
}

/// A Postgres table's text columns whose order is not byte order (name →
/// the collation as shown in a refusal), and the columns under a
/// nondeterministic collation (equality is not byte equality there).
pub(super) struct PgColl {
    pub(super) locale: Vec<(String, String)>,
    pub(super) nondet: Vec<(String, String)>,
}

/// Whether a Postgres collation orders by bytes (code points = UTF-8 bytes).
fn pg_bytes(provider: &str, locale: &str, name: &str) -> bool {
    let l = locale.to_ascii_lowercase();
    matches!(name, "C" | "POSIX" | "ucs_basic" | "pg_c_utf8" | "pg_unicode_fast")
        || provider == "b"
        || (provider != "i" && (l == "c" || l == "posix" || l.starts_with("c.")))
}

/// [`PgColl`] for `table` on the server at `url`: one catalog query (each
/// column's collation with the database's default), read as JSON so it runs
/// on every version (`collisdeterministic` is 12+, `datlocprovider` 15+,
/// `colllocale` 17+).
pub(super) async fn pg_collations(src: &crate::source::postgres::PgSource, table: &str) -> crate::Result<PgColl> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT a.attname AS name, format_type(a.atttypid, NULL) AS ty, to_jsonb(co)::text AS co, \
                (SELECT to_jsonb(d)::text FROM pg_database d WHERE d.datname = current_database()) AS db \
         FROM pg_attribute a JOIN pg_collation co ON co.oid = a.attcollation \
         WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped",
    )
    .bind(crate::dialect::postgres::quote_ident_path(table))
    .fetch_all(src.pool())
    .await
    .map_err(|e| crate::Error::Transfer(format!("sql: probing the collations of {table}: {e}")))?;
    let mut out = PgColl { locale: Vec::new(), nondet: Vec::new() };
    for r in rows {
        let name: String = r.get("name");
        let co: serde_json::Value = serde_json::from_str(&r.get::<String, _>("co")).unwrap_or_default();
        let db: serde_json::Value = r.get::<Option<String>, _>("db").and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
        let s = |v: &serde_json::Value, k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
        let cname = s(&co, "collname");
        let (provider, locale) = if cname == "default" {
            let p = match s(&db, "datlocprovider").as_str() {
                "" => "c".to_string(),
                p => p.to_string(),
            };
            let l = if p == "c" {
                s(&db, "datcollate")
            } else {
                [s(&db, "datlocale"), s(&db, "daticulocale"), s(&db, "datcollate")].into_iter().find(|x| !x.is_empty()).unwrap_or_default()
            };
            (p, l)
        } else {
            let p = match s(&co, "collprovider").as_str() {
                "" | "d" => "c".to_string(),
                p => p.to_string(),
            };
            let l = [s(&co, "colllocale"), s(&co, "colliculocale"), s(&co, "collcollate")].into_iter().find(|x| !x.is_empty()).unwrap_or_default();
            (p, l)
        };
        let shown = if cname == "default" { format!("default ({locale})") } else { format!("{cname} ({locale})") };
        let citext = r.get::<String, _>("ty").eq_ignore_ascii_case("citext");
        // The form a refusal shows and the oracle asks the server under.
        let shown = Locale::Pg { coll: if cname == "default" { String::new() } else { cname.clone() }, citext, shown }.encode();
        if co.get("collisdeterministic").and_then(|x| x.as_bool()) == Some(false) {
            out.nondet.push((name.clone(), Locale::shown(&shown)));
        }
        if !pg_bytes(&provider, &locale, &cname) {
            out.locale.push((name, shown));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs_the_bytes_order_as_the_collation_does() {
        let t = |a: &str, b: &str| pair_ok(a.as_bytes(), b.as_bytes(), false);
        // Agree.
        assert!(t("Supplier#000000009", "Supplier#000000010"), "identical punctuation in the prefix, digits decide");
        assert!(t("abc", "abd") && t("ABC", "ABD") && t("a1", "ab") && t("1", "a") && t("9", "A"));
        assert!(t("ab", "abc") && t("ab", "ab1"), "a prefix with a letter or digit after it");
        assert!(t("A", "b"), "folded a < b and bytes A < b");
        assert!(t("same", "same"), "a tie");
        // Refused.
        assert!(!t("B", "a"), "bytes B < a, the collation a < B");
        assert!(!t("A-D", "AC"), "glibc ignores the hyphen: AC < A-D");
        assert!(!t("ab", "ab-") && !t("ab", "ab "), "a rest of punctuation or space only");
        assert!(!t("a", "A") && !t("aB", "ab"), "the same letter in two cases");
        assert!(!t("e", "é") && !t("é", "f") && !t("éa", "éb"), "non-ASCII");
        assert!(!t("a b", "a-b") && !t("a b", "ab"), "space or punctuation at the first difference");
        assert!(!pair_ok(b"aa", b"ab", true), "a tailored collation: letters refused");
        assert!(pair_ok(b"a1", b"a2", true), "digits keep their rule");
    }

    #[test]
    fn tailored_collations_by_name() {
        for n in ["da_DK.utf8", "da-x-icu", "cs-CZ", "utf8mb4_da_0900_ai_ci", "utf8mb4_czech_ci", "latin2_croatian_ci", "tr_TR.UTF-8",
                  "es-u-co-trad", "utf8mb4_roman_ci", "lt_LT"] {
            assert!(tailored(n), "{n}");
        }
        for n in ["en_US.utf8", "und-x-icu", "de_DE.UTF-8", "utf8mb4_0900_ai_ci", "utf8mb4_general_ci", "latin1_swedish_ci",
                  "utf8mb4_unicode_ci", "fr-FR", "default"] {
            assert!(!tailored(n), "{n}");
        }
    }

    #[test]
    fn byte_order_collations() {
        assert!(pg_bytes("c", "C", "default") && pg_bytes("c", "POSIX", "x") && pg_bytes("c", "C.UTF-8", "x") && pg_bytes("b", "C.UTF-8", "x"));
        assert!(pg_bytes("c", "en_US.utf8", "C") && pg_bytes("i", "und", "ucs_basic"));
        assert!(!pg_bytes("c", "en_US.utf8", "default") && !pg_bytes("i", "und", "und-x-icu") && !pg_bytes("i", "C", "x"));
    }

    #[test]
    fn a_refusal_cuts_values_to_16_characters() {
        let e = refuse("s", "default (en_US.utf8)", b"0123456789abcdefXYZ", b"B").to_string();
        assert!(e.contains("\"0123456789abcdef…\"") && !e.contains("XYZ") && e.contains("COLLATE \"C\""), "{e}");
    }
}
