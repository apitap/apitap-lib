//! Every identifier apitap creates next to a user's table, in one place.
//!
//! # Why this module exists
//!
//! Names used to be built where they were needed: `format!("{bare}__apitap_
//! staging")` in the Postgres sink, the same string again in the MySQL sink,
//! `{dest_table}__apitap_cl` in two destinations, and so on. Nine distinct
//! suffixes across the tree. Nothing enumerated them, so nothing could check
//! anything about them as a set — and three separate defects came out of that
//! one gap:
//!
//! * **Truncation.** Postgres cuts identifiers at 63 bytes without complaint,
//!   so a 63-character destination produced a staging name identical to the
//!   destination. `prepare`'s unconditional DROP then aimed at the destination
//!   itself, and the run reported failure *after* replacing it.
//! * **Namespace drift.** `dispatch` reserved exactly two of the nine suffixes,
//!   so a user table named like any of the other seven artifacts would be
//!   silently destroyed by a sibling's run.
//! * **Patch-by-patch discovery.** The truncation was fixed for
//!   `__apitap_staging`, and only then did a test reveal `__apitap_old`
//!   overflowing too. There was no reason to believe there was not a third.
//!
//! The fix is not another careful `format!`. It is making the SET of artifacts
//! a thing the compiler knows about: [`Artifact::ALL`] is the source of truth,
//! every name goes through [`artifact_ident`], and the invariants are tested
//! across the whole enum rather than on the examples someone thought of. A new
//! artifact gets length safety, namespace reservation and discovery-exclusion
//! by existing, not by someone remembering three call sites.

use md5::Digest;

/// An object apitap creates beside a destination table.
///
/// **Adding a variant is the only supported way to add an artifact.** The
/// namespace reservation in `pipeline::dispatch`, the exclusion lists in table
/// discovery, and the length-safety tests all iterate [`Artifact::ALL`], so a
/// new one is covered the moment it is declared — and cannot be half-covered,
/// which is the state the tree was in before this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Artifact {
    /// Rows land here first; the swap renames it onto the destination.
    Staging,
    /// MySQL parks the outgoing table here between its two RENAMEs.
    Old,
    /// ClickHouse's swap target for a replace.
    New,
    /// Changelog scratch table for a window's apply.
    ChangelogTmp,
    /// The delete-marker sidecar the ClickHouse CDC apply keeps.
    CdcDelete,
    /// The view that derives current state from a changelog table.
    Current,
}

impl Artifact {
    /// Every artifact kind. Iterate this; never write a literal list.
    pub(crate) const ALL: &'static [Artifact] = &[
        Artifact::Staging,
        Artifact::Old,
        Artifact::New,
        Artifact::ChangelogTmp,
        Artifact::CdcDelete,
        Artifact::Current,
    ];

    /// May apitap claim this suffix as its own?
    ///
    /// Every suffix but one contains the word `apitap`, which makes it a
    /// namespace: a user table ending in `__apitap_staging` is a collision
    /// worth refusing, and hiding such a name from discovery cannot hide
    /// anybody's real data.
    ///
    /// `__current` is different and must not be treated the same way. It is
    /// not namespaced, `orders__current` is a perfectly ordinary thing for a
    /// person to call a table, and reserving it would refuse to transfer real
    /// data. It also does not need reserving: the object apitap creates there
    /// is a VIEW, and discovery already lists only base tables and
    /// materialized views.
    ///
    /// The distinction is the reason this is a method and not a blanket rule
    /// over `ALL` — a hand-written list would have gotten it wrong in one
    /// direction or the other, and this one is wrong in the direction that
    /// loses data.
    pub(crate) const fn reserved(self) -> bool {
        !matches!(self, Artifact::Current)
    }

    pub(crate) const fn suffix(self) -> &'static str {
        match self {
            Artifact::Staging => "__apitap_staging",
            Artifact::Old => "__apitap_old",
            Artifact::New => "__apitap_new",
            Artifact::ChangelogTmp => "__apitap_cl",
            Artifact::CdcDelete => "__apitap_cdc_del",
            Artifact::Current => "__current",
        }
    }
}

/// Identifier byte limits per dialect. Postgres is `NAMEDATALEN - 1`; MySQL
/// allows 64 bytes for a table name. ClickHouse and BigQuery are far longer
/// than anything reachable here, so they use [`ROOMY`].
pub(crate) const PG_IDENT_MAX: usize = 63;
pub(crate) const MY_IDENT_MAX: usize = 64;
pub(crate) const ROOMY: usize = 1024;

/// Bytes of md5 hex mixed in when a name has to be shortened.
const HASH: usize = 8;

/// `<bare><suffix>`, guaranteed to fit `limit` and to stay distinct.
///
/// When the whole name fits, it is exactly `bare + suffix` — the shape every
/// existing deployment already has on disk, so this is not a migration.
///
/// When it does not fit, the BARE part is what gives way. That direction is
/// deliberate and load-bearing: the suffix is what makes an object recognisable
/// as apitap's, which is what the sweeps, the namespace reservation and the
/// discovery-exclusion all match on. Truncating the suffix instead — which is
/// what the databases do on their own — is precisely how a staging name came to
/// equal the table it was staging.
pub(crate) fn artifact_ident(bare: &str, artifact: Artifact, limit: usize) -> String {
    let suffix = artifact.suffix();
    if bare.len() + suffix.len() <= limit {
        return format!("{bare}{suffix}");
    }
    // `limit` counts bytes and `bare` may be UTF-8, so cut on a char boundary.
    let room = limit.saturating_sub(suffix.len() + HASH + 1);
    let mut head = String::with_capacity(room);
    for c in bare.chars() {
        if head.len() + c.len_utf8() > room {
            break;
        }
        head.push(c);
    }
    // Hash the suffix in too: two artifacts of the same over-long table share a
    // head, and only the suffix would tell them apart otherwise.
    let h = hex::encode(md5::Md5::digest(format!("{bare}{suffix}").as_bytes()));
    format!("{head}_{}{suffix}", &h[..HASH])
}

/// Does this name look like something apitap made?
///
/// Used by table discovery and by the namespace reservation, so both answer the
/// question the same way and neither can fall behind [`Artifact::ALL`].
pub(crate) fn is_artifact(name: &str) -> bool {
    name == STATE_TABLE
        || Artifact::ALL
            .iter()
            .filter(|a| a.reserved())
            .any(|a| name.ends_with(a.suffix()))
}

/// A SQL predicate that hides every apitap artifact from table discovery.
///
/// Generated, not written out. The hand-written versions listed three of the
/// eight artifacts, so a `schema=` transfer happily picked up
/// `orders__apitap_cl`, `orders__apitap_cdc_del`, `orders__apitap_new` and
/// `orders__current` and replicated apitap's own scratch objects as if a user
/// had made them. Deriving the clause from [`Artifact::ALL`] means a new
/// artifact disappears from discovery the moment it is declared.
///
/// `_` is a LIKE wildcard and every suffix is full of them, so each one is
/// escaped — with a backslash for Postgres, and with `|` plus an explicit
/// `ESCAPE` for MySQL, which does not enable backslash escaping by default
/// under `NO_BACKSLASH_ESCAPES`.
pub(crate) fn sql_exclusion(col: &str, dialect: Dialect) -> String {
    let mut out = String::new();
    for a in Artifact::ALL.iter().filter(|a| a.reserved()) {
        let (pat, esc) = match dialect {
            Dialect::Postgres => (a.suffix().replace('_', "\\_"), String::new()),
            Dialect::MySql => (a.suffix().replace('_', "|_"), " ESCAPE '|'".to_string()),
        };
        out.push_str(&format!(" AND {col} NOT LIKE '%{pat}'{esc}"));
    }
    out.push_str(&format!(" AND {col} <> '{STATE_TABLE}'"));
    out
}

/// Which LIKE-escaping convention to generate for.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Dialect {
    Postgres,
    MySql,
}

/// The state table's name is fixed — it is per-destination, not per-table, so
/// it never needs shortening.
pub(crate) const STATE_TABLE: &str = "_apitap_state";

#[cfg(test)]
mod tests {
    use super::*;

    /// The invariants below run over `Artifact::ALL`, not over a handful of
    /// examples. That is the point of the enum: a new artifact is tested by
    /// existing, so the next `__apitap_old` cannot slip through by being one
    /// nobody thought to write a case for.
    const LIMITS: &[usize] = &[PG_IDENT_MAX, MY_IDENT_MAX];

    #[test]
    fn every_artifact_fits_every_limit_at_every_length() {
        for &a in Artifact::ALL {
            for &lim in LIMITS {
                for n in [1usize, 10, 40, 46, 47, 48, 62, 63, 64, 100, 500] {
                    let bare = "t".repeat(n);
                    let out = artifact_ident(&bare, a, lim);
                    assert!(out.len() <= lim,
                            "{:?} at bare={n} limit={lim}: {} bytes", a, out.len());
                }
            }
        }
    }

    /// The failure that started this module: a name that shortens onto the
    /// table it belongs to. `DROP TABLE IF EXISTS <staging>` then drops the
    /// destination.
    #[test]
    fn an_artifact_is_never_equal_to_the_table_it_belongs_to() {
        for &a in Artifact::ALL {
            for &lim in LIMITS {
                for n in 1..=(lim + 20) {
                    let bare = "t".repeat(n);
                    assert_ne!(artifact_ident(&bare, a, lim), bare,
                               "{:?} at bare={n} limit={lim}", a);
                }
            }
        }
    }

    /// Two artifacts of the SAME table must never collide, or a swap renames
    /// the wrong object onto the destination.
    #[test]
    fn two_artifacts_of_one_table_never_share_a_name() {
        for &lim in LIMITS {
            for n in [10usize, 50, 63, 64, 200] {
                let bare = "t".repeat(n);
                let mut seen = std::collections::HashSet::new();
                for &a in Artifact::ALL {
                    let name = artifact_ident(&bare, a, lim);
                    assert!(seen.insert(name.clone()),
                            "{:?} collides at bare={n} limit={lim}: {name}", a);
                }
            }
        }
    }

    /// Two DIFFERENT tables must never share an artifact, however long a
    /// prefix they have in common — that was the second half of the same bug.
    #[test]
    fn two_tables_never_share_an_artifact() {
        for &a in Artifact::ALL {
            for &lim in LIMITS {
                let x = format!("{}alpha", "p".repeat(lim));
                let y = format!("{}beta1", "p".repeat(lim));
                assert_ne!(artifact_ident(&x, a, lim), artifact_ident(&y, a, lim),
                           "{:?} at limit={lim}", a);
            }
        }
    }

    /// The suffix has to survive at the END, because every sweep, reservation
    /// and exclusion in the tree matches on it.
    #[test]
    fn the_suffix_always_survives_and_stays_terminal() {
        for &a in Artifact::ALL {
            for &lim in LIMITS {
                for n in [1usize, 63, 300] {
                    let out = artifact_ident(&"z".repeat(n), a, lim);
                    assert!(out.ends_with(a.suffix()), "{:?}: {out}", a);
                    // Only the namespaced ones are recognised as apitap's —
                    // `__current` is deliberately not, so that a user table
                    // called `orders__current` is left alone.
                    assert_eq!(is_artifact(&out), a.reserved(),
                               "{:?} recognition disagrees with reserved(): {out}", a);
                }
            }
        }
    }

    /// Deterministic, or a crashed run's leftovers can never be swept.
    #[test]
    fn the_same_inputs_always_give_the_same_name() {
        for &a in Artifact::ALL {
            let bare = "z".repeat(200);
            assert_eq!(artifact_ident(&bare, a, PG_IDENT_MAX),
                       artifact_ident(&bare, a, PG_IDENT_MAX));
        }
    }

    /// A short name keeps exactly the shape deployments already have on disk,
    /// so adopting this module is not a migration.
    #[test]
    fn short_names_are_unchanged_from_the_historical_format() {
        assert_eq!(artifact_ident("orders", Artifact::Staging, PG_IDENT_MAX),
                   "orders__apitap_staging");
        assert_eq!(artifact_ident("orders", Artifact::Old, MY_IDENT_MAX),
                   "orders__apitap_old");
    }

    /// The generated exclusion must name EVERY artifact, in both dialects.
    /// The hand-written clauses it replaces listed three of eight; this test is
    /// the thing that makes that impossible to repeat.
    #[test]
    fn the_sql_exclusion_covers_every_artifact() {
        for d in [Dialect::Postgres, Dialect::MySql] {
            let sql = sql_exclusion("c.relname", d);
            for a in Artifact::ALL.iter().filter(|a| a.reserved()) {
                // The suffix appears with its underscores escaped, so compare
                // on the escaped form rather than the raw one.
                let esc = match d {
                    Dialect::Postgres => a.suffix().replace('_', "\\_"),
                    Dialect::MySql => a.suffix().replace('_', "|_"),
                };
                assert!(sql.contains(&esc), "{:?} missing from {:?}: {sql}", a, d);
            }
            assert!(sql.contains(STATE_TABLE), "state table missing: {sql}");
        }
    }

    /// `__current` must stay OUT of the reserved set: it is not namespaced,
    /// and refusing every `*__current` table would refuse real user data.
    #[test]
    fn the_unnamespaced_suffix_is_not_reserved() {
        assert!(!Artifact::Current.reserved());
        assert!(!is_artifact("orders__current"), "would refuse a real table");
        assert!(is_artifact("orders__apitap_staging"));
        for a in Artifact::ALL.iter().filter(|a| a.reserved()) {
            assert!(a.suffix().contains("apitap"),
                    "{:?} is reserved but not namespaced — reserving it could \
                     hide a user's table", a);
        }
    }

    /// Multi-byte names must not be cut through a character.
    #[test]
    fn a_utf8_name_is_cut_on_a_character_boundary() {
        for &a in Artifact::ALL {
            let out = artifact_ident(&"é".repeat(100), a, PG_IDENT_MAX);
            assert!(out.len() <= PG_IDENT_MAX);
            assert!(out.ends_with(a.suffix()));
        }
    }
}
