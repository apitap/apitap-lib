//! MySQL dialect vocabulary shared by [`crate::source::mysql`] and
//! [`crate::sink::mysql`]: identifier quoting and the binary-type set that must
//! agree column-for-column between the source SELECT (HEX) and the sink
//! LOAD DATA (UNHEX).

/// MySQL udts whose bytes aren't safe as connection-charset text — the TSV lane
/// ships them HEX-encoded and the sink UNHEXes them, so binary round-trips exactly.
pub(crate) fn is_binary_udt(udt: &str) -> bool {
    matches!(
        udt,
        "blob"
            | "tinyblob"
            | "mediumblob"
            | "longblob"
            | "binary"
            | "varbinary"
            | "bit"
            | "geometry"
    )
}

/// `` ` ``-quote a MySQL identifier / dotted path.
pub(crate) fn my_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}
pub(crate) fn my_ident_path(path: &str) -> String {
    path.split('.').map(my_ident).collect::<Vec<_>>().join(".")
}
