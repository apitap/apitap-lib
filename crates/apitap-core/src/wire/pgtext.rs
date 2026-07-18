//! PostgreSQL text-COPY field conventions: the `\N` NULL marker and the
//! backslash escape set — the vocabulary of `WireFormat::TabSeparated` as a
//! Postgres source emits it.


/// Un-escape one PostgreSQL text-COPY field into `out`. Only called when a
/// backslash was seen — the common case borrows the raw bytes.
pub(crate) fn unescape_into(field: &[u8], out: &mut Vec<u8>) {
    let mut i = 0;
    while i < field.len() {
        // Bulk-copy up to the next backslash (rare in real data).
        let mut j = i;
        while j < field.len() && field[j] != b'\\' {
            j += 1;
        }
        out.extend_from_slice(&field[i..j]);
        if j >= field.len() {
            break;
        }
        if j + 1 < field.len() {
            out.push(match field[j + 1] {
                b'b' => 0x08,
                b'f' => 0x0c,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'v' => 0x0b,
                other => other, // covers \\ and any literal escape
            });
            i = j + 2;
        } else {
            out.push(b'\\');
            i = j + 1;
        }
    }
}
