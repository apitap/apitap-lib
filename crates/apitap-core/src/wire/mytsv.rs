//! MySQL `LOAD DATA` TSV field escaping — the `FIELDS ESCAPED BY '\\'` dialect
//! shared by every producer that feeds the MySQL sink (the MySQL source's text
//! lane, the Google Sheets source).

/// Escape one field for MySQL `LOAD DATA ... FIELDS ESCAPED BY '\\'`: backslash,
/// the tab/newline delimiters, CR, NUL and 0x1A get a `\`-prefix; everything else
/// (UTF-8 text, or uppercase HEX for binary columns) rides literally. NULL is
/// written by the caller as the whole field `\N`, never routed here.
pub(crate) fn tsv_escape(field: &[u8], out: &mut Vec<u8>) {
    let mut i = 0;
    while i < field.len() {
        // Bulk-copy the run of clean bytes (the overwhelming common case).
        let start = i;
        while i < field.len() && !matches!(field[i], b'\\' | b'\t' | b'\n' | b'\r' | 0 | 0x1a) {
            i += 1;
        }
        out.extend_from_slice(&field[start..i]);
        if i == field.len() {
            break;
        }
        out.push(b'\\');
        out.push(match field[i] {
            b'\t' => b't',
            b'\n' => b'n',
            b'\r' => b'r',
            0 => b'0',
            0x1a => b'Z',
            other => other, // backslash
        });
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn esc(b: &[u8]) -> String {
        let mut out = Vec::new();
        tsv_escape(b, &mut out);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn tsv_escape_matches_load_data_dialect() {
        assert_eq!(esc(b"hello"), "hello"); // clean bytes ride literally
        assert_eq!(esc(b"a\tb"), "a\\tb"); // tab -> backslash-t
        assert_eq!(esc(b"a\nb"), "a\\nb"); // newline -> backslash-n
        assert_eq!(esc(b"a\\b"), "a\\\\b"); // backslash doubles
        assert_eq!(esc(b"a\rb"), "a\\rb"); // CR -> backslash-r
        assert_eq!(esc(&[b'a', 0, b'b']), "a\\0b"); // NUL -> backslash-0
        assert_eq!(esc(&[b'a', 0x1a, b'b']), "a\\Zb"); // 0x1A -> backslash-Z
        assert_eq!(esc(b""), ""); // empty stays empty (NULL is \N, written elsewhere)
        assert_eq!(esc(b"4869"), "4869"); // HEX output (uppercase hex) untouched
    }
}
