//! Every wire decoder, against input a peer controls.
//!
//! These decoders read bytes off a socket — from a Postgres walsender, a MySQL
//! binlog, a ClickHouse response — and feed lengths and offsets out of those
//! bytes into slicing and, in `arrowcol`, into raw pointer arithmetic. The
//! contract each of them has is one line long:
//!
//!   for ANY input, return `Ok` or `Err`; never panic, never hang, never
//!   allocate on a number the peer chose.
//!
//! Nothing was checking that. Two adversarial code reviews found three
//! violations by reading — a 4 GB allocation from an unchecked `u32`, a
//! panicking authentication conversation, and a truncated Stream Abort that
//! reopened a silent-data-loss path. Reading found them; reading will not find
//! the next one.
//!
//! This is a deterministic harness rather than `cargo-fuzz`, on purpose:
//!
//! * it needs no nightly toolchain, so it runs in the same container as
//!   everything else and inside the release gate;
//! * a failure is reproducible from its seed and prints the exact bytes,
//!   instead of a corpus file someone has to still be holding;
//! * the corpus is built from the VALID messages the other tests already
//!   construct, which is where the interesting shapes are — a random buffer
//!   is rejected by the first tag check, while a real message truncated one
//!   byte early reaches the code that trusts a length.
//!
//! It is not a substitute for a real fuzzer over a long run. It is the part of
//! one that can be a test.
//!
//! ## What it catches, and what it cannot
//!
//! Verified by planting one: an out-of-bounds index reached from a
//! peer-chosen count fails this harness immediately, naming the byte and the
//! value (`the len is 0 but the index is 65536`). That is the class the two
//! code reviews found, and it is now covered by a machine.
//!
//! It does NOT catch an over-large `Vec::with_capacity`. Reverting the cap on
//! the Truncate reserve — a real fix, made after a real finding — left this
//! harness green, because on Linux with overcommit a four-billion-element
//! reservation only claims ADDRESS SPACE: nothing touches the pages, nothing
//! fails, and the process carries on. (Which also means that class is milder
//! than it reads: the dangerous ones are the paths that WRITE, like
//! `BytesMut::zeroed` in the walsender frame reader and `resize` in the MySQL
//! packet reader — and those need a socket, so they are guarded by declared
//! caps and asserted by their own unit tests rather than here.)
//!
//! Worth knowing before trusting a green run on this file.

use super::arrowcol::{ArrowKind, BatchBuilder};
use super::mybinlog::{parse_rows, parse_table_map};
use super::pgoutput::{decode, RelOids};

/// xorshift64*. Written out rather than pulled in: this needs to produce the
/// same bytes on every machine and every release, and a dependency that
/// changes its algorithm silently rewrites the corpus.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
}

/// One valid message of every pgoutput shape this decoder claims to speak.
///
/// The point is coverage of the LENGTH-BEARING paths: a Relation with columns,
/// a tuple with text cells, a Truncate with a relation count. Those are where
/// a peer's number becomes an allocation or an index.
fn pgoutput_corpus() -> Vec<Vec<u8>> {
    let mut out = Vec::new();

    // Begin: final_lsn, commit_ts, xid.
    let mut b = vec![b'B'];
    b.extend_from_slice(&7u64.to_be_bytes());
    b.extend_from_slice(&99i64.to_be_bytes());
    b.extend_from_slice(&5u32.to_be_bytes());
    out.push(b);

    // Commit.
    let mut c = vec![b'C', 0];
    c.extend_from_slice(&7u64.to_be_bytes());
    c.extend_from_slice(&8u64.to_be_bytes());
    c.extend_from_slice(&99i64.to_be_bytes());
    out.push(c);

    // Relation: id, ns, name, replica identity, 2 columns.
    let mut r = vec![b'R'];
    r.extend_from_slice(&42u32.to_be_bytes());
    r.extend_from_slice(b"public\0t\0");
    r.push(b'd');
    r.extend_from_slice(&2u16.to_be_bytes());
    for name in ["id\0", "v\0"] {
        r.push(1);
        r.extend_from_slice(name.as_bytes());
        r.extend_from_slice(&23u32.to_be_bytes());
        r.extend_from_slice(&(-1i32).to_be_bytes());
    }
    out.push(r);

    // Insert with a two-cell tuple: one text, one null.
    let mut i = vec![b'I'];
    i.extend_from_slice(&42u32.to_be_bytes());
    i.push(b'N');
    i.extend_from_slice(&2u16.to_be_bytes());
    i.push(b't');
    i.extend_from_slice(&2u32.to_be_bytes());
    i.extend_from_slice(b"hi");
    i.push(b'n');
    out.push(i);

    // Update carrying an old image, and a delete carrying a key image.
    let mut u = vec![b'U'];
    u.extend_from_slice(&42u32.to_be_bytes());
    u.push(b'K');
    u.extend_from_slice(&1u16.to_be_bytes());
    u.push(b't');
    u.extend_from_slice(&1u32.to_be_bytes());
    u.extend_from_slice(b"7");
    u.push(b'N');
    u.extend_from_slice(&1u16.to_be_bytes());
    u.push(b'u'); // unchanged TOAST
    out.push(u);

    let mut d = vec![b'D'];
    d.extend_from_slice(&42u32.to_be_bytes());
    d.push(b'K');
    d.extend_from_slice(&1u16.to_be_bytes());
    d.push(b't');
    d.extend_from_slice(&1u32.to_be_bytes());
    d.extend_from_slice(b"7");
    out.push(d);

    // Truncate: a COUNT the peer chooses, then that many ids.
    let mut t = vec![b'T'];
    t.extend_from_slice(&2u32.to_be_bytes());
    t.push(0);
    t.extend_from_slice(&42u32.to_be_bytes());
    t.extend_from_slice(&43u32.to_be_bytes());
    out.push(t);

    // The streaming quartet.
    let mut ss = vec![b'S'];
    ss.extend_from_slice(&5u32.to_be_bytes());
    ss.push(1);
    out.push(ss);
    out.push(vec![b'E']);
    let mut sc = vec![b'c'];
    sc.extend_from_slice(&5u32.to_be_bytes());
    sc.push(0);
    sc.extend_from_slice(&7u64.to_be_bytes());
    sc.extend_from_slice(&8u64.to_be_bytes());
    out.push(sc);
    let mut sa = vec![b'A'];
    sa.extend_from_slice(&5u32.to_be_bytes());
    sa.extend_from_slice(&6u32.to_be_bytes());
    out.push(sa);

    out
}

/// Every mutation a broken or hostile peer produces, in the order they are
/// worth trying: a message cut short at each length, then single-bit and
/// whole-byte damage, then a length field replaced with the largest value it
/// can hold — the shape that turns a `u32` into an allocation.
fn mutations(seed: &[u8], rng: &mut Rng) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for n in 0..seed.len() {
        out.push(seed[..n].to_vec());
    }
    for _ in 0..24 {
        let mut m = seed.to_vec();
        if m.is_empty() {
            continue;
        }
        let i = rng.below(m.len());
        m[i] ^= 1 << rng.below(8);
        out.push(m);
    }
    for _ in 0..24 {
        let mut m = seed.to_vec();
        if m.is_empty() {
            continue;
        }
        let i = rng.below(m.len());
        m[i] = (rng.next() & 0xFF) as u8;
        out.push(m);
    }
    // Saturate every 4-byte window: whatever length field lives there now
    // claims the whole address space.
    for i in 0..seed.len().saturating_sub(4) {
        let mut m = seed.to_vec();
        m[i..i + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        out.push(m);
        let mut m2 = seed.to_vec();
        m2[i..i + 4].copy_from_slice(&(i32::MIN).to_be_bytes());
        out.push(m2);
    }
    out
}

#[test]
fn pgoutput_survives_anything_a_peer_can_send() {
    let mut rng = Rng(0x5EED_1234_5678_9ABC);
    let oids = RelOids::new();
    let (mut ok, mut err) = (0usize, 0usize);
    for seed in pgoutput_corpus() {
        for m in mutations(&seed, &mut rng) {
            for in_stream in [false, true] {
                // The contract is "Ok or Err" — the value is not the point,
                // the absence of a panic is.
                match decode(&bytes::Bytes::from(m.clone()), in_stream, &oids) {
                    Ok(_) => ok += 1,
                    Err(_) => err += 1,
                }
            }
        }
    }
    for _ in 0..4000 {
        let len = rng.below(64);
        let buf: Vec<u8> = (0..len).map(|_| (rng.next() & 0xFF) as u8).collect();
        match decode(&bytes::Bytes::from(buf), rng.next() & 1 == 0, &oids) {
            Ok(_) => ok += 1,
            Err(_) => err += 1,
        }
    }
    // The harness has to prove its own reach. If everything errored, this
    // tested the tag dispatch and nothing behind it; if nothing errored, the
    // mutations are not damaging anything. Both halves have to be substantial
    // — that is the difference between a test and a green light.
    assert!(
        ok > 200 && err > 2_000,
        "torture reached too little: {ok} accepted, {err} rejected — a corpus \
         that only errors is testing the first byte"
    );
}

/// `BatchBuilder::push` is where a peer's bytes meet raw pointer arithmetic:
/// it walks a Postgres binary COPY stream IN PLACE, and the file carries
/// seventeen `unsafe` blocks reading through offsets staged from that stream.
#[test]
fn batch_builder_survives_anything_a_peer_can_send() {
    // A valid-ish binary COPY prologue plus one tuple, then everything that
    // can be done to it.
    let mut seed = Vec::new();
    seed.extend_from_slice(b"PGCOPY\n\xff\r\n\0"); // signature
    seed.extend_from_slice(&0u32.to_be_bytes()); // flags
    seed.extend_from_slice(&0u32.to_be_bytes()); // header extension
    seed.extend_from_slice(&2u16.to_be_bytes()); // 2 fields
    seed.extend_from_slice(&8u32.to_be_bytes());
    seed.extend_from_slice(&7i64.to_be_bytes());
    seed.extend_from_slice(&2u32.to_be_bytes());
    seed.extend_from_slice(b"hi");

    let mut rng = Rng(0xC0FF_EE00_1234_5678);
    let kinds = vec![ArrowKind::Int64, ArrowKind::Utf8];
    let (mut ok, mut err) = (0usize, 0usize);
    for m in mutations(&seed, &mut rng) {
        let mut b = BatchBuilder::new(kinds.clone(), 1 << 20);
        match b.push(&m) {
            Ok(()) => ok += 1,
            Err(_) => err += 1,
        }
        // Split delivery too: the straddling-tuple path is a different walk,
        // and it is the one that buffers.
        if m.len() > 3 {
            let cut = 1 + rng.below(m.len() - 2);
            let mut b2 = BatchBuilder::new(kinds.clone(), 1 << 20);
            if b2.push(&m[..cut]).is_ok() {
                let _ = b2.push(&m[cut..]);
            }
        }
    }
    for _ in 0..2000 {
        let len = rng.below(96);
        let buf: Vec<u8> = (0..len).map(|_| (rng.next() & 0xFF) as u8).collect();
        let mut b = BatchBuilder::new(kinds.clone(), 1 << 20);
        match b.push(&buf) {
            Ok(()) => ok += 1,
            Err(_) => err += 1,
        }
    }
    // Same reach check. A COPY stream whose signature is damaged is rejected
    // immediately, so a run that is ALL errors never reached the tuple walk —
    // which is the part with the pointer arithmetic.
    assert!(
        ok > 50 && err > 100,
        "torture reached too little: {ok} accepted, {err} rejected"
    );
}

/// A TABLE_MAP the reader accepts: two columns, LONG and VARCHAR.
fn table_map_seed() -> Vec<u8> {
    let mut tm = Vec::new();
    tm.extend_from_slice(&[42, 0, 0, 0, 0, 0]); // table id, 6 bytes LE
    tm.extend_from_slice(&[0, 0]); // flags
    tm.push(5);
    tm.extend_from_slice(b"bench\0");
    tm.push(1);
    tm.extend_from_slice(b"t\0");
    tm.push(2); // column count
    tm.extend_from_slice(&[0x03, 0x0F]); // LONG, VARCHAR
    tm.push(2); // metadata blob length
    tm.extend_from_slice(&[0xFF, 0x00]); // VARCHAR max length
    tm.push(0x02); // null bitmap
    tm
}

/// The MySQL binlog reader is the same kind of surface as pgoutput, from a
/// different server: a TABLE_MAP declares the shape and a rows event carries
/// values against it. A map whose column count disagrees with its metadata
/// blob is exactly what a truncating relay produces.
#[test]
fn mysql_table_map_survives_anything_a_server_can_send() {
    let mut rng = Rng(0xB1_0106_1234_5678);
    let (mut ok, mut err) = (0usize, 0usize);
    for m in mutations(&table_map_seed(), &mut rng) {
        match parse_table_map(&m) {
            Ok(_) => ok += 1,
            Err(_) => err += 1,
        }
    }
    for _ in 0..3000 {
        let len = rng.below(80);
        let buf: Vec<u8> = (0..len).map(|_| (rng.next() & 0xFF) as u8).collect();
        match parse_table_map(&buf) {
            Ok(_) => ok += 1,
            Err(_) => err += 1,
        }
    }
    assert!(
        ok > 5 && err > 500,
        "table-map torture reached too little: {ok} accepted, {err} rejected"
    );
}

/// Rows events decoded against a VALID map: the map is the contract the row
/// payload is trusted against, so damaging the payload alone is the case
/// where a length inside the row meets a width declared outside it.
#[test]
fn mysql_rows_survive_anything_a_server_can_send() {
    let map = parse_table_map(&table_map_seed()).expect("the seed map must parse");
    let mut rows = Vec::new();
    rows.extend_from_slice(&[42, 0, 0, 0, 0, 0]); // table id
    rows.extend_from_slice(&[0, 0]); // flags
    rows.extend_from_slice(&[2, 0]); // extra-data length (v2 header)
    rows.push(2); // column count, packed
    rows.push(0x03); // present bitmap
    rows.push(0x00); // null bitmap
    rows.extend_from_slice(&7i32.to_le_bytes());
    rows.push(2);
    rows.extend_from_slice(b"hi");

    let mut rng = Rng(0xB1_0106_1234_5679);
    let (mut ok, mut err) = (0usize, 0usize);
    for kind in [30u8, 31, 32, 23, 24, 25] {
        for m in mutations(&rows, &mut rng) {
            match parse_rows(&m, kind, &map) {
                Ok(_) => ok += 1,
                Err(_) => err += 1,
            }
        }
    }
    for _ in 0..3000 {
        let len = rng.below(64);
        let buf: Vec<u8> = (0..len).map(|_| (rng.next() & 0xFF) as u8).collect();
        match parse_rows(&buf, 30, &map) {
            Ok(_) => ok += 1,
            Err(_) => err += 1,
        }
    }
    assert!(
        err > 500,
        "rows torture reached too little: {ok} accepted, {err} rejected"
    );
}
