//! The codegen ORACLE: a hand-written decoder for one fixed schema — the
//! upper bound of what a perfect JIT would generate (widths baked, no
//! staging arrays, no dispatch). Racing it against the production
//! `BatchBuilder` on identical bytes bounds the whole codegen project:
//! if even the oracle can't beat the transpose meaningfully, JIT is dead.
//!
//!     cargo run --release -p apitap-core --example decode_oracle
//!
//! Schema mirrors bench_data_50m's shape: i64, utf8, utf8, utf8, i16,
//! i32, i64, f64, utf8(dec-ish skipped→f64), bool, date, ts, ts, utf8,
//! utf8 — numeric intentionally replaced by f64 to keep the oracle at
//! the FIXED-WIDTH best case (JIT would call back for numeric anyway).

use apitap_core::{ArrowBatch, ArrowKind, FinishedCol};

fn be16(v: i16, out: &mut Vec<u8>) {
    out.extend((2i32).to_be_bytes());
    out.extend(v.to_be_bytes());
}
fn be32(v: i32, out: &mut Vec<u8>) {
    out.extend((4i32).to_be_bytes());
    out.extend(v.to_be_bytes());
}
fn be64(v: i64, out: &mut Vec<u8>) {
    out.extend((8i32).to_be_bytes());
    out.extend(v.to_be_bytes());
}
fn bef64(v: f64, out: &mut Vec<u8>) {
    out.extend((8i32).to_be_bytes());
    out.extend(v.to_be_bytes());
}
fn btext(s: &str, out: &mut Vec<u8>) {
    out.extend((s.len() as i32).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}
fn bbool(v: bool, out: &mut Vec<u8>) {
    out.extend((1i32).to_be_bytes());
    out.push(v as u8);
}

const NCOLS: usize = 15;

/// One synthetic COPY-binary stream: header + N tuples + trailer.
fn synth(rows: usize) -> Vec<u8> {
    let mut s = Vec::with_capacity(rows * 160);
    s.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    s.extend(0u32.to_be_bytes());
    s.extend(0u32.to_be_bytes());
    for i in 0..rows as i64 {
        s.extend((NCOLS as i16).to_be_bytes());
        be64(i, &mut s);
        btext(&format!("sm-{}", i % 97), &mut s);
        btext(&format!("medium-string-{}", i % 991), &mut s);
        btext(&format!("larger-string-payload-{}-{}", i, i % 7919), &mut s);
        be16((i % 32000) as i16, &mut s);
        be32(i as i32, &mut s);
        be64(i.wrapping_mul(7919), &mut s);
        bef64(i as f64 * 0.5, &mut s);
        bef64(i as f64 * 1.25, &mut s);
        bbool(i % 3 == 0, &mut s);
        be32((i % 20000) as i32, &mut s);
        be64(i * 1_000_000, &mut s);
        be64(i * 1_000_000 + 1, &mut s);
        btext(&format!("{{\"k\":{}}}", i % 1000), &mut s);
        btext(&format!("extra-{}", i % 313), &mut s);
    }
    s.extend((-1i16).to_be_bytes());
    s
}

/// The oracle: fixed schema baked in, single pass per row, direct column
/// writes, zero staging, zero dispatch. All-valid fast assumptions the
/// JIT would also specialize on (falls back never — synth has no NULLs).
#[derive(Default)]
struct Oracle {
    c0: Vec<i64>,
    c1o: Vec<i32>, c1d: Vec<u8>,
    c2o: Vec<i32>, c2d: Vec<u8>,
    c3o: Vec<i32>, c3d: Vec<u8>,
    c4: Vec<i16>,
    c5: Vec<i32>,
    c6: Vec<i64>,
    c7: Vec<f64>,
    c8: Vec<f64>,
    c9: Vec<u8>,
    c10: Vec<i32>,
    c11: Vec<i64>,
    c12: Vec<i64>,
    c13o: Vec<i32>, c13d: Vec<u8>,
    c14o: Vec<i32>, c14d: Vec<u8>,
    rows: usize,
}

impl Oracle {
    fn new() -> Self {
        let mut o = Self::default();
        for off in [&mut o.c1o, &mut o.c2o, &mut o.c3o, &mut o.c13o, &mut o.c14o] {
            off.push(0);
        }
        o
    }

    /// Returns bytes consumed (stops at a partial tuple or the trailer).
    #[inline(never)]
    fn push(&mut self, b: &[u8]) -> usize {
        let mut pos = 0usize;
        loop {
            if b.len() - pos < 2 {
                return pos;
            }
            let nc = i16::from_be_bytes(b[pos..pos + 2].try_into().unwrap());
            if nc == -1 {
                return pos;
            }
            let mut o = pos + 2;
            // Bounds check once per row with a conservative minimum, then
            // per-varlen exact checks — the shape baked code would take.
            macro_rules! need {
                ($n:expr) => {
                    if b.len() - o < $n {
                        return pos;
                    }
                };
            }
            macro_rules! fixed {
                ($t:ty, $w:literal, $dst:expr) => {{
                    need!(4 + $w);
                    let p = unsafe { b.as_ptr().add(o + 4) };
                    $dst.push(<$t>::from_be_bytes(unsafe {
                        p.cast::<[u8; $w]>().read()
                    }));
                    o += 4 + $w;
                }};
            }
            macro_rules! varlen {
                ($off:expr, $dat:expr) => {{
                    need!(4);
                    let l = i32::from_be_bytes(b[o..o + 4].try_into().unwrap()) as usize;
                    o += 4;
                    need!(l);
                    $dat.extend_from_slice(&b[o..o + l]);
                    $off.push($dat.len() as i32);
                    o += l;
                }};
            }
            fixed!(i64, 8, self.c0);
            varlen!(self.c1o, self.c1d);
            varlen!(self.c2o, self.c2d);
            varlen!(self.c3o, self.c3d);
            fixed!(i16, 2, self.c4);
            fixed!(i32, 4, self.c5);
            fixed!(i64, 8, self.c6);
            fixed!(f64, 8, self.c7);
            fixed!(f64, 8, self.c8);
            {
                need!(5);
                let v = b[o + 4] != 0;
                let r = self.rows;
                if r % 8 == 0 {
                    self.c9.push(0);
                }
                if v {
                    *self.c9.last_mut().unwrap() |= 1 << (r % 8);
                }
                o += 5;
            }
            fixed!(i32, 4, self.c10);
            fixed!(i64, 8, self.c11);
            fixed!(i64, 8, self.c12);
            varlen!(self.c13o, self.c13d);
            varlen!(self.c14o, self.c14d);
            self.rows += 1;
            pos = o;
        }
    }
}

fn main() {
    let rows = 2_000_000usize;
    println!("synthesizing {rows} rows…");
    let wire = synth(rows);
    println!("wire: {:.1} MB", wire.len() as f64 / 1e6);
    let chunks: Vec<&[u8]> = wire.chunks(256 << 10).collect();

    // Leg A: production BatchBuilder (transpose, staging, dispatch/64).
    let kinds = vec![
        ArrowKind::Int64,
        ArrowKind::Utf8,
        ArrowKind::Utf8,
        ArrowKind::Utf8,
        ArrowKind::Int16,
        ArrowKind::Int32,
        ArrowKind::Int64,
        ArrowKind::Float64,
        ArrowKind::Float64,
        ArrowKind::Bool,
        ArrowKind::Int32,
        ArrowKind::Int64,
        ArrowKind::Int64,
        ArrowKind::Utf8,
        ArrowKind::Utf8,
    ];
    let mut best_a = f64::MAX;
    let mut total_a = 0u64;
    for _ in 0..3 {
        let mut b = apitap_core::BatchBuilder::new(kinds.clone(), 8 << 20);
        let t0 = std::time::Instant::now();
        for c in &chunks {
            b.push(c).unwrap();
            while let Some(batch) = b.take_ready().unwrap() {
                total_a += batch.rows as u64;
                std::hint::black_box(&batch);
            }
        }
        if let Some(batch) = b.finish().unwrap() {
            total_a += batch.rows as u64;
            std::hint::black_box(&batch);
        }
        best_a = best_a.min(t0.elapsed().as_secs_f64());
    }

    // Leg B: the oracle — fed the same bytes minus the 19-byte span
    // header (a JIT prologue would skip it identically).
    let body = &wire[19..];
    let chunks_b: Vec<&[u8]> = body.chunks(256 << 10).collect();
    let mut best_b = f64::MAX;
    let mut total_b = 0u64;
    for _ in 0..3 {
        let mut o = Oracle::new();
        let t0 = std::time::Instant::now();
        let mut carry: Vec<u8> = Vec::new();
        for c in &chunks_b {
            if carry.is_empty() {
                let used = o.push(c);
                if used < c.len() {
                    carry.extend_from_slice(&c[used..]);
                }
            } else {
                carry.extend_from_slice(c);
                let used = o.push(&carry);
                carry.drain(..used);
            }
        }
        total_b += o.rows as u64;
        std::hint::black_box(&o);
        best_b = best_b.min(t0.elapsed().as_secs_f64());
    }

    let gbs_a = wire.len() as f64 / 1e9 / best_a;
    let gbs_b = wire.len() as f64 / 1e9 / best_b;
    println!("production builder: {best_a:.3}s  ({gbs_a:.2} GB/s)  rows={total_a}");
    println!("oracle (baked)    : {best_b:.3}s  ({gbs_b:.2} GB/s)  rows={total_b}");
    println!("oracle speedup    : {:.2}x", best_a / best_b);
    let _ = ArrowBatch { rows: 0, cols: Vec::<FinishedCol>::new() };
}
