# Desain: 3 juta changes/menit di 0,5 core / 256 MB — eksekusi gelombang 0.61

Status: rencana eksekusi untuk `docs/review/2026-10-07-system-review-3jt-per-menit.md` §5.
Target: **50.000 changes/s (3 jt/menit)** pg→ClickHouse, *catch-up* dan *keep-up*,
di kandang **0,5 CPU / 256 MB**, checksum 30/30, dan **naik saat resource naik**.

## 0. Di mana kita sekarang (angka, bukan perasaan)

| metrik | terukur | sumber |
|---|---|---|
| biaya CPU per change (census 6,45 jt changes) | **57 µs/change** | laporan 0.58 §5 |
| grup 30 tabel, catch-up @0,5 CPU | **31.679 ch/s = 1,90 jt/menit** | `cdc-steady-30t-0.58.md:376` |
| grup 30 tabel, paced/keep-up | ~**1,50 jt/menit** | `:398` |
| single-table 0.59 (wheel rilis) | penulis 2,19 jt/menit dikejar @0,40 core, 101 MB | leg b59-single2 |

Matematika target: 50.000/s × 0,5 core = **10 µs/change** anggaran total. Dari 57 µs
berarti **5,7×**; jalur estimasi review (L1a…L6, §2 di bawah) membawa ke 7,3–13,3 µs
= 42–65 rb/s = **2,5–3,9 jt/menit** — cukup dengan margin.

## 1. Peta jalur panas (kode nyata, satu change)

```
socket ──read_frame (walsender.rs:163)──> pump_frames (:98) ──mpsc+permit(G0.2)──>
drain loop (drain.rs:166) ──pgoutput::decode (pgoutput.rs:381)──> tx_buf/streams ──>
Collapser (collapse.rs) | Changes (changelog.rs) ──seal (window.rs)──>
apply lanes (dest_ch.rs) ──RowBinary──> HTTP ──> CH
```

Profil 0.58 (dikelompokkan penuh, §5 review):

| komponen | porsi | catatan |
|---|---|---|
| kernel: jaringan loopback + akuntansi memcg + copy | ±29% | ACK peer ikut ditagihkan ke quota kita |
| libc malloc/calloc/free | ±10,6% | 6 pasang alokasi/free per change |
| framing + decode pgoutput | 6,2% | `pump_frames`, `read_frame`, `decode` |
| scheduler/timer tokio + mpsc | ±6,0% | 1 kirim-terima mpsc/change; 0,33 recvfrom + 0,36 epoll/change saat keep-up |
| collapse + hashing | 4,8% | 4 lookup SipHash |
| refcount `Bytes` | 2,1% | `slice(25..)` |
| render + HTTP ke CH (sisi klien) | 0,7% | apply murah; yang mahal adalah STATEMENT di CH (di bawah) |

Enam pasang alokasi/free per change (review §5, dicek):
1. body frame via `BytesMut::zeroed` (calloc+zero-fill+memcpy) `walsender.rs:158`;
2. box refcount dari `slice(25..)` `:1381`;
3. `Vec` sel `pgoutput.rs:288`;
4–5. key `Vec<Vec<u8>>` (dua alokasi) `collapse.rs:29,172`;
6. `Vec<&[u8]>` saat render key-table `dest_ch.rs:611`.

Biaya CH yang tak terlihat di profil klien: **DELETE p50 61 ms (24.8) / 418 ms pada 8
lane** dan key-table — statement termahal; `changelog=True` (insert-only) mengangkat
MySQL ke **113,8 rb/s TERUKUR**. Ini fakta kunci untuk grup 30 tabel.

## 2. Anggaran dan tuas (dari review §5.3, dipertahankan apa adanya)

| tuas | isi | hemat | keyakinan |
|---|---|---|---|
| L1a | `read_buf` `BytesMut::with_capacity` (tanpa zero-fill), `advance(25)` ganti `slice`, payload di-*move* | 0,3–0,6 µs | sedang-tinggi |
| L1b | **hapus task pump di current_thread**: drain menunggu satu refill lalu memindai semua frame sinkron dari window miliknya; kode `co_win`/`co_refill` (`walsender.rs:1106-1233`) sudah teruji; sekaligus menutup G0.2 | 0,8–1,4 µs | sedang-tinggi |
| L2 | read coalescing: refill <32–64 KB → tunggu ±1 ms (1 timer per refill); alternatif `SO_RCVLOWAT`=64 KB + timer 2–5 ms | 2–4 µs saat keep-up | sedang |
| L3 | collapse key datar tanpa alokasi: ±24 B inline, segmen ber-prefiks; fast path `u64` untuk key int tunggal | 0,6–1,2 µs | sedang-tinggi |
| L4 | index tabel padat ganti `HashMap<String, Collapser>`+SipHash; cache relid terakhir; `Instant::now()` per 256 event | 0,15–0,3 µs | tinggi |
| L5 | daur ulang container per window (map, Vec, buffer render 1+4 MiB) lewat kanal balik berbatas | 0,1–0,3 µs | sedang |
| L6 | arena per window untuk `Vec` sel; render key tanpa `Vec<&[u8]>` | 0,15–0,3 µs | sedang |

Proyeksi: tanpa L2 **11,3–13,3 µs** (35,7–42 rb/s); dengan L2 **7,3–11,3 µs**
(42–65 rb/s). 50 rb/s ada di dalam rentang — asal L2 kena juga saat catch-up;
**§5.7 dulu**: ukur recv-size catch-up sebelum menjanjikan L2.

## 3. Desain low-level per komponen

### 3.1 Read path — buang task, satu buffer milik window
- **current_thread** (sudah dipilih sampai ±2 core, G1.5). Di 0,5 core task pump
  hanyalah biaya wakeup+channel; premisnya ("syscall di core sendiri") memang
  mustahil di kuota ini.
- Buffer read **dimiliki window** dan didaur ulang via kanal balik (L5):
  `Vec<BytesMut>` berkapasitas tetap (mis. 4×256 KB); `read_buf`/`advance`
  menggantikan `zeroed` + `slice`.
- **L2 coalescing**: setelah refill menghasilkan <64 KB, tunggu readiness +
  timer 1 ms; pembatalan aman (menunggu readiness, bukan membaca). Naikkan
  `SO_RCVLOWAT` 64 KB lewat `socket2` (sudah jadi dependency sejak G0.5).
- Frame dipindai sinkron: header 5 B → `advance`; payload `Bytes` di-*move*
  (tanpa `slice_ref`, tanpa promosi refcount untuk REPLICA IDENTITY DEFAULT —
  `Cellv` sudah memakai range ke frame; jaga itu).

### 3.2 Decode pgoutput
- Pertahankan jalur **zero-copy range** yang ada (`pgoutput.rs` `CellR`/`Cellv`).
- `Vec` sel → `SmallVec<[Cell; 16]>` atau arena per window (L6); 15 kolom = nol
  alokasi.
- `cstr()` mengalokasi `String` per identifier — pindahkan ke slice + intern per
  `Relation` (registry sudah per-sesi; cukup sekali per DDL).
- Lookup `rel_oids` hanya bila ada sel biner (L4).

### 3.3 Collapse (intinya)
- `Key = Vec<Vec<u8>>` → **key datar**: satu buffer `SmallVec<[u8; 32]>` berisi
  segmen `u32 len | bytes` (atau `[u8; N]` inline + spill), hash dari slice;
  `HashMap` tetap foldhash. Key 88 B → 32 B, dua alokasi → nol (L3).
- **Fast path int tunggal**: bila PK satu kolom int2/4/8 → key = `u64` langsung;
  map `HashMap<u64, Slot>` terpisah (kasus paling umum di produksi).
- Slot: `enum Slot { Insert, Update{..}, Delete }` — pertahankan semantik
  last-write-wins yang ada; jangan ubah `DeleteSet` (dedup by construction).
- `Instant::now()` dipanggil per 256 event (L4); cache relid terakhir.

### 3.4 Window & memori (256 MB)
- Default window **32 MiB** (terukur: 27.397/s @115 MB; di host sibuk 32 MiB
  mengalahkan 64 MiB — G1.1), plus pengendali adaptif menargetkan 2–5 s.
- Anggaran 256 MB: window ≤32 MiB × (drain+overlap ≤2) + pump ≤32 MiB (G0.2) +
  tx cap 256 MiB (G0.1, refusal) + arena ≤8 MiB + baseline tokio/pg ~10 MiB —
  di bawah 256 dengan kepala; MEMPEAK sudah dilacak leg (101 MB terukur).
- Container didaur ulang antar window (L5): `HashMap::clear()` + `shrink` tidak;
  kapasitas dipertahankan, kanal balik berbatas 2 window.

### 3.5 Apply ClickHouse (tempat statement mahal)
- **Mode replika insert-only (opsional, keputusan produk)**: satu tabel
  `ReplacingMergeTree(version, is_deleted)`; DELETE dan key-table hilang —
  p50 61 ms/statement dan 418 ms @8 lane lenyap. Pembaca memakai `FINAL` atau
  view `argMax`; dokumentasikan celah visibilitas antar-DELETE/INSERT.
  Ini syarat realistis untuk **grup 30 tabel semua aktif** menembus 50 rb/s.
- Tanpa mode itu: key-table delete tetap; render key tanpa `Vec<&[u8]>` (L6),
  satu `INSERT` per window per tabel (sudah), body cap (sudah).
- Lanes: `ch_apply_lanes` sudah membaca kuota cgroup (0463694) — pertahankan
  rumus `round(16*c)` floor 8 cap 16; lane hanya berguna setelah CPU per change
  turun (kalau tidak, lane justru mengunci core).

### 3.6 Pipeline & scaling (resource naik → throughput naik)
- **Formalisasi hukum skala** (tulis di dokumen ini, diuji leg):
  - lanes_apply = clamp(round(K·cores_kuota), 1..16) (sudah);
  - window_bytes = clamp(mem_limit/8, 8..64 MiB) (G1.1);
  - slots=N membagi **semua** anggaran (drain, bootstrap, lane) — G0.8 selesai.
  - Aturan hasil: 0,5 core → target 50 rb/s; 1 core → ≥90 rb/s; 2 core → ≥150 rb/s
    (drain single-thread tetap; yang naik adalah lane apply + slots).
- **Follow mode** (G1.4): satu sesi walsender + satu tenure; menghapus
  setup/teardown 3,6–5,7 s/pass (+6–16% keep-up). Health/metric wajib (proses
  long-running).
- Overlap drain/apply sudah ada; pastikan wait_for_apply (G0.4) tidak menambah
  timer per-event (interval 10 s, nol biaya panas).

## 4. Verifikasi (RED-first, A/B, production-ready)

- Setiap tuas = satu commit: spec RED di `~/apitap-057/redlib.py` (pola yang
  sudah dipakai 20+ kali di sesi ini), suite hijau, leg gate bila ada seam.
- A/B kandang 0,5/256: **n≥3 ronde berselang-seling**, checksum 30/30, `.so` md5
  dicatat, `results.tsv`; profil `perf -g` untuk simbol libc 0x9a72e yang belum
  ter-resolve.
- Leg gate baru:
  - `e2e_three_million.py`: keep-up paced 50 rb/s 120 s, drain tak tertinggal,
    checksum exact, MEMPEAK <256 MB;
  - `e2e_scaling.py`: titik 0,5 vs 1 core dan 256 vs 512 MB — **throughput harus
    naik monoton** (regresi skala = FAIL).
- Urutan: **ukur §5.7 dulu** → L1a → L1b → L3 → L4 → L5 → L6 → A/B L2 →
  MySQL C–G (G1.2 dulu) → CH insert-only.
- Definisi selesai 0.61: catch-up **≤11 µs/change TERUKUR**, keep-up 50 rb/s
  paced checksum-exact di 0,5/256, dua leg di atas hijau, gate penuh hijau,
  rilis 0.61.0 dengan catatan angka (bukan estimasi).

## 5. Yang sengaja TIDAK dilakukan

- **mimalloc/jemalloc**: ditolak dua kali (RSS lebih buruk); L3/L5/L6 menghapus
  sebagian besar alasan; prioritas terakhir.
- **Unix socket / transport**: bukan tuas di loopback (terukur); koalesensi L2
  yang menyerang biaya per-kedatangan paket.
- **PGO retrain**: dilakukan saat rilis, bukan saat iterasi; angka A/B memakai
  build yang sama untuk kedua sisi.
- **Mengubah API Python**: tetap satu baris; semua tuas hidup di Rust.
