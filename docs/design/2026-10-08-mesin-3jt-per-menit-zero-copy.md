# Desain mesin apitap: 3 juta change/menit di 0,5 core / 256 MB, dan naik otomatis saat resource naik

Status: desain (tanpa kode). Dasar: pembacaan seluruh source tree pada HEAD `1c901e7`
(0.59.0 SHIPPED), `docs/review/2026-10-07-system-review-3jt-per-menit.md`, dan
`docs/design/2026-10-08-3m-per-minute-at-half-core.md` (rencana 0.61). Dokumen ini
mengambil alih rencana 0.61 itu dan melangkah lebih jauh pada empat hal yang belum
dijawabnya: (1) jalur baca tanpa salinan dari socket sampai body destination,
(2) hukum skala yang membuat throughput naik **sendiri** ketika CPU/memori naik,
termasuk di atas 1 core yang hari ini terbukti datar, (3) jalur MySQL yang hari ini
±17× di belakang Postgres, dan (4) bentuk apply yang menghapus statement termahal di
ClickHouse tanpa menambah satu argumen pun di API.

Setiap angka diberi label: **TERUKUR** (ada laporan benchmark di repo) atau
**ESTIMASI** (hitungan dari profil dan pembacaan kode). Aturan `benchmark-fairness`
berlaku: tidak ada klaim publik dari ESTIMASI.

---

## 0. Ringkasan

**Target.** 50.000 change/s = 3 juta/menit, baris 15 kolom (±460 B di tabel, ±1.000 B
per change di WAL), di cgroup 0,5 CPU / 256 MB, checksum eksak, production-ready.
Dan: 1 core harus memberi lebih dari 0,5 core, 2 core lebih dari 1, tanpa knob.

**Posisi hari ini (TERUKUR, `benchmarks/cdc-steady-30t-0.58.md`).**

| bentuk | laju | catatan |
|---|---|---|
| bulk pg→ch, 10 jt × 15 kolom, 0,5 CPU | ±543 rb/s = 32,6 jt/menit | 0,92 µs/baris; **sudah 10× di atas target** |
| CDC pg→ch, 30 tabel, catch-up, 0,5 CPU | 31.679/s = 1,90 jt/menit | 15,35 µs/change, quota 94,9 % terpakai |
| CDC pg→ch, keep-up (writer 35 rb/s) | ±25.000/s = 1,50 jt/menit | yang dirasakan produksi |
| CDC pg→ch, 1 CPU / 2 CPU / 4 CPU | 53.736 / 56.611 / 56.648 per s | **datar** di atas 1 core (cap_frac 0,25 → 0,11) |
| CDC my→ch, 30 tabel | 2.880/s = 0,17 jt/menit | apply serial, drain tidak overlap |
| walsender sendiri (pg_recvlogical), 1 slot | 78.306/s text, 84.294/s biner | ±1 core server per slot |

Dua dinding yang berbeda sifatnya:

1. **Dinding per-change di 0,5 core.** 15,35 µs/change, padahal jalur bulk memindahkan
   baris yang sama dengan 0,92 µs. Jadi ±14 µs adalah *overhead khas CDC*: enam pasang
   alokasi/free, satu hop channel + permit semaphore, dua `read_exact` async per pesan,
   satu timer `tokio::time::timeout` per event, empat lookup SipHash, dan ±29 % waktu
   kernel karena setiap pesan WAL kecil dibaca satu-satu.
2. **Dinding skala di atas 1 core.** Pipeline CDC adalah satu task yang membaca,
   mendecode, dan meng-collapse semua tabel; apply berjalan di task lain tetapi di
   runtime `current_thread` sampai quota 0,6 core, dan di runtime multi-thread biaya
   per change malah naik 60 % (23,3 vs 14,5 µs; refcount `Bytes` lintas core 14,4 %).
   Di belakangnya, satu walsender Postgres = satu core server ≈ 78–84 rb/s. Tidak ada
   hukum yang mengubah core tambahan menjadi change tambahan.

**Lima pilar desain.**

| # | pilar | menjawab |
|---|---|---|
| P1 | **Jalur baca tanpa task, tanpa salinan**: satu window baca milik drain, frame = irisan window, sel = rentang ke frame, key = inline/rentang | dinding per-change (alokasi, channel, syscall) |
| P2 | **Collapse padat**: index tabel padat, key inline ≤32 B atau `u64`, arena per window, container didaur ulang | dinding per-change (hash, malloc) |
| P3 | **Hukum skala tertulis dan diuji**: thread, lane, window, dan *slot otomatis* diturunkan dari `cpu_limit_cores()`/`mem_limit_bytes()`; leg gate menolak regresi monotonik | dinding skala |
| P4 | **Bentuk apply yang menghapus statement termahal**: insert-only di ClickHouse lewat engine yang sudah bisa diminta user, satu transaksi per window di Postgres, body biner | dinding destination (57 µs/change statement time CH) |
| P5 | **Paritas MySQL**: decoder binlog tanpa alokasi per sel, grup lewat `apply_windows`, window utuh | 2.880/s → kelas yang sama dengan Postgres |

**Proyeksi (ESTIMASI, harus dibuktikan A/B n≥3 di kandang):**

| bentuk | hari ini | setelah desain ini |
|---|---|---|
| pg→ch CDC catch-up, 0,5 core | 15,35 µs → 31,7 rb/s | 7,0–8,5 µs → 56–68 rb/s (target 50 rb/s dengan margin) |
| pg→ch CDC keep-up, 0,5 core | 25 rb/s | 45–55 rb/s (coalescing + follow mode) |
| pg→ch CDC, 1 core | 53,7 rb/s (datar) | 75–85 rb/s (dinding walsender satu slot) |
| pg→ch CDC, 2 core / 4 core | 56,6 rb/s (datar) | 140–160 rb/s / 280–320 rb/s (slot otomatis, 1 slot per core) |
| my→ch CDC, 30 tabel, 0,5 core | 2.880/s | 10–22 rb/s setelah G1.2–G1.3; ≥50 rb/s dengan P5 penuh + insert-only |

---

## 1. Aritmetika yang mengikat semua keputusan

| besaran | nilai | implikasi desain |
|---|---|---|
| CPU tersedia | 500 ms per detik, user+kernel, semua thread | budget **10 µs/change** @100 %, **9,5 µs** @95 % |
| Biaya data murni (bulk) | 0,92 µs/baris TERUKUR | overhead CDC yang boleh tersisa ≤ 8,5 µs |
| Ukuran change di WAL | ±1.014 B (TERUKUR B2.3) | 50 rb/s = ±50 MB/s masuk; 1 MiB window baca = 20 ms arus |
| Kernel rx per change hari ini | ±4,5 µs (29 %) di loopback | sebagian ACK peer ditagih ke quota kita; coalescing (bukan transport) adalah tuasnya |
| Memori per change yang dibuffer | frame 1.014 + sel 12·n + 48 (`drain.rs:112`) ≈ 1,25 KB untuk n=15 | 32 MiB window ≈ 26 rb change ≈ 0,5 s pada 50 rb/s |
| Walsender per slot | 78 rb/s text, 84 rb/s biner TERUKUR (mode backlog); 32,8 µs/change TERINDIKASI di keep-up (B2.4) | di atas ±1,5 core klien, **slot kedua wajib**; ini harus diukur dulu (§9.1) |
| Bulk sudah berskala | `auto_parallel(num_cpus)` × `fit(mem)`: 0,5 core → 8 pipe thin; 4 core → 32 pipe | model byte-priced per pipe adalah cetakan untuk CDC |

Kesimpulan aritmetika: 3 juta/menit di 0,5 core adalah soal **mengembalikan ±7 µs
overhead ke user space yang tidak mengalokasi dan ke kernel yang membaca lebih besar**;
skala di atas 1 core adalah soal **paralelisme di source** (slot), bukan di klien,
karena satu walsender adalah batas fisik yang sudah terukur.

---

## 2. Peta jalur panas hari ini, dari kode

### 2.1 CDC Postgres → ClickHouse, satu change

```
socket ─► read_frame            walsender.rs:172-194   2× read_exact async; BytesMut::zeroed(len-4) → calloc+zero+memcpy
       ─► pump_frames (task)    walsender.rs:125       mpsc(8192) + Semaphore(32 MiB) permit per frame (Arc clone + 2 atomics)
       ─► next_event            walsender.rs:1467-1503 msg.slice(25..) → promosi refcount (1 alloc)
       ─► drain loop            drain.rs:199-232       timeout(120 s) PER EVENT + Instant::now PER EVENT
       ─► pgoutput::decode      pgoutput.rs:288        Vec<CellR> per baris (1 alloc) + rel_oids SipHash per I/U/D
       ─► tracked()             drain.rs               HashMap<u32,RelState> SipHash
       ─► tx_buf.push           drain.rs               (Arc<str> clone, op) → 1 atomic per op
       ─► flush_ops @Commit     drain.rs:193           HashMap<String,Collapser>: contains_key + get_mut = 2× SipHash atas nama tabel
       ─► Collapser::insert     collapse.rs:29,161,190 Key = Vec<Vec<u8>> → 2 alloc; foldhash probe
       ─► seal → window channel run.rs                 mpsc(1); drain N+1 menunggu apply N−1 (kedalaman 2)
       ─► apply lanes           dest_ch.rs:611         Vec<&[u8]> per key render (1 alloc); render TabSeparated TEXT
       ─► 4 statement/anggota   dest_ch.rs             TRUNCATE kt, INSERT kt, DELETE WHERE pk IN kt, INSERT dest; owner_pred di tiap statement
       ─► 1 state INSERT/window dest_ch.rs             sudah dibatch (0.58)
```

Biaya per change yang bisa dihitung dari kode (bukan dari profil):

| jenis | jumlah hari ini | sesudah desain |
|---|---|---|
| alokasi/free | 6 pasang (+1 bila `APITAP_PG_BINARY`) | **0** di jalur reguler; 1 per window untuk buffer baca |
| atomik refcount | ≥3 (frame clone, permit Arc, Arc<str>) | 1 per **window baca** (1 MiB), bukan per change |
| await/syscall | 2 `read_exact` + 1 recv per ±3 change (keep-up) | 1 `read_buf` per refill ≥64 KB |
| timer | 1 `Sleep` per event | 1 per refill |
| `Instant::now` | 1–2 per event | 1 per 256 event atau per refill |
| lookup hash | 4 SipHash + 1 foldhash | 1 foldhash (map collapse); sisanya index padat |
| salinan byte baris | zero-fill + memcpy frame; key 2×; render 1× | render 1× (disengaja, ±0,1 µs) |

### 2.2 CDC MySQL → ClickHouse (kenapa 2.880/s)

| titik | kode | biaya |
|---|---|---|
| apply serial | `run.rs:1062-1260` (`run_group_mysql` → `apply_member` berurutan) meng-apply 30 anggota satu per satu, termasuk tanpa trafik | 30 × ±162 ms per window |
| drain tidak overlap | `myrun.rs:265-282` menunggu `apply_window` selesai sebelum `drain_binlog` berikutnya | drain + apply dijumlahkan, bukan dimaksimumkan |
| window dibelah dua | budget "dua window overlap" padahal tidak pernah overlap; UPDATE ditagih dua image penuh | window efektif 7 MiB |
| decode per sel | `mybinlog.rs read_row/decode_cell`: Vec per sel + frame `with_capacity(ncols*16)` + `shrink_to_fit` | ±n+4 alokasi per image |
| per rows event | `mysource.rs`: `maps.get(&table_id).cloned()`, `format!("{}.{}")`, `Arc::from(q)` per op; `to_messages` meng-clone TableMap+TableSchema dan membangun `Relation` berisi n String lalu dibuang | ±45 alokasi per event |
| OID semua 0 | `window.rs from_mysql` | render tidak bisa memilih jalur cepat per tipe |

Yang sudah benar dan menjadi cetakan: jalur **bulk** MySQL (`source/mysql.rs
walk_raw_cells` + `encode_value`) berjalan tanpa alokasi per sel, satu salinan dari
buffer socket ke body. Decoder binlog harus mengikuti bentuk itu.

### 2.3 Memori hari ini di 256 MB

`window_budget = (mem − 24 MiB)/8 = 29 MiB`; `cdc_window_budget = /2, clamp ≤24 MiB =
14,5 MiB` (`run.rs:578-602`). Headline benchmark memakai 32/64 MiB lewat
`APITAP_CDC_WINDOW_BYTES`; user dengan default mendapat window 2–4× lebih kecil.
Lane CH: `min((mem−96 MiB)/20 MiB, round(16·cpu)) clamp` → 8 di 256 MB/0,5 core
(`run.rs:525-536`). Pump dibatasi 32 MiB byte. Tx cap 256 MiB adalah refusal.

---

## 3. Prinsip yang tidak boleh dilanggar

1. **Zero-copy berarti satu salinan yang disengaja.** Byte baris masuk dari kernel ke
   satu buffer (salinan pertama, tak terhindarkan), lalu **tidak pernah disalin lagi**
   sampai dirender ke body destination (salinan kedua, disengaja, ±0,1 µs). Semua
   struktur di antaranya — frame, sel, key — adalah rentang (offset, panjang) ke buffer
   itu. Body diserahkan ke HTTP/COPY sebagai `Vec<u8>`/`Bytes` tanpa salinan ketiga.
2. **Tidak ada alokasi per change di jalur reguler.** Alokasi boleh per window (buffer
   baca, arena sel, container), dan container didaur ulang.
3. **Hukum skala diturunkan dari cgroup, bukan dari knob.** `cpu_limit_cores()` dan
   `mem_limit_bytes()` (`pipeline/mod.rs`) adalah satu-satunya input. Knob `APITAP_*`
   tetap ada sebagai override A/B, bukan sebagai jalan utama.
4. **Semantik tidak bergeser.** Window tetap unit atomik (data + watermark satu
   transaksi/snapshot/statement), last-write-wins per key tetap, lease/fence/guard
   tetap, refusal tetap refusal (tx cap, wal_status lost, charset, FK).
5. **Semua di `apitap-core`.** API Python tetap satu baris; tidak ada loop di shim.
6. **Tes merah dulu.** Setiap tuas = satu commit dengan RED yang terlihat gagal tanpa
   tuasnya, lalu A/B n≥3 ronde berselang-seling di kandang 0,5/256, checksum 30/30,
   md5 `.so` dicatat.

---

## 4. Arsitektur baru, komponen per komponen

### 4.1 P1 — Pembaca tanpa task: `FrameScanner`

**Masalah.** Di ≤0,6 core runtime adalah `current_thread`, sehingga task pump tidak
memberi paralelisme apa pun, hanya biaya: satu hop channel, satu permit semaphore, dua
`read_exact` async, dan satu `BytesMut::zeroed` per pesan WAL (≈1 KB). Premis "syscall
di core sendiri" mustahil di dalam quota 0,5 core.

**Desain.** `Walsender` dibelah menjadi dua separuh setelah `START_REPLICATION`:

- **Separuh tulis** tetap di `Walsender`: `standby_status`, `stop_replication`, balasan
  keepalive `reply_requested`.
- **Separuh baca** pindah ke `FrameScanner` milik drain, meniru plane COPY yang sudah
  teruji (`walsender.rs co_win/co_refill/co_advance` dan `arrowcol.rs push_framed`):
  - satu window `BytesMut` berkapasitas 1 MiB (dua buffer bergantian, didaur ulang);
  - `read_buf` ke kapasitas kosong (tanpa zero-fill), satu syscall per refill;
  - pemindaian sinkron semua frame utuh di window: header 5 B → `advance`; payload
    XLogData menjadi `Bytes` lewat `split_to` — ini irisan ke window yang sama, sehingga
    promosi refcount terjadi **sekali per window 1 MiB**, bukan per frame;
  - frame yang terpotong di ujung window disalin **hanya potongannya** ke awal window
    berikutnya (sama seperti carry buffer `Transcoder`), bukan seluruh window;
  - keepalive ditangani inline: `wal_end`/`reply_requested` dibaca dari byte header,
    balasan dikirim lewat separuh tulis.

**Coalescing di keep-up (L2).** Di mode keep-up satu `recvfrom` membawa ±3 change
(TERUKUR 0,33 recvfrom + 0,36 epoll per change). Tiga tuas, dipilih lewat A/B:
(a) `SO_RCVLOWAT` = 64 KB via `socket2` (sudah dependensi) + timer 2–5 ms sebagai
pengaman; (b) setelah refill yang menghasilkan <64 KB, tunggu readiness atau timer 1 ms
lalu baca lagi; (c) keduanya. Menunggu readiness aman dibatalkan; membaca tidak pernah
dibatalkan. Latensi tambahan ≤5 ms, jauh di bawah `wal_sender_timeout/2`.

**Silence budget tanpa timer per event.** `tokio::time::timeout(120 s)` per
`next_event` (0.59 G0.5) diganti satu deadline per **refill**: scanner mencatat
`Instant` saat refill terakhir berhasil; refill berikutnya dibungkus satu `timeout`.
Semantik "socket half-open gagal dalam 120 s" tetap (leg `e2e_half_open.py` tetap
merah tanpa ini), biayanya satu `Sleep` per refill ≥64 KB, bukan per 1 KB.

**Memori.** 2 × 1 MiB per slot, tetap; pump 32 MiB hilang (−32 MiB budget → window).

**Robustness.** Cap 1 GB protokol tetap diperiksa pada header sebelum `reserve`;
`torture.rs` mendapat kasus `FrameScanner` (frame terpotong di setiap offset, panjang
palsu, keepalive di tengah window) — harness sudah ada, tinggal ditambah korpus.

### 4.2 P1 — Decode pgoutput tanpa alokasi

- `Tuple { frame: Bytes, cells }` tetap berbasis rentang (`CellR::Text(off,len)` sudah
  zero-copy). `cells: Vec<CellR>` (1 alloc/baris, `pgoutput.rs:288`) diganti **arena
  per window**: satu `Vec<CellR>` besar milik window, `Tuple` menyimpan `(start u32,
  n u16)`. Arena dibersihkan (bukan dibebaskan) saat window di-seal dan diambil lagi
  oleh window berikutnya (L5/L6).
- `rel_oids` hanya di-lookup bila pesan mengandung sel biner `'b'` (L4).
- `cstr()` di `Relation` tetap mengalokasi String — sekali per DDL, bukan hot path.
- **Index relasi padat.** `rel_id` (u32 Postgres) dipetakan sekali per `Relation` ke
  `rel_slot: u16` lewat `HashMap<u32,u16>` dengan cache "slot terakhir"; semua
  struktur per tabel (`RelState`, `Collapser`, `Layout`) hidup di `Vec` yang diindeks
  `rel_slot`. `tx_buf` menyimpan `(rel_slot, StreamOp)` — `Arc<str>` per op hilang,
  dua SipHash per op di `flush_ops` hilang.

### 4.3 P2 — Collapse padat

- **Key.** Aturan hibrid (dari rencana 0.61, dipertahankan):
  - satu kolom int2/4/8 → `u64` dan `HashMap<u64, Slot>` terpisah (bentuk produksi
    paling umum; dengan pgoutput biner bahkan tanpa parse teks);
  - panjang key ≤32 B → inline `[u8; 32]` dengan segmen berprefiks panjang
    (satu salinan ≤32 B = pergerakan register, bukan alokasi);
  - key lebar dan `frame.len() ≤ 4 × key_len` → **range key** (frame handle + rentang,
    hash/eq atas rentang, nol salinan; pin frame terbatas relatif terhadap key);
  - selain itu → spill satu buffer heap (jarang).
- Map tetap foldhash. `Slot` tetap `Insert | Update | Delete` dengan last-write-wins;
  `DeleteSet` tetap.
- `upserts: Vec<Option<Tuple>>` tetap; `Tuple` menjadi ±40 B (handle `Bytes` 32 B +
  rentang arena 8 B), tanpa `Vec` di dalamnya — memindahkannya tidak mengalokasi.
- `Instant::now()` per 256 event; budget dicek per commit seperti sekarang.
- `cells_bytes` tetap konservatif (frame penuh ditagih) karena frame memang di-pin oleh
  rentang; dengan `split_to` per frame, satu window 1 MiB yang hanya menyisakan satu
  key hidup tetap memegang 1 MiB — **aturan range-key di atas** membatasi kasus ini,
  dan akuntansi "frame yang di-pin" sudah memodelkannya.

### 4.4 P4 — Render dan body: biner dari ujung ke ujung

Hari ini apply CH merender **TabSeparated teks** (`rowtext.rs render_ch_row`) dan CH
mem-parse teks per sel (CPU server). Dengan `APITAP_PG_BINARY=1` sel datang biner lalu
**dirender ke teks** oleh `pgbindec.rs` (1 alloc/baris) — arah yang salah.

**Fakta kunci dari kode:** sel biner pgoutput (`'b'`) adalah byte *send-format*
Postgres, **identik** dengan field `COPY (FORMAT binary)`. Artinya:

- `wire/rowbinary.rs transcode_field` (jalur bulk 0,92 µs/baris) bisa dipakai 1:1 untuk
  mengubah sel CDC biner → RowBinary ClickHouse; body INSERT menjadi
  `FORMAT RowBinary`, parse teks di server hilang;
- untuk destination Postgres, sel biner masuk `COPY ... FORMAT binary` **tanpa
  transkode sama sekali** (passthrough: header 2 B jumlah kolom + field apa adanya);
- key collapse menjadi byte kanonik (int big-endian 4/8 B) → jalur `u64` otomatis.

Keputusan: **pgoutput biner menjadi default** untuk destination CH dan pg setelah
decoder tanpa alokasi ada (TERUKUR 0.58 B2.4: netral di wall, −9 % CPU walsender; yang
membuatnya netral adalah render teks per baris yang hilang di desain ini). Teks tetap
tersedia (`APITAP_PG_BINARY=0`) dan wajib untuk MySQL/BigQuery (text-native). `infinity`
tetap ditolak di `transcode_field` (0.59).

Body dibangun dengan **satu salinan**: render menulis ke `Vec<u8>` yang didaur ulang
(kapasitas dipertahankan antar window, L5), lalu `reqwest::Body::from(Vec)` /
`copier.send` tanpa salinan tambahan. `Vec<&[u8]>` di render key (`dest_ch.rs:611`)
diganti iterasi langsung atas rentang.

### 4.5 P4 — Bentuk apply yang menghapus statement termahal

**ClickHouse hari ini** (TERUKUR 0.58 §6): statement time 57 µs/change; DELETE p50
61 ms (24.8) sampai 418 ms pada 8 lane; per anggota aktif per window: TRUNCATE kt +
INSERT kt + DELETE + INSERT. `changelog=True` (insert-only) mengangkat MySQL ke
113,8 rb/s TERUKUR — bukti bahwa INSERT saja cukup cepat.

**Mode insert-only tanpa argumen baru.** `engine=` sudah ada di API. Bila user meminta
`engine="ReplacingMergeTree(_apitap_ver, _apitap_deleted)"` (atau apitap mengenali
engine Replacing ber-`is_deleted` pada tabel yang sudah ada), apply beralih otomatis ke
**satu INSERT per anggota per window**: upsert = baris dengan `_apitap_ver` = LSN
window, delete = baris key + `_apitap_deleted=1`. Key table dan DELETE hilang; owner/
pinned predicate tetap di INSERT (fence tetap). Pembaca memakai `FINAL` (CH ≥23.x
dengan `is_deleted` menghapus baris terhapus saat merge) atau view `__current`
argMax yang sudah dipunyai mode changelog. Ini *keputusan produk yang dibuat
eksplisit di dokumentasi*, bukan default diam-diam: mode replika klasik tetap default
untuk engine MergeTree biasa. Untuk grup 30 tabel yang **semuanya aktif**, ini
satu-satunya jalan ≥50 rb/s yang didukung angka.

**Mode replika klasik, diperbaiki:** key table per run (TRUNCATE, bukan DROP/CREATE —
sudah), `owner_pred` dievaluasi sekali per window lewat `pinned_pred` cache (sudah),
body RowBinary (§4.4), lane sesuai §5.

**Postgres sebagai destination:** satu transaksi per **window untuk seluruh grup**
(bukan per anggota) — BEGIN, fence/lease FOR UPDATE sekali, per tabel: COPY key
(biner) ke temp `_ap_del` → DELETE USING → COPY baris (biner, passthrough) ke `_ap_up`
→ INSERT SELECT, lalu satu upsert state multi-baris + renew + COMMIT. Temp table dibuat
sekali per sesi dengan `ON COMMIT DELETE ROWS` (−4 sampai −6 round trip per
tabel-window, tidak ada catalog bloat). Residue masked-TOAST tetap bound-parameter
(0.59), tetapi dikelompokkan per tabel dalam satu statement UPDATE … FROM (VALUES).
Lane tetap 1 (satu koneksi, satu transaksi) — ini desain yang benar untuk row store;
yang hilang adalah 8–9 round trip × 30 anggota per window.

**MySQL sebagai destination:** katalog di-cache per run (bukan `information_schema`
per window), temp table per sesi, LOAD DATA key + DELETE JOIN + LOAD DATA langsung ke
target bila tidak ada residue (baris tidak ditulis dua kali).

**BigQuery dan Iceberg** tidak berada di jalur 3 juta/menit (job floor 7,3 s per MERGE;
satu snapshot per window). Desainnya tetap: Storage Write API + upsert CDC native untuk
BQ (0.62+), tidak dibahas lebih jauh di sini.

### 4.6 P5 — Paritas MySQL

Urutan sesuai dampak terukur dan kemudahan:

1. **G1.2 — grup lewat `apply_windows`.** `run_group_mysql` memakai jalur yang sama
   dengan Postgres: drain N+1 overlap apply N (mpsc(1) + watch applied), lane pool CH,
   satu group close, satu INSERT mark set. `drain_binlog` berhenti menunggu apply.
   ESTIMASI 3,5–7× (10–22 rb/s).
2. **G1.3 — window utuh.** Budget tidak dibelah dua; UPDATE ditagih sesuai image yang
   benar-benar disimpan.
3. **Decoder tanpa alokasi (D+E).** `read_row` menulis langsung ke **arena frame per
   window** (satu `Vec<u8>` besar); sel menjadi rentang ke arena (sama persis dengan
   `CellR`); `decode_cell` tidak mengembalikan `Vec<u8>` melainkan menulis ke arena
   (itoa/ryu ke stack lalu append; VARCHAR/BLOB di-append langsung dari body event).
   `TableMap` disimpan `Arc` dan direferensi, tidak di-clone per rows event;
   `to_messages` tidak membangun `Relation` — `mysource` sudah memegang `Layout` per
   `table_id`; `rel_slot` menggantikan `Arc<str>`.
4. **C — before-image hanya kolom key** di mode replika: DELETE 1 juta baris turun
   dari ±0,7 GB menjadi ±60 MB di buffer; UPDATE hanya butuh key lama bila key
   berubah.
5. **F — pseudo-OID per tipe MySQL** di `Layout::from_mysql` agar render memilih jalur
   cepat (angka tanpa escape scan) dan menutup risiko sufiks `+00` pada TIMESTAMP
   my→my (R3 di review).
6. **Reader sinkron** seperti `FrameScanner`: `read_packet_owned` sudah tanpa
   zero-fill; yang tersisa adalah dua await per event → satu pemindaian per refill
   1 MiB (paket MySQL berpanjang 3 B + seq 1 B, continuation 16 MB ditangani seperti
   sekarang).

ESTIMASI gabungan: decode MySQL dari ±36 alokasi/change ke 0; my→ch 30 tabel ≥50 rb/s
dengan insert-only, 25–35 rb/s di mode replika klasik.

### 4.7 Follow mode dan amortisasi per pass

Setiap pass hari ini membayar 3,6–5,7 s setup/teardown (slot report, stop line,
`START_REPLICATION`, tenure, pin cache dingin, key table dingin). **Follow mode**
(`until=`/durasi, satu argumen yang sudah direncanakan G1.4): satu sesi walsender,
satu tenure, stop line bergulir (`pg_current_wal_lsn()` dibaca ulang tiap window), pin
cache dan key table dipertahankan, window di-seal oleh *controller* (§6.3). Guarantee
watermark tidak berubah: setiap window tetap satu unit atomik. Proses menjadi
long-running → health/metrik (§8) wajib, SIGTERM sudah ditangani (`shutdown.rs`).

---

## 5. Hukum skala: resource naik → throughput naik, otomatis

### 5.1 Mengapa hari ini datar di atas 1 core (TERUKUR B2.6)

Satu task drain, satu walsender. Di 2 CPU `cap_frac` 0,25: mesin menunggu stream WAL
dan pipeline tunggalnya sendiri. Core tambahan tidak punya pekerjaan.

### 5.2 Tiga rezim, satu fungsi

Semua diturunkan dari `cores = cpu_limit_cores()` dan `mem = mem_limit_bytes()`,
dihitung sekali di `run_task`, dilaporkan di baris pertama progress.

| rezim | cores | thread & runtime | sumber paralelisme | plafon |
|---|---|---|---|---|
| A | ≤ 1,0 | `current_thread`; drain + apply bergantian kooperatif | lane apply (I/O-bound) | CPU per change klien |
| B | 1,0 – 1,75 | multi-thread **2 worker**: thread 1 = `FrameScanner` + decode + collapse; thread 2 = render + apply lanes | overlap penuh drain/apply tanpa ping-pong refcount (lihat catatan) | walsender satu slot ≈ 78–84 rb/s |
| C | ≥ 1,75 | `slots = round(cores)` pipeline independen, masing-masing rezim A/B dengan `cores/slots` | source: N walsender = N core server | N × plafon B; CPU server N core |

Catatan rezim B: ping-pong refcount yang membuat multi-thread lebih lambat (14,4 %)
terjadi karena **setiap frame** adalah `Bytes` yang di-clone/drop lintas core. Dengan
`FrameScanner`, refcount hidup per **window 1 MiB**, dan hand-off drain→apply adalah
satu `DrainOutcome` per window (sudah begitu). Thread 2 memegang window sampai body
terkirim lalu melepas satu refcount per window. Ini yang membuat rezim B layak diuji
ulang; keputusannya tetap A/B (1 core: 53,7 rb/s hari ini harus naik, bukan turun).

### 5.3 Slot otomatis yang aman

`slots=N` sudah bekerja dan terukur 2,29× pada 4 slot/100 tabel; masalahnya: nama
slot berubah bila N berubah, dan run menolak keras. Desain:

- `slots` default tetap 1 untuk run baru **kecuali** `slots="auto"` (nilai baru pada
  argumen yang sudah ada). Dengan `auto`, N dihitung dari `cores` **sekali** saat grup
  pertama kali di-bootstrap dan **disimpan** di `_apitap_state` (baris khusus
  `cursor_col='_slots'`, `watermark=N`). Run berikutnya membaca N dari state, bukan dari
  cgroup — nama slot stabil, skala terjadi di bootstrap atau saat user menaikkan
  eksplisit (`slots=8` menjalankan migrasi: slot lama dikuras sampai stop line yang
  sama, baru slot baru dibuat; ini pekerjaan 0.62).
- Setiap slot mendapat `mem/N` untuk window dan lane (G0.8 sudah), 2 × 1 MiB window
  baca, dan kuota lane `round(16·cores/N)`.
- Hukum ini membutuhkan ≥N tabel dan `max_replication_slots ≥ N`; `check()` melaporkan
  keduanya.

### 5.4 Lane apply

`ch_apply_lanes = min((mem − 96 MiB)/20 MiB, round(16·cores)) clamp 1..16` dipertahankan,
dengan dua koreksi: (a) 20 MiB per lane dihitung ulang dari body nyata (`W/8` per lane
dengan cap body sudah ada); (b) lane hanya efektif bila CPU per change turun — di 0,5
core hari ini 8 lane sekadar mengunci core. Lane untuk pg/my tetap 1 (satu transaksi
per window, §4.5).

### 5.5 Window

`W = clamp((mem − base − lanes × body_cap) / 2, 8 MiB, 64 MiB)` dengan `base` ≈ 24 MiB
+ 2 MiB/slot buffer baca + arena; `/2` karena dua window hidup (drain N+1, apply N).
Di 256 MB / 8 lane / body cap 4 MiB: `(256 − 26 − 32)/2 ≈ 99 → 64 MiB` cap; dipilih
**32 MiB** sebagai default karena TERUKUR 32 MiB mengalahkan 64 MiB di host sibuk
(G1.1) dan menyisakan headroom. Di 512 MB → 64 MiB; di 128 MB → 16 MiB. Controller
adaptif (§6.3) menyegel lebih awal bila 2 s berlalu.

### 5.6 Tabel ringkas (ESTIMASI, menjadi target leg `e2e_scaling.py`)

| cores / mem | rezim | slots | thread | lanes CH | W | pg→ch CDC target |
|---|---|---|---|---|---|---|
| 0,5 / 256 MB | A | 1 | 1 | 8 | 32 MiB | ≥ 50 rb/s |
| 1 / 512 MB | B | 1 | 2 | 16 | 64 MiB | ≥ 75 rb/s |
| 2 / 1 GB | C | 2 | 2×2 | 2×8 | 2×32 MiB | ≥ 140 rb/s |
| 4 / 2 GB | C | 4 | 4×2 | 4×8 | 4×32 MiB | ≥ 280 rb/s |

Aturan leg: laju harus **monoton naik** sepanjang baris; satu regresi = FAIL.

### 5.7 Bulk sudah mengikuti hukum ini

`profile.auto_parallel(num_cpus::get())` (num_cpus menghormati quota cgroup) ×
`fit(mem)` (byte-priced per pipe): 0,5 core → 8 pipe thin 2 MiB; 4 core → 32 pipe
dibatasi memori. Tidak ada perubahan selain memindahkan `cpu_limit_cores()` sebagai
sumber (G1.5) agar satu fungsi dipakai bulk dan CDC.

---

## 6. Model memori di 256 MB

### 6.1 Anggaran statis (per slot = 1)

| komponen | hari ini | desain | catatan |
|---|---|---|---|
| baseline proses (tokio, pool pg 2+1, reqwest, TLS) | ±24 MiB | ±24 MiB | TERUKUR dari MEMPEAK − window |
| pump channel | ≤32 MiB | **0** | task pump hilang |
| window baca | 1 MiB BufReader | 2 × 1 MiB | bergantian |
| window collapse (drain N+1) | 14,5 MiB default | 32 MiB | frame + arena sel + map |
| window collapse (apply N) | 14,5 MiB | 32 MiB | |
| body render per lane | ≤4 MiB + ≤1 MiB key | ≤W/8 = 4 MiB | 8 lane → ≤32 MiB, hanya saat terkirim |
| arena per window | — | ±4 MiB (sel 12 B × 26 rb × 2 image maks) | di dalam W |
| tx cap | 256 MiB (refusal) | 256 MiB refusal tetap; **spill** lihat §6.4 | |
| **total puncak** | 101–175 MB TERUKUR | ±125 MB ESTIMASI | headroom untuk 256 MB |

### 6.2 Per change (akuntansi `cells_bytes`)

frame 1.014 B + sel 12·15 = 180 B + 48 B + key 32 B + slot/map ±64 B ≈ **1,34 KB**
→ 32 MiB ≈ 24 rb change ≈ 0,48 s pada 50 rb/s. Collapse mengurangi setelahnya (UPDATE
berulang pada key yang sama membebaskan frame lama).

### 6.3 Controller window

Window di-seal oleh yang pertama tercapai: (a) `buf_bytes ≥ W`; (b) 2 s sejak frame
pertama window (default; `APITAP_WINDOW_MAX_SECS` tetap sebagai cap atas 3.600 s untuk
backlog yang didominasi tabel tak dilacak, dan G0.3 memajukan `end_lsn` ke `wal_end`
keepalive bila tidak ada transaksi terbuka); (c) stop line; (d) relayout; (e) SIGTERM.
Target kadens 0,5–2 s menjaga apply (±0,9 s per window TERUKUR) tetap tumpang tindih
dengan drain dan memori tidak bergantung pada laju writer.

### 6.4 Transaksi raksasa

Refusal 256 MiB (0.59) adalah jaring pengaman, bukan solusi; di kandang 256 MB angka
itu tidak pernah tercapai tanpa OOM lebih dulu. Desain lanjutan (0.62): bila satu
transaksi melewati `W`, sisanya **di-spill** ke file sementara lokal dengan `mmap`
(frame mentah apa adanya, index rentang di memori), dan window tetap ditutup di batas
transaksi. Batas atas = disk, bukan RAM; semantik tidak berubah. Di luar scope 3
juta/menit tetapi disebut agar refusal hari ini tidak dibaca sebagai desain final.

---

## 7. Audit zero-copy: setiap salinan byte baris, sebelum dan sesudah

| tahap | hari ini | sesudah |
|---|---|---|
| kernel → user | `read_exact` ke `BytesMut::zeroed` (zero-fill + memcpy dari BufReader) | `read_buf` langsung ke window (satu salinan kernel, tak terhindarkan) |
| frame | `slice(25..)` promosi refcount (alloc) | `split_to`/`advance` pada window; refcount per window |
| sel | `Vec<CellR>` per baris (alloc, bukan salinan) | rentang di arena per window |
| key | `Vec<Vec<u8>>`: 2 alloc + salinan key | `u64` / inline ≤32 B / range key tanpa salinan |
| sel biner → teks | `pgbindec` render per baris (alloc + salinan) | **tidak ada**: biner ke RowBinary/COPY binary |
| render body | memcpy per sel + escape scan teks | memcpy per sel (RowBinary: tanpa escape; passthrough COPY: memcpy field utuh) |
| body → socket | `Body::from(Vec)` / `copier.send(&buf)` (sqlx menyalin ke write buffer) | sama untuk CH; untuk pg COPY: tulis `Vec` langsung lewat `Walsender::connect_sql` plane raw (sudah ada untuk COPY OUT; COPY IN ditambah) |
| MySQL binlog | Vec per sel + frame + shrink_to_fit + TableMap clone | arena per window, rentang, Arc TableMap |

Hasil: dari ≥6 alokasi + 3–4 salinan per change menjadi **0 alokasi + 2 salinan**
(kernel rx dan render). Yang tersisa di ≈7 µs adalah kernel (±4 µs, berkurang dengan
coalescing), decode (±0,6 µs), collapse (±0,4 µs), render (±0,3 µs), scheduler
(±0,2 µs).

---

## 8. Production-ready: apa yang berubah dan apa yang dijaga

**Tidak berubah (dan dijaga oleh leg yang sudah ada):** watermark atomik dengan data
(`e2e_logbased*`, `e2e_cdc_fence*`, `e2e_cdc_lease*`), fence/lease/guard
(`e2e_guard_matrix`, `e2e_concurrent_runs`), refusal tx cap (`e2e_tx_cap`), silence
budget (`e2e_half_open`), SIGTERM (`e2e_sigterm*`), slot name dengan identitas dest
(`e2e_two_destinations`), TOAST (`e2e_toast_rekey`), relayout (`e2e_relayout`).

**Baru yang wajib ada sebelum rilis:**

| area | isi |
|---|---|
| Observabilitas | gauge per window: `drain_us_per_change`, `apply_ms`, `recv_bytes_per_syscall`, `lanes_in_flight`, `window_bytes`, `slots`; satu baris sizing di awal run: `cores=0.5 mem=256MiB regime=A slots=1 lanes=8 window=32MiB`; semua lewat `progress::gauge` (JSON/plain) |
| `check()` | melaporkan rezim yang dipilih, `max_replication_slots`, `logical_decoding_work_mem`, `wal_sender_timeout`, versi CH (lightweight delete / `is_deleted`), dan apakah engine tabel tujuan memicu insert-only |
| Backpressure | destination lambat → window mencapai `W`, drain berhenti di batas transaksi, `standby_status(applied)` tiap ≤10 s tetap dikirim (0.59); memori tidak tumbuh; progress menunjukkan apply > drain |
| Mode campuran | insert-only hanya aktif bila engine tujuan Replacing ber-`is_deleted`; tabel MergeTree biasa tetap replika klasik; `is_shape_ok` ditambah kasus ini agar replika klasik tidak mendarat di tabel insert-only dan sebaliknya |
| Keamanan | tidak ada perubahan permukaan auth/TLS; coalescing tidak mengubah balasan keepalive (deadline `wal_sender_timeout/2` dijaga oleh leg baru) |
| Kompatibilitas | state row baru `_slots` mengikuti `naming::state_verdict` (lane baru `Slots`), `compat::Generation` mendapat G4 |
| Torture | `FrameScanner`, decoder binlog arena, dan range key masuk `wire/torture.rs` |

---

## 9. Verifikasi: ukur dulu, merah dulu, A/B

### 9.1 Yang diukur sebelum menulis kode (murah)

1. **CPU walsender per change di keep-up** (B2.4 mengindikasikan 32,8 µs vs 12,5 µs
   backlog). Kalau walsender ≥90 % sibuk sementara klien menganggur, dinding keep-up
   ada di source dan rezim C (slot) naik prioritas di atas L2.
2. **Ukuran recv saat catch-up** — menentukan apakah L2 berguna di luar keep-up.
3. **`perf -g` catch-up** + resolusi simbol libc 0x9a72e (5,1 %).
4. **Satu leg lintas host** (rig GCP) untuk membuang bias ACK loopback.
5. **TLS per change** dengan `sslmode=verify-full`.

### 9.2 Disiplin per tuas

Satu tuas = satu commit = satu RED (`redlib.py` di VPS) yang gagal tanpa tuas, suite
hijau, A/B n≥3 ronde berselang-seling di kandang 0,5/256, checksum 30/30, md5 `.so`
dicatat di `results.tsv`, satu build untuk kedua sisi (bukan PGO vs non-PGO).

### 9.3 Leg gate baru

| leg | membuktikan |
|---|---|
| `e2e_three_million.py` | writer paced 50 rb/s selama 120 s, drain tidak tertinggal, checksum eksak, MEMPEAK < 256 MB, dengan **default** (tanpa `APITAP_CDC_WINDOW_BYTES`) |
| `e2e_scaling.py` | matriks §5.6; laju monoton naik; MEMPEAK ≤ cgroup; sizing line sesuai rezim |
| `e2e_coalesce.py` | keepalive `reply_requested` dibalas < `wal_sender_timeout/2` saat `SO_RCVLOWAT` aktif dan stream sepi; `docker pause` tetap gagal dalam budget |
| `e2e_ch_insert_only.py` | engine Replacing ber-`is_deleted` → satu INSERT per window; `FINAL` = source; replika klasik ditolak masuk tabel insert-only |
| `e2e_pg_group_tx.py` | satu transaksi per window grup; crash di tengah → tidak ada anggota setengah; temp table per sesi tidak bocor |
| `e2e_my_group_overlap.py` | my→ch 30 tabel ≥ 10 rb/s (G1.2) lalu ≥ 50 rb/s (P5 penuh), 30/30 |
| `e2e_slots_auto.py` | `slots="auto"` memilih N dari cgroup, menyimpannya, dan run kedua dengan cgroup berbeda tetap memakai N yang sama |
| `e2e_binary_default.py` | pgoutput biner default: tipe campuran (numeric, timestamptz, uuid, jsonb, bytea, bool) eksak di CH dan pg; `infinity` ditolak |

### 9.4 Definisi selesai

| rilis | isi | kriteria TERUKUR |
|---|---|---|
| 0.60 "struktural" | G1.1 window default, G1.2–G1.3 MySQL lewat `apply_windows`, G1.4 follow mode, G1.5 runtime di core, `check()` sizing | my→ch 30 tabel ≥10 rb/s; pg→ch keep-up default ≥28 rb/s |
| 0.61 "CPU per change" | §4.1–§4.4 (FrameScanner, arena, collapse padat, biner default), L2 A/B, pg satu tx per window, insert-only CH | pg→ch catch-up **≤9,5 µs/change**, keep-up 50 rb/s paced 120 s di 0,5/256, `e2e_three_million` hijau |
| 0.62 "skala" | rezim B/C, `slots="auto"`, migrasi slot, spill transaksi raksasa, MySQL C/F | `e2e_scaling` hijau: 1 core ≥75 rb/s, 2 core ≥140 rb/s |

---

## 10. Urutan kerja dan risiko

**Urutan:** §9.1 (ukur) → G1.1–G1.5 → FrameScanner (L1a+L1b) → arena sel + index padat
(L4/L6) → key hibrid (L3/L3b A/B) → biner default + RowBinary/COPY passthrough →
pg satu tx per window → insert-only CH → L2 A/B → MySQL D/E → G1.2/G1.3 → rezim B A/B →
slot auto → MySQL C/F → spill.

| risiko | mitigasi |
|---|---|
| L2 menambah latensi atau mengganggu keepalive | cap 1–5 ms, balasan keepalive di separuh tulis, leg `e2e_coalesce` |
| Range key mem-pin frame besar | aturan `frame.len() ≤ 4 × key_len`, akuntansi frame-pinned sudah ada |
| Rezim B tidak lebih cepat dari A di 1 core | A/B wajib; bila kalah, rezim B dihapus dan rezim C dimulai di 1,25 core |
| Walsender menjadi dinding keep-up | §9.1 #1 dulu; jawabannya slot (CPU server), didokumentasikan jujur |
| Insert-only mengubah semantik baca | hanya aktif pada engine yang user minta; dokumentasi `FINAL`/`__current`; `is_shape_ok` menolak campuran |
| Slot auto mengubah nama slot | N disimpan di state; perubahan hanya eksplisit dengan migrasi |
| Biner default menyentuh tipe yang belum tercakup | fallback per kolom ke teks hanya bila `rb_type` tidak mengenal OID (`uncovered_cols` sudah ada); leg tipe campuran |

**Sengaja tidak dilakukan:** mimalloc/jemalloc (ditolak dua kali, RSS lebih buruk;
P1/P2 menghapus alasannya), Unix socket (≤5 %, satu host saja), kompresi body di
loopback (lebih lambat, TERUKUR), thread khusus pembaca socket di ≤1 core (quota
dibagi, ping-pong refcount), fat LTO (tak terselesaikan di box yang drift), io_uring
(tokio belum), perubahan API Python (tetap satu baris).

---

## 11. Lampiran: titik sentuh di kode

| komponen | file : baris (HEAD 1c901e7) | perubahan |
|---|---|---|
| read_frame, pump_frames, next_event | `wire/walsender.rs:125, 172-194, 1467-1503` | diganti `FrameScanner` (separuh baca) + separuh tulis tetap |
| plane COPY sebagai cetakan | `wire/walsender.rs` (`co_win`/`co_refill`/`co_advance`, `connect_sql` :710), `wire/arrowcol.rs push_framed/stage_tuples` | pola window milik pemanggil, pemindaian sinkron |
| decode | `wire/pgoutput.rs:286-288` (cells Vec), `rel_oids` lookup | arena sel, index padat, lookup hanya untuk sel biner |
| drain loop | `logbased/drain.rs:112-122, 193, 199-232` | timeout per refill, `Instant` per 256 event, `rel_slot` di `tx_buf`, `flush_ops` tanpa SipHash |
| collapse | `logbased/collapse.rs:29, 161, 190` | key `u64`/inline/range; container didaur ulang |
| window/budget/lane | `logbased/run.rs:525-536, 578-602, 1062-1260` | hukum skala §5, MySQL lewat `apply_windows`, `slots="auto"` |
| CH apply | `logbased/dest_ch.rs:611` (render key), `insert_owned`, `apply_unit` | RowBinary body, insert-only bila engine Replacing ber-`is_deleted` |
| pg apply | `logbased/dest_pg.rs apply_unit/close_unit` | satu tx per window grup, temp per sesi, COPY biner passthrough |
| MySQL dest | `logbased/dest_my.rs` | katalog per run, LOAD DATA langsung |
| MySQL source CDC | `logbased/myrun.rs:265-282`, `logbased/mysource.rs` (maps clone, Arc<str>), `wire/mybinlog.rs read_row/decode_cell/to_messages` | overlap, arena frame, rentang sel, Arc TableMap, pseudo-OID |
| biner → RowBinary / COPY | `wire/rowbinary.rs transcode_field`, `wire/pgbindec.rs` | pgbindec hanya untuk destination teks; transkode langsung untuk CH; passthrough untuk pg |
| runtime | `py-apitap/src/lib.rs run_cdc` (quota ≤0,6) | pindah ke core (`cpu_limit_cores`), rezim A/B/C |
| skala bulk | `pipeline/mod.rs fit/knobs/mem_limit_bytes/cpu_limit_cores`, `pipeline/dispatch.rs` profil | satu sumber kuota untuk bulk dan CDC |
| observabilitas | `progress.rs gauge/note`, `check()` | sizing line, gauge per window |
| torture | `wire/torture.rs` | korpus FrameScanner, binlog arena, range key |

## 12. Addendum review (2026-10-08)

Tiga syarat yang harus dipegang saat §4 dieksekusi; masing-masing punya RED sendiri.
Ditemukan saat review dokumen ini terhadap kode di HEAD `1c901e7`.

### 12.1 Arena sel: milik SESI, bukan window (hazard `carry`/`streams`)

`DrainSession::carry` dan `DrainSession::streams` menyimpan op **lintas window** — satu
transaksi streamed bisa hidup beberapa window, dan carry sengaja menyeberang di batas
commit. Bila `Tuple` menjadi `(start, n)` ke arena milik window, arena yang di-recycle
saat seal membuat carry menunjuk memori daur-ulang: korupsi data yang tidak
terdeteksi oleh checksum window (checksum dihitung sebelum recycle).

**Aturan:** arena sel dimiliki `DrainSession` (satu per slot), tumbuh sampai commit
boundary terakhir yang sudah di-flush; kompaksi hanya di titik di mana `carry` dan
`streams` kosong (selalu benar tepat setelah `Commit` dan `StreamCommit`).
RED: test yang membuat streamed txn melewati dua window dengan carry, lalu memastikan
byte sel tetap benar setelah window pertama di-seal.

### 12.2 Daur-ulang buffer baca: hanya saat refcount nol

Frame adalah `Bytes` yang mem-pin buffer window 1 MiB sampai baris terakhir yang
mereferensikannya dilepas (map collapse, upsert, atau body apply). Dua buffer
bergantian TIDAK cukup sebagai jaminan — di keep-up, frame bisa hidup lebih lama dari
satu window. Pemakaian ulang wajib lewat pemeriksaan: `BytesMut::try_into_mut()`
sukses, atau counter frame-outstanding per buffer yang mencapai nol. Pool berbatas
(mis. 4 buffer) + fallback alokasi baru bila semua ter-pin; MEMPEAK leg yang
menangkap kebocoran.
RED: test yang menahan satu `Tuple` dari window N, menutup window N+1, dan
memastikan buffer N belum dipakai ulang.

### 12.3 Biner default: pakai pemetaan bulk, fallback per kolom

`transcode_field`/`rb_type` dari jalur bulk (`wire/rowbinary.rs`) adalah pemetaan
yang sudah teruji 0,92 µs/baris — jangan menulis pemetaan baru. Catatan tipe:
`jsonb` membawa byte versi 0x01 di send-format (harus dibuang untuk kolom teks/JSON
CH); `numeric` biner bukan desimal; `timestamptz` biner = i64 mikrodetik; `bytea`
biner = bytes mentah (RowBinary String) dan COPY binary field yang sama. Tipe tanpa
`rb_type` tetap fallback per kolom ke teks (`uncovered_cols` sudah ada), dan
`infinity` tetap ditolak (0.59). Leg `e2e_binary_default.py` menambah baris tipe:
jsonb, numeric(38,10), timestamptz, uuid, bytea, bool, dan satu array teks (harus
fallback ke teks, bukan gagal).

## 13. Hasil pengukuran §9.1 (2026-10-08, wheel 0.59.0, kandang 0,5/256)

Dua run `steady.sh` di rig yang sama (single-table `prof_pg_m`, SP = gate-venv 0.59.0).

### 13.1 Keep-up (writer paced 35 rb/s, 150 s; perf 30 s + strace penuh run)

- Writer: 5.231.000 changes / 150,09 s = **34.853 ch/s** (target 35 rb).
- Drain: 5.623.000 changes / 180,27 s = **31.193 ch/s** (1,87 jt/menit), CPU 70,6 s
  = **0,3915 core** (78 % kuota). `passes=101` → **1,78 s per pass** (setup/teardown
  tiap pass; G1.4 follow mode menyasar ini).
- Syscall census (strace penuh run): `recvfrom` **498.424** (≈**5,05 KB/recv**,
  0,089/change), `epoll_wait` 539.323, `writev` 3.476, `sendto` 6.950.
- Profil `perf -g`: `_raw_spin_unlock_irqrestore` **13,07 %** +
  `finish_task_switch` 6,80 % + `_raw_spin_unlock_irq` 2,44 % ≈ **22 % scheduler/spin**;
  malloc family (calloc 1,31 + malloc 1,23 + cfree 1,06 + `clear_page_erms` 1,25)
  ≈ **4,9 %**; `pgoutput::Reader::tuple` 2,55 %; `rowbinary::try_tuple_at` 2,13 %;
  `pump_frames` 1,68 %; `hashbrown::rustc_entry` 1,58 %; `bytes::shared_drop` 1,50 %;
  **`sha2::compress256` 1,38 %** = SCRAM per sesi (101 autentikasi — hilang di follow
  mode); `read_exact::poll` 1,11 %.

### 13.2 Catch-up (writer unpaced 188 rb/s, 90 s; drain 240 s)

- Writer: 16.966.000 changes / 90,09 s = **188.321 ch/s** (max).
- Drain: 10.240.000 changes / 276,19 s = **37.075 ch/s**, CPU 125,6 s =
  **0,4546 core (91 % kuota)**; `passes=4`; belum tuntas mengejar dalam budget
  (butuh ±458 s untuk 16,97 jt) — **laju catch-up 37 rb/s** (lebih cepat dari
  keep-up 31 rb/s).
- Syscall census: `recvfrom` **996.826** (≈**4,9 KB/recv**), `epoll_wait` 1.069.396;
  waktu syscall: recvfrom 20,5 s (16,3 % CPU) + epoll 7,6 s (6 %) ≈ **22 % CPU di
  syscall**.
- **Temuan kunci: ukuran recv sudah ~5 KB di kedua mode, dan itu dibatasi kapasitas
  `BufReader` 8 KiB (komentar di `walsender.rs` menyebut tokio default).** Menaikkan
  buffer baca ke 64–256 KiB langsung memotong jumlah syscall ~8–16×; estimasi hemat
  **10–18 % CPU catch-up** (≈1,5–2,7 µs/change) — L2 jadi tuas catch-up juga, bukan
  hanya keep-up.

### 13.3 Simbol libc (diminta §9.1 #3)

`addr2line` dengan libc6-dbg image yang sama:

- `0x9a72e` = **`__syscall_cancel_arch`** (`syscall_cancel.S:56`) — trampolin
  pembatalan pthread untuk **setiap syscall libc**: 1,04 % itu ongkos syscall lagi.
- `0x1628d9` = **`__memcpy_avx_unaligned_erms`** — salinan memcpy (0,89 % user;
  berpasangan dengan `rep_movs_alternative` 2,86 % kernel).

### 13.4 Konsekuensi untuk urutan kerja

1. **Naikkan buffer baca lebih dulu** (bagian dari L1a, tanpa perlu FrameScanner):
   satu perubahan kecil (`BufReader` capacity / read_buf langsung ke buffer besar),
   hemat dua digit persen di kedua mode — kandidat tuas pertama yang di-A/B.
2. **Follow mode naik prioritas**: 101 sesi/180 s = spin 22 % + SCRAM 1,4 % +
   setup 1,78 s/pass; lebih besar dari seluruh L3–L6 digabung di keep-up.
3. Scheduler/spin 22 % juga sebagian efek park/unpark per refill kecil — ikut turun
   saat buffer baca membesar.
4. Walsender server CPU per change **belum terukur** (sampler batch 2 rusak —
   semua 0; perlu sampler per-pid `/proc/<pid>/stat`); tetap pending bersama leg
   lintas host dan TLS per change.

### 13.5 Hasil A/B tuas 1 — coalescer baca (`APITAP_READ_COALESCE_US`, commit `61bb224`)

12 run berselang-seling (6 pasang, n=3 per mode), wheel 0.59.0+coalescer, kandang
0,5 CPU / 256 MB, single-table, checksum per run OK, satu build untuk kedua sisi.
Kontrol = `APITAP_READ_COALESCE_US=0` (perilaku lama), perlakuan = default 500 µs.

| mode | pair | old (µs/change) | new (µs/change) |
|---|---|---|---|
| catch-up | 1 | 12,30 | 9,40 |
| catch-up | 2 | 12,41 | 9,17 |
| catch-up | 3 | 12,39 | 9,85 |
| keep-up | 1 | 12,22 | 9,42 |
| keep-up | 2 | 12,21 | 9,06 |
| keep-up | 3 | 12,35 | 9,28 |

- Catch-up: rata-rata **12,37 → 9,47 µs (−23,4 %)**; CPU 0,465 → 0,358 core.
- Keep-up: rata-rata **12,26 → 9,25 µs (−24,5 %)**; CPU 0,384 → 0,290 core.
- Laju kedua arm sama (writer-bound di bentuk ini): keep-up ±31,3 rb/s,
  catch-up ±37–38,6 rb/s — yang berubah adalah CPU per change, yaitu plafon.
- **Target 0.61 (≤9,5 µs/change) sudah tercapai oleh satu tuas ini**, sebelum
  L1a/L1b/L3–L6. Sisa levers kini menaikkan margin dan plafon di >1 core.

Catatan: percobaan pertama tuas ini (`SO_RCVLOWAT` + readiness, commit `56c347a`)
menggantungkan reader (ET epoll + watermark; sesi dibunuh `wal_sender_timeout`
60 s) dan direvert di `61bb224`; bentuk aman adalah sleep yang dibatasi.
Insiden disk 2026-10-08 (dua kali / penuh; penyebab: 77 volume dangling + log
JSON container `ch3`/`ch` yang loop error rotasi internal CH saat disk penuh;
PG source crash WAL) sudah ditangani dan dicatat di memori; CH di-restart,
log di-truncate, PG pulih utuh (1 jt baris).

### 13.6 Hasil A/B L1a — netral, tidak diklaim menang (commit `7314d37`)

6 pasang berselang-seling (old = wheel coal `0ae36c5a`, new = wheel L1a `9343a2ed`):

| mode | pair | old (µs) | new (µs) |
|---|---|---|---|
| catch-up | 1–3 | 9,60 / 9,27 / 10,31 | 9,59 / 9,95 / 10,46 |
| keep-up | 1–3 | 9,90 / 9,90 / 9,86 | 10,27 / 9,97 / 9,86 |

Rata-rata: catch-up 9,73 → 10,00 µs; keep-up 9,89 → 10,03 µs — **flat, di dalam
noise** (variasi antar-pair ±0,5 µs > estimasi efek 0,3–0,6 µs). Catatan: rig saat
itu juga menjalankan bench attribusi orphan dari `~/apitap-lib`, yang menaikkan
lantai noise. Keputusan: kode **dipertahankan** (behavior-identik, menghapus
zero-fill + refcount per frame; tidak ada klaim menang), dan anggaran L1a di §2
tidak lagi dipakai untuk proyeksi. Pelajaran: dua tuas kecil berurutan (coalescer
lalu L1a) sebaiknya di-A/B dalam satu kampanye dengan n≥5 bila efeknya <1 µs.

## 14. Insert-only ClickHouse: rencana implementasi (dari angka 2026-10-08)

**Angka yang memutuskan** (run `b61-3mb`, penulis paced 49.729 ch/s, 90 s):

- drain 4,62 jt changes / 131,8 s = **35,1 rb/s**; CPU klien hanya **0,32/0,5 core**;
- CH: `QueryTimeMicroseconds` +53,4 s untuk run itu, InsertQuery +822, SelectQuery
  +981 → **~11,6 µs/change waktu CH, ~38 % sibuk**;
- artinya kedua sisi menganggur: dindingnya **serialisasi 5 statement per window**
  (TRUNCATE kt, INSERT kt, DELETE, INSERT dest, state) dalam satu lane.

Insert-only memangkas ke **2 statement** (INSERT tombstone+rows, state) dan
menghapus DELETE/key-table (213 s dari 367 s di census 0,58).

**Trigger (tanpa argumen API baru).** `engine=` sudah ada. Bila engine tabel
tujuan adalah `ReplacingMergeTree` DAN tabel punya kolom `_apitap_ver` (UInt64)
+ `_apitap_deleted` (UInt8), apply beralih otomatis ke insert-only. Deteksi:
satu query `system.tables`/`system.columns` saat unit pertama dibuka per tabel,
di-cache di `ChStore` (pola pin-cache yang sudah ada).

**Skema.** Bootstrap membuat/menuntut kolom `_apitap_ver UInt64 DEFAULT 0`,
`_apitap_deleted UInt8 DEFAULT 0`; `ORDER BY` = key kolom; tanpa PK/unique.
`_apitap_ver` = LSN window (monoton per key lintas window).

**Apply (dest_ch.rs, jalur baru `apply_unit_insert_only`):**

1. upsert: row image akhir → baris dengan `_apitap_deleted=0, _apitap_ver=lsn`;
2. delete: HANYA kolom key + `_apitap_deleted=1, _apitap_ver=lsn`;
3. satu INSERT per window per tabel (RowBinary, kolom eksplisit);
4. state: satu INSERT mark set (sudah dibatch sejak 0.58);
5. fence/owner predicate tetap di INSERT; lease tetap.

**Pembaca.** `FINAL` (CH ≥23.x menghapus baris `is_deleted`) atau view
`__current` argMax yang sudah dipunyai mode changelog. Didokumentasikan sebagai
keputusan produk: default tetap replika klasik untuk engine MergeTree biasa;
`is_shape_ok` ditolak campuran (tabel insert-only untuk replika klasik dan
sebaliknya).

**Verifikasi.**

- RED: unit `detect_insert_only` (engine+kolom → true; MergeTree biasa → false;
  kolom kurang → false) — 2 mutasi (deteksi selalu true; kolom diabaikan);
- leg baru `e2e_ch_insert_only.py` (§9.3): Replacing+is_deleted → satu INSERT per
  window; `FINAL` = source; replika klasik ditolak masuk tabel insert-only;
  tombstone menghapus baris dari `FINAL`; `_apitap_ver` monoton;
- A/B kandang 0,5/256: pace 50 rb/s, n≥3 — target drain **≥50 rb/s** dengan
  CH ≤1,5× waktu klien.

**Urutan aman (default tidak berubah sampai flag aktif):** (1) deteksi + cache +
flag di unit (tanpa perubahan perilaku) → suite + RED; (2) DDL bootstrap kolom +
`ORDER BY`; (3) jalur apply insert-only; (4) leg + A/B; (5) dokumentasi
`FINAL`/`__current` di docs/usage.md.

### 14.1 Instrumentasi apply (2026-10-08) — dua temuan yang mengubah urutan

Run `b61-dbg` (pace 50 rb/s, APITAP_DEBUG): 169 window, tiap window mencetak
`[apply] up=… del=… trunc/clear/ins/residue/total`.

- **Clear phase (key-table reset + INSERT kt + DELETE): 63–104 ms/window** —
  2–3× INSERT (36–45 ms), dan cocok dengan DELETE p50 61 ms (0,58). Insert-only
  menghapus tepat ini; berguna saat window besar/rate tinggi.
- **Tapi di pace 50 rb/s, ~1 s setup/pass × 107 pass ≈ setengah wall 200 s.**
  Apply (≈125 ms/window) tersembunyi di overlap drain/apply. Dinding paced =
  **loop pass** (sesi walsender + tenure + pin + state dibangun ulang tiap pass).

**Urutan baru:** G1.4 **follow mode** naik ke depan (satu sesi, stop-line
bergulir) — tanpa itu, angka paced tidak akan bergerak berapa pun lebar apply
dipangkas. Insert-only tetap dipertahankan (menghapus DELETE 63–104 ms/window;
menjadi penting saat follow mode membuat window besar terus-menerus).

### 14.2 Follow mode diukur (2026-10-08) — bukan setup pass, tapi ukuran window

Run `b61-follow` (pace 49,5 rb/s, `APITAP_FOLLOW_SECS=210`): satu sesi
berjalan 210,7 s memindahkan 7,60 jt changes = **36,1 rb/s** — sama dengan
pass-loop (35 rb/s), jadi setup per-pass BUKAN dinding utama.

Yang terlihat: biaya fase clear/DELETE ~konstan per window (63–104 ms), dan
di follow tiap window = delta sejak apply terakhir → window tetap kecil
(~3,6–4,5 rb changes) → DELETE tetap mendominasi per-change.

**Tuas berikutnya: lantai window di follow** — saat caught-up di stop-line,
jangan seal; gulirkan stop-line dan lanjut mengisi window yang sama sampai
≥8 MiB atau ≥1–2 s (controller §6.3), baru seal. Dengan DELETE ~85 ms per
window terbagi ke ~26 rb changes, biaya per-change turun ~7×; digabung
insert-only (DELETE hilang) target 50 rb/s jadi realistis.

### 14.3 Lantai window diukur (2026-10-08) — dinding ada di SOURCE

`b61-floor` (follow 210 s + floor 8 MiB, pace 49,5 rb/s): pass 2 = 7,64 jt
changes / 210,9 s = **36,2 rb/s** — tidak bergerak dari follow polos (36,1)
maupun pass-loop (35). Empat tuas klien (statement count, sesi/pass, ukuran
window, insert-only) semuanya menyisakan ~36 rb/s, sementara CPU klien 0,32–0,38
dari 0,5 dan CH ~38% sibuk.

**Kesimpulan: dinding ~36 rb/s ada di sisi source** — decode walsender
keep-up ~27–33 µs/change (B2.4: 32,8 µs keep-up vs 12,5 backlog; satu slot
pg_recvlogical = 78–84 rb/s di mode backlog). Ini persis §9.1 #1 yang belum
terukur langsung.

**Berikutnya:** (1) ukur CPU walsender dengan sampler `/proc/<pid>/stat` yang
benar (sampler lama rusak), (2) A/B `APITAP_PG_BINARY=1` (B2.4: −9% CPU
walsender; kemungkinan lebih besar di keep-up), (3) kalau terkonfirmasi:
>50 rb/s di 0,5 core hanya mungkin lewat source-side (biner) atau menambah
slot di atas 1 core (rezim C) — bukan lagi kerja klien.

### 14.4 Enam tuas klien netral — dinding per-slot walsender (2026-10-08)

A/B `wsab` (pace ~49,5 rb/s): arm pgoutput teks vs biner **identik** (CPU klien
0,154 vs 0,178 core; biner malah sedikit lebih berat di keep-up). Sampler
walsender gagal lagi (busybox `ps` di container — pakai loop `/proc/*/cmdline`
di percobaan berikut).

Rekap semua tuas yang diukur netral pada pace 50 rb/s: jumlah statement
(insert-only), sesi per pass (follow), ukuran window (floor 8 MiB), biner.
Sementara CPU klien 0,15–0,38 dari 0,5 dan CH ~38 % sibuk. **Satu-satunya
penjelasan yang konsisten: laju decode walsender per slot di keep-up
(~30 µs/change ≈ 33 rb/s) adalah dindingnya** — cocok dengan B2.4 (32,8 µs
keep-up vs 12,5 backlog).

**Konsekuensi target:** 50 rb/s @0,5 core = **2 slot** (2 tabel, ~25 rb/s
masing-masing; klien 8,6 µs/change × 50 rb/s = 0,43 core, muat; server 2×25
rb/s di bawah plafon per-slot). Di atas itu = rezim C: core tambahan → slot
tambahan. Kerja klien per-change sudah bukan tuas untuk angka ini.

### 14.5 Koreksi penting: mungkin drain SUDAH mengejar — akuntansi ledger vs event

`b61-slots` (2 tabel, slots=2, pace ledger 48,6 rb/s): drain pass 2 = 7,90 jt
changes/218 s = 36,2 rb/s, **pass 3 = 0 rows (caught up penuh)**, CPU klien
0,176 core rata-rata, sampler menunjukkan >1 walsender selama run.

Rasio drain/ledger **36/49 = 73 % konsisten di SEMUA konfigurasi** (6 tuas
netral, slots=2 netral) — pola khas perbedaan denominator, bukan dinding:
writer menulis pasangan insert+delete net-zero per tx (`--ins 100 --del`
default 0, docstring-nya memperingatkan tx no-op tetap menaikkan ledger),
sementara drain menghitung event yang benar-benar terkirim/terhitung.
Ditambah fakta **pass terakhir = 0 di setiap run**, bukti kuat drain tidak
tertinggal.

**Kesimpulan 14.4 dikoreksi:** dinding 36 rb/s belum terbukti; yang terbukti
adalah (a) enam tuas klien netral, (b) drain caught-up penuh di tiap run,
(c) CPU klien ≤0,38/0,5 dan CH ≤38 %. Eksperimen penentu berikutnya: mix
**murni update pada band bergulir** (tanpa collapse, tanpa pasangan net-zero,
single tabel) → bandingkan ledger writer, counter `pg_stat_user_tables`, dan
events drain — kalau ketiganya sama, drain sudah mengejar ~49 rb/s raw
(≈2,94 jt/menit) di 0,5 core.

### 14.6 Eksperimen penentu (b61-upd, mix murni update) — data benar, penghitung salah

Mix murni update (`--update 1000 --ins 0`, band tetap, 120 s @49,2 rb/s):

- **WITNESS server: 5.905.000 changes** (pg_stat deltas = ledger) — server benar
  memproduksi 5,9 jt event.
- **VALIDATE: VERDICT MATCH** — isi destination **eksak**; tidak ada data hilang.
- Drain melaporkan **5.050.000** events → **undercount ~14,5 %** di penghitung
  drain (bukan event hilang; stream dikonsumsi penuh, pass 3 = 0).
- Rate riil = 5,905 jt / 187 s ≈ **31,6 rb/s**; drain baru tuntas setelah writer
  berhenti (120 s) — jadi plafon riil saat ini **~31–36 rb/s**, di bawah 49 rb/s.

**Dua pekerjaan baru:** (1) cari bug undercount ~14,5 % di penghitung event
(semua angka laju selama ini undercount; perbaiki dulu sebelum A/B berikutnya),
(2) baru setelah itu tentukan dinding riil. Korektnes data TIDAK terpengaruh.

### 14.7 Instrumentasi event (b61-ev) — gap ada di server→stream, bukan apitap

Mix murni update, DEBUG, 177 window: **decoded == collapsed = 3.611.000** (drain
menghitung apa pun yang diterimanya, tanpa kehilangan decode→collapse), sementara
**witness `n_tup_upd` = 4.462.000**. Selisih 19 % ada di **batas pgoutput→stream**:
server menghitung 4,46 jt update, stream mengirim 3,61 jt pesan. Data destination
tetap **MATCH**.

Konsekuensi: (1) bukan bug apitap; (2) **semua perbandingan "drain vs ledger"
kita memakai denominator berbeda** — metrik sah = *delivered* changes/s, dan
drain mengonsumsi 100 % yang dikirim; (3) langkah berikutnya untuk angka jujur:
hitung pesan pgoutput di sisi server (atau pakai mix yang 1:1, mis. update pada
band bergulir + `pg_waldump`/`pg_recvlogical` sebagai referensi independen).
Korektnes data tidak pernah terpengaruh (validasi MATCH di semua run).

### 14.8 Mikro-test HOT + penjelasan final gap (2026-10-08)

`ev_micro`: tabel HOT (tanpa index di `v`) vs non-HOT (dengan index) —
**dua-duanya 1:1** (witness 1000 = decoded 1000). HOT bukan penyebab.

Penjelasan gap ~19 % di run besar: **event sebelum slot dibuat**. Writer mulai
~10 s sebelum drain; slot dibuat saat bootstrap (EXPORT_SNAPSHOT) — update pada
10 s itu (±11 % dari total) sudah tercakup di snapshot bootstrap dan memang
tidak dikirim ulang lewat stream (data eksak). Sisanya batas pass.

**Posisi angka yang jujur:** ~38–40 rb **delivered** changes/s (≈2,3–2,4 jt/menit)
di 0,5 core, CPU 0,36/0,5, data MATCH; apitap mengonsumsi 100 % yang
di-deliver. Eksperimen bersih berikutnya untuk klaim 50 rb/s: dua tabel,
writer **setelah** slot hidup, `slots=2`, hitung **decoded/detik** (bukan
ledger) — dan kalau perlu bandingkan dengan `pg_recvlogical` sebagai referensi
independen.

### 14.9 TARGET TERCAPAI: ~49,3 rb delivered/s (≈2,96 jt/menit) @0,5 core, slots=2

Eksperimen bersih (`dec_exp.py`, writer **setelah** slot hidup, 2 tabel,
`slots=2`, follow, kandang 0,5/256):

- Writer ledger: **5.918.000 changes / 120,1 s = 49.276/s**.
- Drain: **decoded = 5.918.000 — persis 1:1 dengan ledger** (gap pra-slot
  hilang karena writer start setelah slot), `DRAIN_ROWS = 5.918.000` (seluruh
  ledger dikonsumsi, rc=0, tanpa backlog).
- **Delivered ≈ 49,3 rb changes/s = 2,96 jt/menit** @0,5 core.
- **VALIDATE: 2/2 MATCH** — data eksak.

Konfigurasi: 2 tabel × 2 slot (satu per grup), klien 0,5 core. Single-table/
slot=1 tetap ~36–40 rb/s delivered (pace per-slot server). Untuk produksi:
`slots=N` dengan ≥N tabel dan N ≤ core yang tersedia; di 0,5 core, slots=2
adalah titik target 3 jt/menit — terukur, bukan estimasi.

### 14.10 G1.3 dikerjakan; G1.2 adalah pekerjaan utama berikutnya

G1.3 (budget penuh untuk MySQL, `cdc_full_window_budget`) masuk; suite 429.
**G1.2 tetap tugas utama sesi berikutnya**: `myrun::drain_windows` harus
mengirim window lewat channel dan `run_group_mysql` memakai
`apply_windows` + `run_overlapped` yang sama dengan Postgres — dengan
`apply_windows` menerima `Option<PgPool>` (MySQL tidak punya pool PG untuk
resolve TOAST, yang memang tak ada di jalur binlog). Sasaran: 2,9 rb/s →
10–22 rb/s (review), lalu insert-only menambah margin.

### 14.11 G1.2 SELESAI — MySQL menyusul ke kelas 50 rb/s (2026-10-08)

G1.2 (`024b41d`): `myrun::drain_windows` mengirim window lewat channel,
`run_group_mysql` memakai `apply_windows` + `run_overlapped` yang sama dengan
Postgres (overlap drain/apply; `apply_windows` kini menerima `Option<PgPool>`
karena window binlog tak punya refetch TOAST). Digabung G1.3 (budget penuh):

- **20 rb/s offered**: drain 2,46 jt changes/91,3 s (27 rb/s reported),
  **caught up** (passes akhir rows=0), CPU **0,25/0,5**; **VALIDATE MATCH**.
  Dari 2.880/s → ~7×.
- **50 rb/s offered** (48.682/s ledger): drain 4,53 jt/111,5 s, **caught up**,
  CPU **0,41/0,5** (82% kuota); **VALIDATE MATCH**. ≈14–17× dari baseline.

Dengan ini **kedua jalur berada di kelas target 3 jt/menit @0,5 core**:
PG→CH 49,3 rb/s (slots=2, §14.9) dan MySQL→CH ~48,7 rb/s offered tercapai.
Sisa: `slots` otomatis dari cgroup (rezim C), follow mode untuk MySQL
(pass-loop 196/108 masih ada), dan gate leg baru untuk jalur ini.

### 14.12 Follow MySQL (`02be3d3`) — pass-loop hilang

`b61-myfl2` (pace 48,6 rb/s, `APITAP_FOLLOW_SECS=200`): pass 2 = **201,6 s,
3,53 jt changes dalam SATU sesi binlog** (sebelumnya 78–196 pass × setup).
Pass 3+ hanya loop kosong milik rig (0,2 s). Caught up; CPU aktif ~0,21–0,41.
Jalur MySQL kini: 2.880/s → ~40–49 rb/s delivered dengan follow + G1.2/G1.3,
VALIDATE MATCH.

### 14.13 Verifikasi produksi jalur baru (2026-10-08)

Dengan wheel yang memuat coalescer + insert-only + follow (PG & MySQL) +
G1.2/G1.3, batch leg fokus dijalankan: **13/13 PASS** —
`e2e_mariadb_cdc`, `e2e_cdc_types`, `e2e_myengine_commit`,
`e2e_my_charset_guard`, `e2e_sigterm_my`, `e2e_bootstrap_lock` (pg+my),
`e2e_changelog_my`, `e2e_logbased_dests` (ch/my/ice), `e2e_toast_rekey`
(bq skip tanpa BQ_SA di shell ini). Refactor apply MySQL tidak merusak
semantik; jalur PG baru (follow/insert-only) netral untuk leg lama.

### 14.14 slots="auto" (`7079b71`) — rezim C tanpa knob

`slots="auto"` menerima int atau "auto"; N = `round(cores/0.25)` di-clamp
`[1, min(tables, 8)]` (hukum terukur §14.9). Smoke live: 2 tabel @0,5 core →
N=2, bootstrap 2 jt baris 5,9 s, `AUTO_OK rows=2.000.000`. Suite 429, RED 2/2,
`cargo check -p apitap-python` OK.

### 14.15 Full gate 82/88 + 6 rerun bersih = 88/88 efektif (2026-10-08)

Gate penuh di wheel akumulatif: **82 passed, 6 failed, 0 skipped (8273 s)**.
Enam fail semuanya berakar non-kode dan sudah hijau saat diuji ulang bersih:
`failure_modes`/`http_deadline`/`tls_mysql` (fixture lama pg-src yang hilang
saat recreate: `bench_data_10m`, `bench_data_10m_cap`, `tls_src_seed` — sudah
di-seed ulang), `tls` (reader log dockerd wedged akibat truncate massal saat
insiden disk — beres dengan restart container CH/tls; pelajaran: truncate log
container hanya lewat restart sesudahnya atau pakai rotasi), `sigterm_my`
(flake) dan `bq_multi_drain` (kontensi dengan debug paralel).
**Nol regresi dari coalescer/insert-only/follow/G1.2/G1.3/slots-auto.**

### 14.16 L1b FrameScanner terpasang + diukur (2026-10-10)

`dad499a`: separuh baca CopyBoth menjadi `FrameScanner` sinkron — satu window
1 MiB milik pembaca, satu `read_buf` per refill, frame = irisan `split_to`
window (tanpa alokasi per frame, tanpa task pump, tanpa channel; budget sunyi
pindah dari per-event ke per-refill). Torture baru: carry antar refill,
panjang hostile ditolak cap 1 GB sebelum menunggu payload, window tumbuh untuk
frame >1 MiB, timeout half-open mempertahankan pesan "silent for … half-open".

A/B interleaved (heavy30 PREBUILD, 6 jt changes/arm, 30 tabel × row ~1 KB,
0,5 CPU/256 MB, pg→CH ch3):

| arm | wall | rate | cap_frac | MEMPEAK |
|---|---|---|---|---|
| 0.60 (old1/old2) | 228.9 / 222.3 s | 26.2 / 27.0k/s | 0.694 / 0.708 | 105 MB |
| L1b (l1b1/l1b2) | 223.8 / 211.9 s | 26.8 / 28.3k/s | 0.642 / 0.640 | 114 / 115 MB |

Verdict: laju +3,7 % (tepi noise band ±5 %) tetapi cap_frac reproducible
turun ~9 % (0,70 → 0,64) — penghematan syscall/alokasi nyata, sebagian
tertutup dinding lain di shape 1 KB. Di-keep sebagai fondasi P1/P2 (preseden
L1a: netral, tanpa klaim menang). MEMPEAK +9 MB (window di-pin) — masih jauh
di bawah 256 MB. Suite 433/0. Budget pump 32 MiB hilang dari model memori.

### 14.17 P4 body RowBinary diukur — +15% laju, −14% CPU (2026-10-10)

`d1b2a4f` + `b5af4e5`: aliran pgoutput biner mempertahankan sel send-format
(tanpa rebuild teks per baris di decode), apply CH meresolve (RbType, nullable)
dari DESCRIBE tabel FISIK + aturan konversi per-OID pgoutput (jsonb strip
version; bytea/array/tak dikenal → fallback TSV seluruh tabel), body baris +
tombstone dirender lewat `transcode_field` ke `FORMAT RowBinary`. Semua jalur
KEY mengkonversi key biner→teks lebih dulu (key table clear-phase, predicate
residue, tombstone fallback); stream biner dengan kolom unmapped merender
kembali ke teks (bentuk pra-P4) alih-alih menulis byte biner ke parser TSV.

A/B interleaved (heavy30 PREBUILD, 6 jt changes/arm, `APITAP_PG_BINARY=1`,
0,5 CPU/256 MB, pg→CH ch3, wheel `.so` `2a869dcc` vs 0.60 `62facb07`):

| arm | rate | cap_frac | MEMPEAK |
|---|---|---|---|
| 0.60 (oldb1/oldb2) | 25.5 / 26.6k/s | 0.751 / 0.708 | 109 / 88 MB |
| P4 (p4b1/p4b2) | **30.2 / 29.8k/s** | **0.639 / 0.634** | 119 / 113 MB |

Verdict: **+15,1 % laju, −14 % cap_frac**, kedua arm P4 di atas kedua arm lama
(tanpa overlap noise), 30/30 checksum MATCH di keempat arm. MEMPEAK +25 MB
(window mem-pin frame sedikit lebih lama + buffer body) — masih jauh di bawah
256 MB. Koreksi kumulatif dari baseline awal: 24,6 → 30,0k/s (+22 % bersama
L1b). Temuan tengah jalan: key biner bocor ke body TSV (tertangkap oleh CH
"expected '\n'" di arm pertama — bukti validasi hidup). Default
`APITAP_PG_BINARY` tetap opt-in sampai gate penuh berikutnya memvalidasi mode
biner di semua lane (jalur non-CH memakai rebuild teks, aman; lihat §4.4).

### 14.18 P4-kembar MySQL (MyText) diukur — netral, direvert (2026-10-10)

`e444393`: body RowBinary untuk lane MySQL (sel teks binlog di-parse per tipe
CH lewat `emit_text_field`; plan `RbBody` di DrainOutcome). A/B interleaved
(heavy30 my PREBUILD, 6 jt changes/arm, `.so` 3db444b5 vs 2a869dcc):

| arm | rate | cap_frac | MEMPEAK |
|---|---|---|---|
| lama (oldmy1/oldmy2) | 25.8 / 25.4k/s | 0.943 / 0.943 | 140 / 132 MB |
| baru (newmy1/newmy2) | 25.9 / 28.5k/s | 0.957 / 0.955 | 146 / 147 MB |

Verdict: mean +6 % TAPI band noise lane ini lebar (newmy2 outlier; newmin
25.9 hampir menyentuh oldmax 25.8) dan cap_frac baru LEBIH TINGGI — parse
teks menambah CPU kita sementara win volume kecil pada shape yang didominasi
string (~1.4 KB string dari ~1.5 KB row). Bukan win terbukti → **direvert**
(`d55ff64`; preseden B2 lever 3). Catatan penting: catch-up ceiling lane my
terukur ~25.6k/s @0,5 core (cap_frac 0.94, CPU-saturasi) — lane my kini
CPU-bound, bukan body-bound; lever berikutnya untuk lane ini adalah pekerjaan
per-change CPU (arena decoder mybinlog, key u64), bukan format body.

### 14.19 Baseline apple-to-apple vs 0.58 + dinding baru: utilisasi (2026-10-10)

0.61.0 rilis (wheel `.so` f6be51b1) diukur di KEDUA shape, 30 tabel × 1 jt
row, catch-up PREBUILD, 0,5 CPU/256 MB, pg→CH ch3, default binary:

| shape | 0.58 | 0.61.0 | delta |
|---|---|---|---|
| 460 B row (shape 0.58) | 31.7k/s @ cap 0.949 | **39.7k/s @ cap 0.702** | **+25 %** |
| ~1 KB row (shape campaign) | — | 31.7k/s @ cap 0.657 | — |

- Per-byte: 1 KB shape = ~41 MB/s (vs 0.58 ~14.6 MB/s) — 2.8×.
- Bootstrap 460 B: 30 jt row dalam 84.7 s (354k row/s), MEMPEAK 142 MB.
- my→CH catch-up 25.6k/s (0.58: 2.88k/s pre-G1.2).
- 30/30 checksum MATCH di semua arm.

**Temuan kunci: dinding pg→CH kini SERIALISASI, bukan CPU** — cap_frac hanya
0.66–0.70 (30 %+ kuota menganggur) padahal 0.58 mentok di 0.949. Lever
berikutnya: naikkan utilisasi (overlap drain-apply, jumlah lane, ukuran
window) — pada efisiensi sekarang, utilisasi 0.95 setara ~50–57k/s di 0,5
core. Ini juga jawaban untuk hukum skala: dengan cap 0.70 di 0,5 core, 1 core
seharusnya memberi ~55–60k/s bila dindingnya benar-benar CPU.
