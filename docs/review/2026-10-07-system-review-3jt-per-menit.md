# Review sistem apitap 0.58.0: jalan menuju 3 juta rows/menit di 0,5 CPU / 256 MB

**Tanggal:** 2026-10-07 · **HEAD:** `0710a94` · **Jenis:** review statis seluruh proyek (tanpa build, tanpa run)
**Pembaca:** pemilik proyek, untuk memutuskan urutan kerja 0.59 → 0.61

**Cakupan.** Dua crate (`apitap-core` ±58,5 rb baris, `py-apitap`), patch `vendor/sqlx-core`, dokumen di `docs/`, dan catatan benchmark di `benchmarks/`.

**Cara kerja.**
- Tujuh reviewer berjalan paralel, masing-masing dengan lensa skill Rust yang berbeda (§15).
- Lalu saya baca ulang sendiri baris kode untuk temuan yang paling berat. Temuan yang sudah saya baca sendiri ditandai **[dicek]**. Temuan yang hanya dilaporkan reviewer (dengan kutipan file:line) ditandai **[agen]**.
- Angka ditandai **TERUKUR** (dengan sumber) atau **ESTIMASI** (dengan alasannya).

**Tafsiran target.** "3 juta per menit dengan 15 row data" saya baca sebagai **3.000.000 baris/menit = 50.000 baris/detik, dengan 15 kolom per baris**. Ini sama dengan skema benchmark standar apitap: int, varchar 20/100/500, smallint, int, bigint, double, numeric(18,4), bool, date, timestamp, timestamptz, json, text.

---

## 0. Ringkasan

1. **Sync bulk (replace/append/merge) sudah melewati target** di kandang 0,5 CPU / 256 MB:

   | Rute | Laju terukur | Kelipatan target |
   |---|---|---|
   | pg→ch | ±32 jt/menit | 11× |
   | my→ch | ±17 jt/menit | 5× |
   | pg→pg | ±9,6 jt/menit | 3× |
   | read Arrow | ±48 jt/menit | 16× |
   | pg→BigQuery | ±3,3–3,7 jt/menit | tipis |

   S3/GCS/Iceberg belum pernah diukur di kandang ini.

2. **CDC belum mencapai target:**
   - **pg→ch, 30 tabel dalam satu grup:** 1,5 jt/menit saat keep-up dan 1,9 jt/menit saat catch-up murni.
   - **my→ch, 30 tabel:** hanya 0,17 jt/menit (0.57.0). Penyebabnya struktural: grup MySQL di-apply satu tabel demi satu, dan drain tidak pernah tumpang tindih dengan apply. Decode-nya sendiri cepat: satu tabel MySQL terukur 84–135 rb change/s.
   - **CDC ke BigQuery lewat MERGE:** ±0,15 jt/menit. Ini plafon bawaan desain MERGE.

3. **Fisikanya: 50.000 change/s di 0,5 core berarti CPU ≤ 10 µs per change.** Pada 95% quota, budgetnya 9,5 µs. Sekarang 15,35 µs (TERUKUR), jadi perlu dipangkas ±38%.

4. **Jalan yang realistis ada tiga gelombang.** Semuanya ESTIMASI dan harus dibuktikan lewat A/B di kandang:
   - **Struktural.** MySQL diberi bentuk pipeline Postgres (overlap + lane pool + satu group close). Default ukuran window disamakan dengan yang terbukti di benchmark. Ditambah *follow mode* satu baris, dengan loop-nya di Rust.
   - **CPU per change di jalur Postgres.** Framing tanpa task pump di runtime current_thread, *read coalescing*, collapse key tanpa alokasi, dan container yang didaur ulang. Estimasinya 15,35 → ±9–13 µs.
   - **Bentuk apply di destination.** Mode replika insert-only di ClickHouse, satu transaksi per window di Postgres, dan Storage Write API di BigQuery.

5. **Proyeksi jujur untuk CDC pg→ch di 0,5 CPU: 2,2–3,2 jt/menit.**
   - Angka 3 juta hanya tercapai kalau *read coalescing* mendarat di separuh atas estimasi, **dan** walsender di sisi source tidak menjadi tembok (§5.7).
   - MySQL→ch justru punya peluang lebih besar melewati 3 juta setelah perbaikan struktural, karena decode-nya sudah cukup cepat.

6. **Production-ready:** review pagi ini (`2026-10-07-prod-readiness-v0.58.0.md`) menyatakan **READY**. Review ini menemukan **±12 masalah berat yang belum tercakup di sana**, beberapa berupa kehilangan data *diam-diam* (tanpa error). Sebagian besar justru makin parah ketika laju dinaikkan, jadi itu dibereskan **sebelum** mengejar throughput.

7. **Agar gampang dan aman tanpa melanggar aturan satu baris:**
   - `apitap.check(...)` untuk preflight yang mencetak SQL perbaikannya;
   - kelas exception yang bisa ditindaklanjuti, plus `apitap.reset(...)`;
   - default yang aman: TLS, memori source, dan replace yang tidak menimpa tabel orang;
   - metrik per window (bukan per baris);
   - CLI + image container;
   - wheel untuk aarch64/macOS/musl.

### Sepuluh temuan baru terpenting

| # | Temuan | Akibat | Status |
|---|---|---|---|
| 1 | Nama slot Postgres = hash(source + **nama** tabel tujuan), tanpa identitas destination. Slot tidak aktif bernama sama dianggap sisa dan **di-drop** (`run.rs:936-946`, `:1295-1315`) | Pipeline staging/cutover dari source yang sama bisa merebut slot prod. Prod lalu menolak dan harus re-bootstrap; histori `changelog=True` di celah itu hilang | [dicek] |
| 2 | Iceberg: commit yang gagal karena 5xx atau timeout (status sebenarnya *tidak diketahui*) tetap menghapus file data (`iceberg.rs:369-374`, `:1172-1180`, `:1811-1820`) | Snapshot yang ternyata sudah ter-commit merujuk file yang sudah dihapus: tabel rusak, dan watermark melompati window | [dicek] |
| 3 | MySQL: event `QUERY "COMMIT"` dari tabel non-transaksional (MyISAM/Aria/MEMORY) tidak ditangani. Cek `BEGIN` membandingkan potongan 6 byte dengan kata 5 huruf, jadi tidak pernah cocok (`mysource.rs:656`) | MariaDB: baris hilang diam-diam, karena event GTID berikutnya mengosongkan buffer (`:744-748`). MySQL: baris tertahan sampai XID berikutnya | [dicek] |
| 4 | MySQL: string dengan charset non-UTF-8 (mis. latin1) diteruskan mentah dari binlog, sedangkan bootstrap membacanya lewat koneksi utf8mb4 (`mybinlog.rs:495-499`) | Kolom String ClickHouse berisi campuran encoding tanpa error | [agen] |
| 5 | Socket replikasi Postgres tanpa TCP keepalive dan tanpa timeout baca (`walsender.rs:629`, `drain.rs:175-180`), sementara lease diperpanjang oleh task terpisah | Koneksi half-open (NAT, failover jaringan) membuat run macet selamanya sambil memegang lease. WAL menumpuk sampai slot hangus atau disk penuh | [dicek] |
| 6 | Batas memori window baru dicek saat commit (`drain.rs:233-241`). Antrean pump dibatasi **jumlah frame** (8.192), bukan byte (`walsender.rs:1328`) | Satu transaksi raksasa (mis. `INSERT…SELECT` 1 jt baris) memicu OOM-kill yang berulang tiap run. Baris lebar (≥32 KB) bisa OOM walau transaksinya kecil | [dicek] |
| 7 | apitap menyetel `logical_decoding_work_mem=1GB` di **DB produksi user**, per walsender (`walsender.rs:600-608`, `:1283-1290`). Default Postgres 64 MB; komentar di kode bilang "kept LOW" | Instance 2–4 GB dengan `slots=N` bisa kena OOM di sisi source. Yang tumbang database prod user, bukan apitap | [dicek] |
| 8 | Apply CDC ke Postgres memakai pola delete-then-insert (`dest_pg.rs:144-192`) tanpa cek foreign key | Dengan `ON DELETE CASCADE`, baris anak ikut terhapus dan tidak pernah kembali, diam-diam. Dengan `RESTRICT`, window gagal terus | [agen] |
| 9 | `read(...).lazy().filter(col != "x")` di-push ke server sebagai `<>` (`__init__.py:363-364`). Collation MySQL `*_ci` dan `char(n)` Postgres membuat `<>` mengembalikan **lebih sedikit** baris daripada polars | Baris `'Active'` / `'ACTIVE '` hilang dari hasil tanpa error | [dicek] |
| 10 | `slots=N`: hanya budget window yang dibagi N. Bootstrap N grup berjalan bersamaan dan masing-masing merencanakan memori untuk seluruh cgroup (`run.rs:1515-1524`, catatan di `:480-488`) | OOM-kill saat bootstrap. Slot yang tertinggal di-drop di run berikutnya, lalu OOM lagi. Padahal `usage.md:1151` menjanjikan budget dibagi N | [dicek sebagian] |

Ditambah dua temuan supply chain:
- `mysql_async 0.37.0` di `Cargo.lock` **sudah di-yank** di crates.io. Ini saya cek langsung lewat API crates.io; versi 0.37.1 memperbaiki data race di statement cache.
- Postgres menerima permintaan password cleartext/MD5 di semua mode TLS (`walsender.rs:775-781`) [dicek]. Akibatnya MITM bisa men-downgrade SCRAM dan mendapat password replikasi.

---

## 1. Target dan aritmetika fisik

| Besaran | Nilai | Catatan |
|---|---|---|
| Target | 50.000 baris/s | 3 jt/menit |
| CPU yang tersedia | 0,5 core = 500 ms CPU tiap detik | quota cgroup, total semua thread |
| Budget CPU per baris/change | **10 µs** @100% · 9,5 µs @95% · 8,5 µs @85% | user + kernel |
| Ukuran baris 15 kolom | ±460 B di tabel Postgres; ±1.000 B WAL per change | TERUKUR (`benchmarks/README.md`, `wal-tcp-cost-0.58.md`) |
| Arus WAL pada target | ±50 MB/s ≈ 400 Mbit/s | link 1 Gbit cukup; WAN lintas region perlu dicek |
| Biaya TLS di produksi | +0,2–0,7 µs/change | ESTIMASI. Benchmark kemungkinan berjalan tanpa TLS (loopback docker); perlu dicek di log rig |
| Isi satu window 64 MiB | ±66 rb change ≈ 1,3 s pada 50 rb/s | memori aman di 256 MB (TERUKUR 170 MB peak) |
| Sisi source (walsender) | 1 proses = 1 core server. Mode backlog: 12,5 µs/change (≈80 rb/s); indikasi mode keep-up: 30–33 µs/change (≈30 rb/s) | §5.7: ini bisa menjadi tembok sebelum klien |

**Pembanding paling penting:** jalur bulk memindahkan baris 15 kolom yang sama dengan ±0,92 µs/baris di kandang yang sama (TERUKUR: 10 jt baris dalam 18,4 s dengan quota penuh). Artinya ±14 dari 15,35 µs per change di CDC adalah *overhead khas CDC*, bukan biaya data itu sendiri. Ruang perbaikannya ada.

---

## 2. Posisi hari ini (angka terukur, 0,5 CPU / 256 MB kecuali disebut lain)

| Rute | Mode | Laju | jt/menit | Peak RSS | Sumber |
|---|---|---|---|---|---|
| pg→ch | bulk, 10×1 jt × 15 kolom | ±543 rb/s | **32,6** | 119 MB | `bench-capped-pg-ch-0.57.md` |
| my→ch | bulk, 10×1 jt × 15 kolom | ±277 rb/s | **16,6** | 99 MB | `bench-capped-my-ch-0.57.md` |
| pg→pg | bulk 10 jt | ±160 rb/s | **9,6** | 81 MB | `logbased-cdc.md` |
| my→pg | bulk 10 jt (build Juli, lama) | ±172 rb/s | 10,3 | — | `benchmarks/README.md` (tiny box) |
| pg→BigQuery | bulk 1 jt | 55–62 rb/s | 3,3–3,7 | — | `benchmarks/README.md` |
| read (Arrow streaming) | 10 jt × 15 kolom | ±806 rb/s | 48 | 190 MB | `read-showdown.md` |
| S3/GCS Parquet, Iceberg | bulk | **belum diukur di kandang ini** | ESTIMASI 6,6–9,6 | — | §8 |
| pg→ch CDC, 30 tabel | catch-up murni | 31.679/s | **1,90** | 163 MB | `cdc-steady-30t-0.58.md` B2.3 |
| pg→ch CDC, 30 tabel | keep-up dengan writer 34 rb/s | 24.995/s | **1,50** | 173 MB | idem B2.5 (berlaku untuk CH 24.8 maupun 25.8) |
| pg CDC 1 tabel, 15 kolom lebar | stress | ±34 rb/s | 2,0 | — | `py-apitap/README.md:84` |
| my→ch CDC, 30 tabel (0.57.0) | keep-up | 2.880/s | **0,17** | 55 MB | `bench-capped-my-ch-cdc-0.57.md` |
| my CDC 1 tabel | stress | 84–135 rb/s | 5–8 | — | `py-apitap/README.md:84`, `ch-ingest-r3.md` |
| pg→BigQuery CDC | MERGE, 1 tabel | ±2,5 rb/s | 0,15 | — | `bq-cdc-optimize.md` [agen] |
| CDC → pg / → my / → Iceberg, 30 tabel | — | **belum diukur** | ? | — | — |

**Catatan konfigurasi.** Angka CDC headline diukur dengan `APITAP_CDC_WINDOW_BYTES` 32/64 MiB. Default di 256 MB hanya **14,5 MiB** (`run.rs:578-602`) [dicek], jadi user biasa mendapat window ±4× lebih banyak dan laju yang lebih rendah dari headline.

---

## 3. Kesimpulan review per area

| Area | Vonis | Inti |
|---|---|---|
| Throughput bulk | **LEWAT** | 3–16× target. Yang tersisa adalah ketahanan di tabel sangat besar (§8: batas 5 GiB S3, metadata footer Parquet) |
| Throughput CDC Postgres | **KURANG (50–63%)** | CPU per change di klien, ditambah kemungkinan tembok walsender di mode keep-up |
| Throughput CDC MySQL | **JAUH (6%)** | Apply grup serial dan tanpa overlap. Decode bukan masalah |
| Throughput CDC BigQuery | **JAUH (5%), struktural** | Biaya tetap MERGE per window; perlu Storage Write API |
| Kebenaran CDC Postgres | **INTI BENAR, ADA LUBANG** | Handoff snapshot↔slot dan confirm-after-commit benar. Lubangnya: tabrakan slot, FK cascade, `infinity` yang wrap, guard 0 baris, span CTID tanpa snapshot bersama |
| Kebenaran CDC MySQL | **RISIKO TINGGI** | Tabel non-transaksional, charset, TIMESTAMP→MySQL, TIME negatif berfraksi, DDL berawalan komentar |
| Iceberg | **RISIKO TINGGI** | Commit ambigu menghapus file yang mungkin sudah dirujuk snapshot |
| Liveness saat beban berlebih | **RISIKO TINGGI** | Socket half-open, transaksi raksasa, tidak ada keepalive selama apply panjang, slot hangus tanpa panduan |
| Keamanan | **SEDANG** | Downgrade auth, SQL splice di `dest_pg`, backslash di identifier CH, default TLS yang tidak seragam |
| Operabilitas | **SEDANG** | Progress CDC Postgres selalu 0; tidak ada metrik; Ctrl-C diabaikan; kill saat bulk meninggalkan tabel terkunci |
| Kemudahan pakai | **SEDANG** | Tidak ada preflight; 26 env knob (yang terdokumentasi di `stability.md` hanya 6); wheel hanya x86_64 |
| Supply chain | **SEDANG** | `mysql_async` sudah di-yank, rustls kena advisory, MSRV salah, dua versi parquet |
| Kualitas kode Rust | **BAIK** | `unsafe` minim dan sound; panic di bulk ditangkap; tidak ada `Debug` pada struct berisi kredensial; refusal selalu menyebut perbaikannya |

---

## 4. Ke mana 15,35 µs per change habis (CDC Postgres)

**Batasan tabel ini.**
- Persentasenya TERUKUR dari satu-satunya profil 0.58: `cdc-steady-profile/raw/steady-pg.perf.symbols.txt`. Profil itu dari satu tabel, writer berpacing, `cpu-clock`, tanpa call graph. Bucket-nya dikelompokkan ulang oleh reviewer hot path.
- Nilai µs-nya ESTIMASI: persentase × 15,35. Total 15,35 itu sendiri berasal dari run catch-up 30 tabel.
- Keep-up terukur 14,5 µs/change, jadi kedua regime biayanya mirip.

| Tahap | Porsi | ±µs | Bukti utama |
|---|---|---|---|
| Kernel jaringan: rx, kirim ACK, proses ACK di socket peer, nftables/conntrack, lock socket | ±29% | 4,5 | `net_rx` 7,0%, jalur ACK 4,8%, `nft_*` 6,3% + ±3% |
| Kernel scheduler, epoll, timer, clock | ±9,7% | 1,5 | `finish_task_switch` 3,9% (simbol kernel teratas) |
| Kernel: masuk syscall, seccomp, AppArmor | ±6,4% | 1,0 | `do_syscall_64`, `__seccomp_filter` |
| Kernel: akuntansi memori socket + salin data | ±4,9% | 0,75 | `refill_stock`, `mod_memcg_state`. Page fault nyaris nol (`clear_page_erms` 0,07%) |
| libc malloc/calloc/free | ±10,6% | 1,6 | `__libc_calloc`, `malloc`, `cfree` + internal |
| libc pada satu alamat yang tak ter-resolve (0x9a72e) | ±5,1% | 0,8 | Tidak muncul di profil MySQL. Perlu `perf -g` |
| Framing + decode pgoutput | 6,2% | 0,95 | `pump_frames`, `read_frame`, `decode` |
| Scheduler/timer tokio + channel mpsc | ±6,0% | 0,9 | |
| Collapse + hashing | 4,8% | 0,73 | |
| Refcount `Bytes` | 2,1% | 0,33 | `shared_drop`, `promotable_even_clone` |
| Render body + HTTP ke ClickHouse | 0,7% | 0,1 | sisi apply murah di klien |

**Biaya per change, dari pembacaan kode** [agen; titik utamanya dicek]:
- **Enam pasang alokasi/free:**
  1. body frame lewat `BytesMut::zeroed` (calloc + zero-fill + memcpy, `walsender.rs:158`) [dicek];
  2. box refcount yang dibuat oleh `slice(25..)` (`:1381`);
  3. `Vec` sel (`pgoutput.rs:288`);
  4. dan 5. key `Vec<Vec<u8>>`, yaitu dua alokasi (`collapse.rs:29,172`);
  6. `Vec<&[u8]>` saat render key-table (`dest_ch.rs:611`).
- **Tambahan lainnya:**
  - 4 lookup SipHash;
  - 1 kali kirim-terima mpsc;
  - 1 `Instant::now()`;
  - 2 `read_exact` async per pesan (`walsender.rs:136-161`) [dicek].
- **Di mode keep-up:** satu siklus park → recv → ACK untuk setiap ±3 change (TERUKUR 0,33 recvfrom dan 0,36 epoll_wait per change).

**Dua koreksi atas catatan internal** (lengkapnya di §12):
- "Transport <10%" hanya menjumlahkan 7 simbol kernel. Kalau semua simbol dikelompokkan, jaringan ±29%. Di **loopback**, pemrosesan ACK milik peer ikut ditagihkan ke quota apitap.
- "Allocator ~18%" sebenarnya seluruh libc; keluarga malloc sendiri ±10,6%.

Implikasinya ada dua:
- Angka rig kemungkinan **pesimistis** untuk deployment lintas host. Perlu satu leg lintas host sebelum mengoptimasi jalur kernel.
- *Read coalescing* adalah tuas yang nyata, walaupun Unix socket memang bukan tuas.

---

## 5. Ide dan rencana menuju 3 juta/menit

### 5.0 Prinsip yang dipakai

- **Ukur dulu, baru ubah.** Setiap tuas di bawah adalah hipotesis sampai ada A/B di kandang 0,5/256 MB: n≥3 ronde berselang-seling, checksum 30/30, `.so` md5 dicatat, dan gate 80/80.
- **Tes harus terlihat MERAH dulu tanpa perbaikannya** (aturan *control-the-test-first*).
- **API tetap satu baris.** Semua loop dan mode baru hidup di Rust.
- **Gelombang 0 duluan.** Laju yang lebih tinggi memperbesar akibat setiap lubang liveness dan memori.

### 5.1 Gelombang 0: prasyarat yang menentukan throughput (dikerjakan dulu)

| # | Perubahan | Kenapa dulu | Biaya memori |
|---|---|---|---|
| G0.1 | **Batas byte untuk satu transaksi**, dengan refusal yang menyebut transaksi, tabel, ukuran, dan perbaikannya. Nanti bisa ditambah spill ke file | Mengganti OOM-kill berulang dengan error yang bisa ditindaklanjuti | 0 |
| G0.2 | **Antrean pump dibatasi byte dan ditagihkan ke window** (atau dihapus, lihat L1b) | Baris lebar ≥32 KB saat ini bisa OOM | turun |
| G0.3 | **Deadline window 10–30 s**, plus `end_lsn` maju ke `wal_end` keepalive saat tidak ada transaksi terbuka | Window tak lagi bisa membeku sampai 3.600 s di backlog yang didominasi tabel tak dilacak | 0 |
| G0.4 | **Kirim `standby_status(applied)` tiap ≤10 s selama menunggu apply** (`run.rs:1636-1647`) | Apply >60 s membuat walsender dibunuh `wal_sender_timeout`, di setiap run | 0 |
| G0.5 | **TCP keepalive + timeout pada `pump.frames.recv()`** (aman dibatalkan, karena yang dibatalkan penerimaan dari channel, bukan pembacaan socket), seperti yang sudah dipakai jalur MySQL (`mysource.rs:505-531`) | Menutup kasus macet selamanya | 0 |
| G0.6 | **Cek `wal_status='lost'` sebelum `START_REPLICATION`**, error terpandu, dan opsi `on_slot_lost="rebootstrap"` (satu argumen) | Saat ini muncul `55000` mentah, dan panduan "slot is GONE" tidak pernah keluar | 0 |
| G0.7 | **`logical_decoding_work_mem` mengikuti nilai server** (atau ≤256 MB) dan dilaporkan oleh `check()` | Melindungi DB produksi user, yang dampaknya dikalikan `slots=N` | 0 di klien |
| G0.8 | **`slots=N`: pembagi memori juga diterapkan ke bootstrap dan lane** (atau bootstrap grup dijalankan bergiliran) | Janji di dokumentasi menjadi benar | turun |
| G0.9 | **Progress CDC Postgres diisi** (`apply_windows` tidak pernah memanggil `progress::add_rows`; hanya MySQL di `run.rs:1246`) [dicek] | Tanpa ini operator tak bisa membedakan macet dari lambat | 0 |

### 5.2 Gelombang 1: struktural, murah, efek besar

| # | Perubahan | Efek (ESTIMASI) | Memori | Risiko |
|---|---|---|---|---|
| G1.1 | **Default window CDC = ukuran yang terbukti** (32 MiB di 256 MB), plus pengendali adaptif yang menargetkan window 2–5 s di dalam batas memori | +10–24% untuk user dengan default. TERUKUR: 32 MiB → 27.397/s @115 MB; di host yang sibuk 32 MiB mengalahkan 64 MiB | +±36 MB | rendah |
| G1.2 | **Grup MySQL lewat `apply_windows`**: overlap drain/apply, lane pool, satu group close, satu INSERT mark set. Saat ini `run.rs:1240-1247` meng-apply 30 anggota satu per satu (termasuk anggota tanpa trafik) dan `myrun.rs:265-282` menunggu apply sebelum drain berikutnya [dicek] | 3,5× di CH 24.8 (±10 rb/s); 5–7× di CH 25.8 (15–22 rb/s) | +1 window | sedang |
| G1.3 | **Window MySQL tidak lagi dibelah dua tanpa alasan**: saat ini memakai budget "dua window overlap" padahal tidak pernah overlap, dan UPDATE ditagih dua image penuh | 1,8–2× sebelum G1.2 | +35 MB | rendah |
| G1.4 | **Follow mode di engine**: satu argumen, mis. `follow=True` atau `until=...`. Satu sesi walsender, satu tenure, stop-line yang bergulir; cache pin dan key table dipertahankan | Menghapus setup/teardown 3,6–5,7 s per pass: +6–16% keep-up. Guarantee watermark tidak berubah | 0 | sedang (proses jadi long-running; butuh health/metrik §10) |
| G1.5 | **Pilihan runtime pindah ke core**: current_thread dipakai sampai ±2 core, membaca `cpu_limit_cores` yang sudah menelusuri cgroup | 0 di 0,5 CPU. Mencegah +60% CPU/change yang terukur di 1 CPU (23,3 vs 14,5 µs) | 0 | rendah |

### 5.3 Gelombang 2: CPU per change di jalur Postgres

| # | Tuas | Hemat (ESTIMASI) | Risiko | Keyakinan |
|---|---|---|---|---|
| L1a | `read_buf` ke `BytesMut::with_capacity` (tanpa zero-fill), `advance(25)` sebagai ganti `slice`, payload di-*move* ke Tuple. Tanpa promosi refcount untuk tabel REPLICA IDENTITY DEFAULT | 0,3–0,6 µs | rendah | sedang-tinggi |
| L1b | **Tanpa task pump di current_thread.** Drain menunggu satu refill lalu memindai semua frame utuh secara sinkron dari window miliknya sendiri. Kode `co_win`/`co_refill` di plane COPY (`walsender.rs:1106-1233`) sudah teruji; premis pump ("syscall di core sendiri") memang mustahil di 0,5 core | 0,8–1,4 µs (sekaligus menutup G0.2) | sedang | sedang-tinggi |
| L2 | **Read coalescing.** Setelah refill yang menghasilkan <32–64 KB, tunggu ±1 ms (satu timer per refill, bukan per pesan). Alternatifnya `SO_RCVLOWAT`=64 KB + timer 2–5 ms. Menunggu readiness aman dibatalkan, membaca tidak | **2–4 µs saat keep-up.** Saat catch-up: belum diketahui (bisa ±0 kalau bacaan di sana sudah besar) | rendah-sedang (+≤1 ms latensi) | sedang (keep-up) |
| L3 | **Collapse key datar tanpa alokasi**: ±24 B inline dengan segmen ber-prefiks panjang, plus jalur cepat `u64` untuk key int2/4/8 tunggal | 0,6–1,2 µs; key 88 → 32 B | rendah-sedang | sedang-tinggi |
| L4 | Index tabel padat sebagai ganti `HashMap<String, Collapser>` + SipHash; cache relid terakhir; lookup `rel_oids` hanya bila ada sel biner; `Instant::now()` tiap 256 event | 0,15–0,3 µs | sangat rendah | tinggi |
| L5 | Daur ulang container per window (map, Vec, buffer render 1+4 MiB) lewat kanal balik dengan batas kapasitas | 0,1–0,3 µs | rendah | sedang |
| L6 | Arena per window untuk `Vec` sel; render key tanpa `Vec<&[u8]>` | 0,15–0,3 µs | rendah | sedang |

**Jalur MySQL** (setelah G1.2):
- C: before-image hanya mendecode kolom key di mode replika, dan hanya byte key yang ditagih. Hemat 1,5–2,5 µs; DELETE 1 jt baris turun dari ±0,7 GB menjadi ±60 MB di buffer.
- D: decode langsung ke frame, ±18 alokasi lebih sedikit per image (0,6–1,2 µs).
- E: lapisan terjemahan pgoutput dibuang. Saat ini dibangun `Relation` berisi 15 String per event lalu langsung dibuang. Hemat 0,1–1,5 µs.
- F: column plan per TABLE_MAP dengan pseudo-OID sungguhan. Saat ini semua OID bernilai 0 (`window.rs:97-101`) [dicek]. Hemat 0,2–0,4 µs, sekaligus menutup risiko TIMESTAMP di §7.
- G: buffer body CDC dipakai ulang (0,2–0,4 µs).

### 5.4 Gelombang 3: bentuk apply di destination

| Destination | Perubahan | Efek (ESTIMASI) | Catatan |
|---|---|---|---|
| ClickHouse | **Mode replika insert-only (opsional)**: `ReplacingMergeTree(version, is_deleted)`. Key table dan DELETE hilang, padahal keduanya statement termahal (DELETE p50 61 ms di 24.8 dan 418 ms pada 8 lane, TERUKUR) | Satu-satunya cara meyakinkan agar grup 30 tabel yang semuanya aktif mencapai 50 rb/s | Keputusan produk: pembaca butuh `FINAL` atau view argMax. `changelog=True` yang sudah ada mengangkat MySQL ke 113,8 rb/s (TERUKUR) |
| ClickHouse | Dokumentasikan celah visibilitas: di antara DELETE dan INSERT sebuah window, pembaca sesaat melihat key hilang | — | jujur ke user |
| Postgres | **Satu transaksi per window untuk seluruh grup** (saat ini anggota di-apply serial, masing-masing membayar BEGIN, fence, state, dan COMMIT) | Hemat 30–150 ms per window; replay grup parsial hilang | lock baris dipegang lebih lama |
| Postgres | Temp table dibuat sekali per sesi (`ON COMMIT DELETE ROWS`), bukan 2× `CREATE TEMP` per tabel per window | −4 sampai −6 round trip per tabel-window; mencegah catalog bloat | rendah |
| Postgres | **UPDATE masked-TOAST berbasis set** (saat ini satu statement literal per event) | ≥10× untuk "update status di baris berisi jsonb besar" | sedang |
| MySQL | Cache katalog per run; LOAD DATA langsung ke target setelah delete-join (baris tidak ditulis dua kali) | −3/−4 round trip per window | rendah |
| BigQuery | **Storage Write API + upsert CDC native** (`_CHANGE_TYPE`, `_CHANGE_SEQUENCE_NUMBER`, `max_staleness`). MERGE hilang | Satu-satunya jalan realistis ke ≥10 rb/s | butuh dependensi gRPC; biaya ±$0,025/GiB (verifikasi) |
| BigQuery | Filter rentang key konstan di klausa `ON` MERGE, sebagai solusi sementara | bytes billed & latensi turun | ukur dulu |

### 5.5 Gelombang 4: ketahanan bulk di tabel sangat besar

- **Parquet (S3/GCS/Iceberg):**
  - rotasi file per ±512 MiB per pipe, karena CopyObject S3 dibatasi 5 GiB dan metadata footer tumbuh ±1–2 MB per juta baris tanpa dihitung planner;
  - body upload sebagai `bytes::Bytes` (hemat 8 MiB per pipe di S3 dan 16 MiB di GCS/BQ, cukup untuk satu pipe ekstra);
  - satu buffer teks kontigu per kolom;
  - `REQUIRED` untuk kolom NOT NULL;
  - A/B dictionary;
  - statistik di level chunk.
- **Replace di Postgres:**
  - index dibangun di staging sebelum swap, grant di dalam transaksi swap, lalu `ANALYZE`;
  - `lock_timeout` + retry pada `DROP`. Saat ini DROP menunggu lock eksklusif tanpa batas dan semua pembaca mengantre di belakangnya.
- **MySQL sebagai destination bulk:**
  - LOAD DATA dirotasi tiap 64–256 MB per pipe;
  - span diurutkan per PK agar InnoDB menerima data sesuai urutan.
- **Snapshot bersama untuk span CTID**: satu `pg_export_snapshot()` per run. Saat ini UPDATE yang memindahkan baris antar halaman bisa menghilangkan atau menggandakan baris.

### 5.6 Proyeksi

**CDC pg→ch, catch-up, 30 tabel, 0,5 CPU:**

| Tahap | µs/change | change/s @95% quota | jt/menit |
|---|---|---|---|
| Hari ini (TERUKUR) | 15,35 | 31.679 | 1,90 |
| + Gelombang 2 tanpa L2 (ESTIMASI) | 11,3–13,3 | 35,7 rb–42,0 rb | 2,1–2,5 |
| + L2 kalau juga kena di catch-up (ESTIMASI) | 7,3–11,3 | 42 rb–65 rb | 2,5–3,9 |
| **Target** | **9,5** | **50.000** | **3,0** |

**Mode keep-up** (yang dialami produksi, TERUKUR 25 rb/s): G1.1 + G1.4 (+16–40%) membawanya ke ±29–35 rb/s sebelum kerja per-change. Sesudah itu plafonnya ditentukan pertanyaan §5.7.

**MySQL→ch, 30 tabel:**
- 2.880/s hari ini;
- G1.2 → 10–22 rb/s;
- G1.3 + C–G → plafon decode satu tabel menunjukkan >50 rb/s secara fisik mungkin;
- dengan mode insert-only atau `changelog`, 3 jt/menit adalah target yang **masuk akal** (ESTIMASI).

**Janji yang jujur ke user sampai ada bukti A/B:**
- *"Bulk ≥3 jt baris/menit di 0,5 CPU/256 MB (terukur 3–16×)."*
- *"CDC Postgres ±1,5 jt/menit keep-up per pipeline 0,5 CPU; target 3 jt dalam pengerjaan."*

### 5.7 Pertanyaan terbuka yang harus diukur dulu (murah, tanpa kode)

1. **Apakah walsender di source menjadi tembok di mode keep-up?** B2.4 mencatat CPU walsender **70,30 s untuk 2.144.000 change = 32,8 µs/change**, sedangkan probe backlog `pg_recvlogical` hanya **12,5 µs/change** (2,6× lebih murah). Satu walsender = satu core server, sehingga di pola transaksi kecil-kecil plafonnya ±30 rb/s per slot, berapa pun optimasi klien.
   - Kesimpulan B2.3 "walsender punya headroom 2,5×" diukur di mode backlog, dan **belum tentu berlaku untuk keep-up**.
   - Cara mengukur: CPU walsender per change × ukuran transaksi × kecepatan klien. Kalau walsender ≥90% sibuk sementara klien menganggur, tembok ada di source. Solusinya lalu di sisi source (slot kedua, yang berarti CPU server tambahan), bukan di klien.
2. **Loopback vs lintas host.** Di loopback, proses ACK milik peer ikut ditagihkan ke quota apitap. Satu leg di rig GCP 3-mesin (`gcp-benchmark.md`) akan menunjukkan berapa µs/change sebenarnya di produksi.
3. **Profil catch-up dengan `perf -g`, `strace -c`, dan `nstat`**, termasuk memecahkan alamat libc 0x9a72e (5,1%).
4. **Ukuran recv saat catch-up.** Ini yang menentukan apakah L2 berguna di luar mode keep-up.
5. **Biaya TLS per change** dengan `sslmode=verify-full`.
6. **Ulangi stress beban berlebih dengan benar**: drain dijalankan duluan, dan stderr writer tidak lewat `tee` (§12). Perilaku di 500 rb change/s sampai sekarang **belum pernah terukur**.

### 5.8 Yang tidak disarankan (dan alasannya)

| Ide | Alasan |
|---|---|
| Thread khusus pembaca socket | Quota cgroup dipakai bersama. Refcount `Bytes` lintas thread terukur 14,4%; multi-thread di 1 CPU terukur 23,3 vs 14,5 µs/change |
| `slots=2` dalam satu proses 0,5 CPU | Tiap slot mendecode seluruh WAL (CPU source ×2), window terbelah dua, dan nama slot berubah sehingga re-bootstrap. Baru layak dipertimbangkan kalau §5.7 #1 membuktikan walsender yang jadi tembok |
| Unix socket | Hanya ±5% dan hanya untuk deployment satu host |
| Kompresi body di loopback | Terukur lebih lambat, karena menghabiskan CPU klien yang paling langka |
| mimalloc | Sudah ditolak dua kali (RSS lebih buruk). Fakta baru satu-satunya adalah mimalloc v3, dan tuas L3/L5/L6 menghapus sebagian besar alasannya. Prioritas terakhir |
| io_uring untuk socket | Belum didukung tokio (io_uring baru untuk file, dan masih `tokio_unstable`) |
| Fat LTO | Hasilnya tidak bisa disimpulkan di box yang drift. Hanya layak diuji ulang di box yang tenang |

---

## 6. Sync dari Postgres (khusus)

### 6.1 Yang sudah benar (pertahankan)

- **Handoff snapshot↔slot:**
  - slot dibuat dengan `EXPORT_SNAPSHOT`, dan walsender tetap terbuka selama load (`run.rs:1317-1388`);
  - setiap span memakai REPEATABLE READ + `SET TRANSACTION SNAPSHOT`, di kedua jalur COPY;
  - watermark = consistent point slot.

  Probe min/max di luar snapshot tetap aman, karena hasilnya superset [agen].
- **Confirm-after-commit:**
  - slot hanya dikonfirmasi setelah commit di destination;
  - balasan keepalive melaporkan LSN yang sudah di-*apply*, bukan yang baru di-drain;
  - cek "watermark tertinggal dari confirmed LSN" menangkap commit yang hilang.
- **Fitur protokol:**
  - streaming protokol v2 dengan rollback savepoint yang presisi;
  - TOAST rekey dilakukan sebagai pemindahan baris;
  - TRUNCATE diterapkan berurutan;
  - `publish_via_partition_root`;
  - refusal untuk REPLICA IDENTITY NOTHING.

### 6.2 Risiko (urut berdasarkan dampak)

| # | Tingkat | Risiko | Perbaikan |
|---|---|---|---|
| P1 | TINGGI | Tabrakan nama slot antar destination (§0 #1) [dicek] | Asal destination (tanpa kredensial) masuk ke nama slot/publication baru. Nama slot disimpan di `_apitap_state`. Drop slot hanya kalau state destination ini yang menyebutnya. Tambah leg gate "satu source → dua destination" |
| P2 | TINGGI | Socket half-open → macet selamanya (§0 #5) [dicek] | G0.5 |
| P3 | TINGGI | Transaksi raksasa → OOM berulang (§0 #6) [dicek] | G0.1 |
| P4 | TINGGI | FK CASCADE + delete-then-insert (§0 #8) [agen] | Tolak saat admission bila ada FK yang menyentuh anggota grup, atau `SET LOCAL session_replication_role=replica` bila diizinkan |
| P5 | TINGGI | `logical_decoding_work_mem=1GB` di source (§0 #7) [dicek] | G0.7 |
| P6 | SEDANG | Nilai `'infinity'` pada timestamp/date di-wrap jadi tanggal sampah di ClickHouse: offset epoch dijumlahkan tanpa cek overflow (`rowbinary.rs:291-298`) [dicek], dan build release tidak mengecek overflow. Jalur Arrow dan MySQL menolaknya dengan jelas | `checked_add` + refusal bernama |
| P7 | SEDANG | `numeric` tanpa presisi (tipe uang yang umum) menjadi Float64 untuk destination non-pg tanpa peringatan; error NaN tidak menyebut kolom | Peringatan atau refusal di `check()`; opsi Decimal(38,s) |
| P8 | SEDANG | Guard 0 baris: re-bootstrap atas tabel source yang kosong (outbox/queue) meninggalkan baris lama di destination dan menulis watermark baru di atasnya | Source kosong = swap ke tabel kosong; tolak bila 0 baris tetapi `reltuples > 0` |
| P9 | SEDANG | Span CTID bulk (key UUID/komposit/tanpa key) tanpa snapshot bersama: baris bisa hilang atau ganda | Satu `pg_export_snapshot()` per run |
| P10 | SEDANG | Tidak ada keepalive selama apply panjang (G0.4); tidak ada deteksi `wal_status` (G0.6); slot tidak dibuat dengan `FAILOVER` (PG17), jadi failover HA = re-bootstrap penuh; decoding dari standby (PG16+) gagal dengan error mentah | G0.4, G0.6; opsi `FAILOVER`; error terpandu |
| P11 | SEDANG | Tidak ada pengaturan timeout sesi: `statement_timeout` di level role membunuh COPY panjang; walsender pemegang snapshot mungkin kena `idle_in_transaction_session_timeout`/`transaction_timeout` (verifikasi di rig) | Kirim lewat `options` saat startup, sama seperti `work_mem` |
| P12 | SEDANG | Replace diam-diam membuang trigger, RLS, ownership, default/identity, dan keanggotaan publication; FK dari tabel lain baru gagal saat DROP terakhir | Precheck + peringatan; dokumentasikan |
| P13 | SEDANG | Nama tabel mixed-case gagal di CDC (`::regclass` tanpa quote); `pk_columns` ikut menghitung kolom `INCLUDE` | Quote identifier; filter `indnkeyatts` |
| P14 | SEDANG | `ALTER PUBLICATION … SET (publish_via_partition_root)` dijalankan di **setiap** run, sehingga butuh ownership publication | Cek nilai saat ini dulu |
| P15 | RENDAH-SEDANG | Replay window parsial bisa tidak idempoten (insert→rekey + sel TOAST yang di-mask) | Hilang dengan "satu transaksi per window" (§5.4). Buat tes MERAH dulu |
| P16 | RENDAH | `synchronous_commit` tidak disetel; tidak ada `application_name`; `money` terkirim dalam format locale | `SET LOCAL synchronous_commit=on`; `application_name='apitap/<ver>'`; set `lc_monetary` |

### 6.3 Checklist Postgres untuk user (masuk ke `check()` dan dokumentasi)

1. **Versi dan logical decoding.**
   - PG ≥14 (≥13 untuk parent partisi).
   - Logical decoding aktif:
     - self-managed/Azure: `wal_level=logical`;
     - RDS/Aurora: `rds.logical_replication=1`;
     - Cloud SQL: `cloudsql.logical_decoding=on`;
     - Neon: logical replication diaktifkan;
     - Supabase: pakai host langsung, bukan pooler.
2. **Retensi WAL.** `max_slot_wal_keep_size` di-set dan cukup besar untuk WAL selama satu bootstrap penuh. Pasang alert pada gauge `slot.wal`.
3. **Hak akses.** Role dengan LOGIN + REPLICATION (RDS: `rds_replication`; Cloud SQL/Azure: `ALTER ROLE … REPLICATION`), pemilik tabel yang dipublikasikan, dan punya hak CREATE di database.
4. **Bentuk tabel.** Setiap tabel punya PK, REPLICA IDENTITY DEFAULT, dan nama huruf kecil. Perubahan skema butuh ALTER manual di destination.
5. **Ukuran transaksi.** Transaksi di source jauh di bawah ±100 MB hasil decode bila memori 256 MB. Backfill besar dijalankan sebagai `replace`/`append`.
6. **Timeout.** Role apitap tidak boleh punya `statement_timeout`, `idle_in_transaction_session_timeout`, atau `transaction_timeout`.
7. **Koneksi.** `sslmode=verify-full` + `sslrootcert` CA provider, ke port langsung (bukan pooler).
8. **Destination.**
   - tidak ada FK/trigger di tabel replika;
   - `synchronous_commit=on`;
   - punya hak TEMP;
   - ekstensi dan tipe yang sama terpasang;
   - jadwal swap replace jauh dari query baca yang panjang.
9. **Bulk dari standby.** Butuh `hot_standby_feedback=on`.
10. **Selama perbaikan P2 (macet selamanya) belum dirilis**, pasang timeout eksekusi di scheduler. Drop slot ketika jadwal pipeline dihentikan.

---

## 7. MySQL

### 7.1 Akar masalah throughput grup [dicek]

Grup MySQL tidak pernah memakai lane pool 0.58: `apply_windows` hanya dipanggil dari `drain_group` milik Postgres. Ada tiga sebab:
- **Apply serial.** `run_group_mysql` meng-apply 30 anggota satu per satu, masing-masing dengan unit dan INSERT state sendiri, termasuk anggota tanpa trafik (`run.rs:1240-1247`).
- **Drain tidak tumpang tindih dengan apply.** `drain_windows` menunggu `apply_window` selesai sebelum mendecode window berikutnya (`myrun.rs:265-282`).
- **Window kecil.** Window 14,5 MiB, dan UPDATE ditagih dua image penuh.

Model biaya sederhana, dari sensus statement 0.57 [agen]:
- satu anggota yang aktif menghabiskan ±162 ms per window;
- 30 anggota × 162 ms ≈ 4,9 s per ±15 rb change;
- itu memprediksi ±2,9 rb/s, sementara yang TERUKUR 2.880/s.

Model yang sama memprediksi grup pg 0.57 di ±9 rb/s, dan yang TERUKUR 8.348/s. Jadi selisih apple-to-apple-nya **±2,9×, bukan 10×**. Sisanya berasal dari lane, window 64 MiB, dan CH 25.8 yang dimiliki angka pg.

### 7.2 Risiko kebenaran

| # | Tingkat | Risiko | Status |
|---|---|---|---|
| R1 | TINGGI | Tabel non-transaksional: commit lewat `QUERY "COMMIT"` tidak ditangani; cek `BEGIN` membandingkan potongan 6 byte dengan "begin"; GTID MariaDB mengosongkan buffer (§0 #3). Belum ada precheck storage engine | [dicek] |
| R2 | TINGGI | Charset non-UTF-8 diteruskan mentah (§0 #4); `fetch_schema` tidak membaca `CHARACTER_SET_NAME` | [agen] |
| R3 | TINGGI bila terbukti | MySQL→MySQL dengan kolom TIMESTAMP: decoder menambahkan sufiks `+00` (`mybinlog.rs:761`). Karena OID MySQL selalu 0, `render_my_value` tidak membuangnya (`rowtext.rs:262-272`). Kalau server memberi warning, `no_warnings` menolak **setiap** window (`dest_my.rs:467-493`). **Belum ada leg gate** source MySQL → destination MySQL yang memuat TIMESTAMP (semua leg dengan destination MySQL memakai source Postgres) | [dicek]; butuh tes satu baris |
| R4 | SEDANG | TIME negatif berfraksi rusak: bagian integer meleset satu, fraksi menjadi byte bukan digit; tes hanya mencakup TIME tanpa fraksi | [agen] |
| R5 | SEDANG | Teks DECIMAL tidak kanonik (`0000000012.5000`); DECIMAL presisi >38 masuk sebagai String CH dan diam-diam berbeda dari teks hasil bootstrap | [agen] |
| R6 | SEDANG | Tanggal nol: CDC menulis `0000-00-00`, bulk menolaknya, dan bulk mengubah `2024-00-00` diam-diam menjadi 2023-11-30 | [agen] |
| R7 | SEDANG | `is_ddl` hanya mengecek awalan statement (`mysource.rs:881-886`) [dicek]: `/*…*/ TRUNCATE t` tidak direplikasi dan tidak dilaporkan; pemotongan `s[..n]` bisa panic pada teks non-ASCII | [dicek] |
| R8 | SEDANG | Label ENUM dan signedness diambil dari katalog **sekarang** tetapi diterapkan ke event backlog | [agen] |
| R9 | SEDANG | Transaksi besar menahan image penuh sampai XID → OOM berulang (tuas C dan G0.1) | [agen] |
| R10 | RENDAH-SEDANG | `binlog_row_image` hanya dicek di sesi apitap sendiri; JSON native dan tipe temporal lama MariaDB ditolak (keras, tapi menghambat adopsi); Galera berbagi `server_id` | [agen] |
| R11 | SEDANG | Byte jaringan bisa memicu panic: `&body[..6]` di setiap rows event (`mysource.rs:616`), NEWDECIMAL p=s=0. Panic menjadi `PanicException`, turunan `BaseException`, sehingga lolos dari `except Exception` dan melanggar janji `stability.md`. CRC32 binlog diminta tetapi tidak pernah diverifikasi | [agen] |

Yang sudah bagus:
- event tak dikenal, INCIDENT, JSON parsial, dan payload terkompresi ditolak dengan keras;
- ada guard untuk binlog yang sudah di-purge, posisi yang mendahului server, dan identitas server yang berubah;
- DECIMAL didecode eksak tanpa float;
- insert ke ClickHouse memakai `session_timezone=UTC`;
- `server_id` dibuat unik otomatis.

---

## 8. Lakehouse, BigQuery, source ClickHouse, Arrow read

### 8.1 Vonis per destination: bisakah 50 rb baris/s × 15 kolom di 0,5 CPU / 256 MB?

| Destination | Vonis | Tahap pengikat |
|---|---|---|
| S3/GCS Parquet (bulk) | ESTIMASI ya sampai ±50 jt baris (110–160 rb baris/s). Di atasnya S3 gagal karena CopyObject 5 GiB, dan metadata footer bisa OOM | CPU encode Parquet |
| GCS CSV | ESTIMASI ya | CPU gzip-1 |
| BigQuery bulk | TERUKUR ya, tipis (55–62 rb/s untuk 1 jt baris, build Juli) | round trip load job |
| BigQuery CDC (MERGE) | TERUKUR **tidak** (±2,5 rb/s) | biaya tetap MERGE ±7,3 s + 0,08 s per 1 rb baris |
| Iceberg bulk/merge | ESTIMASI ya (encoder yang sama) | CPU encode |
| Iceberg CDC | belum diketahui, kemungkinan tidak | ±7 panggilan HTTP berurutan per window; metadata terus tumbuh |
| Source ClickHouse | TERUKUR ya, hanya ke ClickHouse (10 jt baris dalam 20,6 s, 125 MB) | server source |
| Arrow read streaming | TERUKUR ya (±806 rb baris/s) | — |
| `to_polars()` (materialisasi penuh) | TERUKUR **tidak** (peak 5,14 GB untuk 10 jt baris) | RAM. Butuh refusal dini yang menyarankan `.lazy()` / `.to_parquet()` |

### 8.2 Temuan reliabilitas

| # | Tingkat | Temuan | Status |
|---|---|---|---|
| H1 | TINGGI | Commit Iceberg yang ambigu menghapus file (§0 #2). Perbaikan: snapshot id dipilih acak oleh klien, jadi setelah respons non-2xx (409 juga), reload tabel dan cari id itu di `meta.snapshots()` sebelum retry atau sweep. Tambah retry 5xx dengan backoff | [dicek] |
| M1 | SEDANG-TINGGI | S3: satu pipe = satu file, difinalisasi dengan CopyObject satu request (maks 5 GiB) → tabel ±140–240 jt baris gagal *setelah* seluruh upload selesai; batas 10.000 part tidak dicek | [agen] |
| M2 | SEDANG | Metadata footer Parquet bertambah seiring jumlah baris dan tidak dihitung planner (+100–200 MB pada 100 jt baris / 2 pipe) | [agen] |
| M3 | SEDANG | Poll `jobs.get` BigQuery bulk tanpa retry; cleanup `quiesce` mengabaikan error poll-nya sendiri | [agen] |
| M4 | SEDANG | BigQuery menganggap setiap 308 berarti chunk utuh diterima, tanpa cek header Range (GCS sudah mengeceknya) | [agen] |
| M5 | SEDANG | Kredensial statis: AWS tanpa default chain/refresh, token katalog Iceberg tanpa OAuth refresh, GCP wajib key file → CDC panjang mati saat token kedaluwarsa | [agen] |
| M6 | SEDANG | Budget retry object store ±1,5 s, hanya untuk 5xx, tanpa 429/408 dan tanpa jitter | [agen] |
| M7 | SEDANG | Source ClickHouse: `count()` lalu stream di query terpisah → gagal pada tabel yang sedang ditulisi (CH tidak punya snapshot lintas query) | [agen] |
| M8 | SEDANG (biaya) | MERGE BigQuery kemungkinan memindai seluruh target di setiap window (belum diukur; cek `INFORMATION_SCHEMA.JOBS.total_bytes_billed`) | [agen] |
| M9 | SEDANG (verifikasi) | Ukuran part multipart tidak seragam; R2 kemungkinan menolak part non-final yang tidak sama besar | [agen] |
| S1 | RENDAH-SEDANG | Sweep file lama S3/GCS bersifat rekursif dan hanya memfilter nama file → menghapus dataset lain yang bersarang di bawah folder tabel | [agen] |
| L5 | SEDANG | Callback FFI Arrow tanpa `catch_unwind`; `blocking_recv` di dalam callback → proses Python **abort** bila dipanggil dari dalam runtime tokio; string skema anak menunjuk ke data milik parent | [agen] |

---

## 9. Production readiness: daftar gabungan

Sudah dideduplikasi dan diurutkan. Detail per sumber ada di §6–§8; di sini hanya posisinya.

**Kehilangan atau kerusakan data diam-diam** (prioritas tertinggi):
1. Tabrakan slot antar destination (P1)
2. Commit ambigu Iceberg (H1)
3. Commit tabel non-transaksional MySQL (R1)
4. Charset MySQL (R2)
5. FK CASCADE di destination Postgres (P4)
6. Pushdown `!=` di `lazy()` (§0 #9)
7. `infinity` yang wrap (P6)
8. Guard 0 baris (P8)
9. Span CTID tanpa snapshot (P9)
10. Sweep S3/GCS bersarang (S1)
11. DDL berawalan komentar (R7)
12. TIME negatif, DECIMAL, tanggal nol (R4–R6)

**Liveness dan memori:**
- Socket half-open (P2)
- Transaksi raksasa dan antrean pump (G0.1/G0.2)
- `slots=N` (G0.8)
- Tanpa keepalive selama apply (G0.4)
- Window tanpa batas waktu (G0.3)
- Slot hangus tanpa panduan (G0.6)
- `to_polars()` tanpa refusal

**Keamanan:**
- Downgrade auth Postgres ke cleartext/MD5 di `prefer`/`require` (`walsender.rs:775-781`) [dicek]. MySQL sudah menolak hal yang sama (`mywire.rs:631-660`). Perbaikan:
  - tolak kode 3/5 kecuali `verify-full` atau opt-in eksplisit (seperti `require_auth` di libpq);
  - SCRAM dengan channel binding.
- SQL di jalur residu `dest_pg` hanya menggandakan `'` dan bergantung pada `standard_conforming_strings` yang tidak disetel (`dest_pg.rs:657-677`) [agen]. Perbaikan: bind parameter, atau minimal `SET standard_conforming_strings=on` per sesi. `from_utf8_lossy` di sana juga diam-diam mengubah byte.
- Identifier ClickHouse tidak meng-escape backslash (`ch_ident`, 40 call site) [agen].
- `logical_decoding_work_mem` di source (P5).
- TLS default tidak seragam [agen]:
  - MySQL sebagai destination memverifikasi sertifikat, sebagai source tidak;
  - ClickHouse memakai HTTP polos + Basic auth tanpa peringatan;
  - Postgres `prefer` diam-diam turun ke plaintext.

**Operabilitas:**
- Progress CDC pg = 0 (G0.9)
- Ctrl-C diabaikan selama satu panggilan penuh
- Kill saat bulk meninggalkan staging sehingga run berikutnya menolak
- Pola resmi `except LockedError: return` menyembunyikan tabel yang berhenti di-refresh **selamanya**
- Bootstrap yang dibunuh meninggalkan slot yatim yang menahan WAL
- Error type tidak bisa membedakan "aman di-retry" dari "salah input"; 10 call site memetakan kegagalan DB menjadi `ValueError`

---

## 10. Gampang dan aman dipakai, tetap satu baris

| Prioritas | Usulan | Yang dilihat user |
|---|---|---|
| P0 | **`apitap.check(src, dst, table=..., mode=...)`**: argumen sama dengan `transfer`, read-only, mengumpulkan semua temuan (bukan berhenti di yang pertama), dibangun di core dari probe yang sudah ada. Isinya: versi, `wal_level`/flag provider, hak REPLICATION, ownership + SQL `CREATE PUBLICATION` persis, slot/sender bebas, `max_slot_wal_keep_size`, slot yatim, TLS yang benar-benar dinegosiasikan, grants MySQL, retensi binlog, kolom JSON, Replicated engine CH, artefak tertinggal beserta umur dan SQL DROP-nya, rencana memori (pipe/chunk/window/lane), dan **"run berikutnya akan: bootstrap \| drain dari X \| menolak karena …"**. Kegagalan prasyarat di `transfer()` membawa laporan lengkap ini | Satu baris yang menampilkan semua yang kurang, lengkap dengan SQL perbaikannya |
| P0 | **Stop dan kill yang bersih**: lane bulk membaca flag stop di batas chunk (jalur error yang sudah ada men-drop staging, dan saat bootstrap juga slot-nya); `check_signals` dipoll tiap ±200 ms di sekitar `py.detach`, lalu `KeyboardInterrupt` di-raise ulang; `LockedError` diberi field `holder`/`artifact`/`age_s`/`drop_sql`; gauge `slot.wal` dipancarkan sebelum `Tenure::acquire` | Deploy dan Ctrl-C membersihkan diri; sisa yang mati memicu alert, bukan bersembunyi |
| P0 | **Supply chain**: `cargo update -p mysql_async` (0.37.1); rustls ke 0.23.45; tokio ke jalur LTS 1.53.x; CI gagal bila ada dependensi langsung yang di-yank; MSRV dibetulkan | — |
| P1 | **Exception yang bisa ditindaklanjuti**: `apitap.Error`, `ConnectError(RuntimeError)` (retry), `PreconditionError(ValueError)` (betulkan server), `ResyncRequired(RuntimeError)` (slot hilang, watermark tertinggal, binlog di-purge), `DataError`. Semuanya turunan kelas yang sudah dijanjikan di `stability.md`. Ditambah **`apitap.reset(src, dst, table=...)`** di bawah guard: membersihkan state, baris pending, slot, dan publication pipeline ini (state ClickHouse/Iceberg bukan sekadar DELETE) | Scheduler tahu kapan harus retry, kapan memanggil manusia |
| P1 | **Observabilitas tanpa biaya per baris**: `serde_json` untuk event (saat ini escaping hanya `"`→`'`, sehingga JSON bisa rusak); event `run.plan`, `window.done{changes, bytes, apply_ms, lag_s, retained_bytes}`, `transfer.error{class, retryable, artifact}`, `transfer.done{caught_up, lag_s, retries, peak_rss, mem_budget}`; ekspor counter per window ke `APITAP_METRICS_FILE` (format textfile Prometheus) dan OTLP/HTTP-JSON lewat reqwest yang sudah ter-link (tanpa SDK OTel, tanpa server) | Lag CDC dan RSS terlihat di dashboard |
| P1 | **Default aman**: aturan loopback untuk semua engine (peringatan sekarang, refusal di minor berikutnya dengan opt-out yang disebut); `logical_decoding_work_mem` mengikuti server; **replace menolak tabel yang sudah ada tanpa state apitap kecuali `overwrite=True`** (peringatan di 0.59, refusal di 0.60) | Tidak ada lagi tabel orang yang tertimpa diam-diam |
| P1 | **Packaging**: CI membangun manylinux aarch64, musllinux, macOS arm64/x86_64, dan sdist. Wheel PGO x86_64 yang lolos gate dinaikkan ke draft release dengan sha256 di tag; `publish.yml` memverifikasinya dan smoke-import di Python 3.9 dan 3.14; image dan action di-pin. **Workflow dipush manual oleh pemilik** (PAT tidak bisa menulis `.github/workflows`) | `pip install apitap` berfungsi di Mac/Graviton/Alpine |
| P1 | **CLI + image**: `python -m apitap transfer\|check\|reset`; secret dari `APITAP_SRC`/`APITAP_DST`, tidak pernah dari argv; exit code 0 / 75 (locked/connect) / 78 (precondition) / 65 (data); `ghcr.io/apitap/apitap:<ver>` non-root + contoh CronJob (`concurrencyPolicy: Forbid`) | Pakai di cron/K8s tanpa menulis Python |
| P1 | **Typing**: `py.typed` + `_apitap.pyi`, `mode` sebagai `Literal`; tiga potong logika engine dipindah dari shim ke Rust: `_predicate_sql` (sumber bug pushdown `!=`), pembaca cgroup root-only di `to_parquet` dan `cpu_quota_cores` (yang juga menentukan runtime) | IDE/mypy paham API; aturan "engine di Rust" terpenuhi |
| P1 | **Dokumentasi**: halaman hak akses Postgres + provider managed; troubleshooting CDC; **panduan sizing** per pasangan source→destination di 0,5/256 dari angka terukur (§2); hapus teks basi (README menunjuk `driver.rs`/`connectors/`; "stateless watermark"; heartbeat di `log_based.md` yang tidak ada di kode) | User tahu apa yang bisa diharapkan sebelum mencoba |
| P2 | **Registri knob**: 26 nama `APITAP_*` (24 dibaca, 14 terdokumentasi; `stability.md` hanya menyebut 6). Semua dibaca sekali, dicetak di `run.plan`, dan typo `APITAP_*` diperingatkan. Knob dev pindah ke `APITAP_DEV_*`. Bypass keamanan `CH_CDC_ALLOW_REPLICATED` jadi argumen API atau dihapus | Konfigurasi bisa ditelusuri |
| P2 | **Wheel lebih ramping**: satukan parquet 54/58; feature-gate Iceberg (arrow 58, avro, moka); naikkan versi reqwest/hyper/jsonwebtoken; hapus fork `sqlx-core` yang hanya mengubah satu konstanta (upstream-kan knob-nya, atau pindahkan COPY-in ke plane hand-rolled; fork berbentuk path package kemungkinan tak terlihat oleh `cargo audit`) | Wheel lebih kecil, audit lebih jujur |

Semua usulan di atas mempertahankan aturan keras proyek:
- `check`, `reset`, dan `follow=` masing-masing tetap **satu baris**;
- loop dan logikanya ada di `apitap-core`.

---

## 11. Supply chain dan dependensi

| Item | Fakta | Tindakan |
|---|---|---|
| `mysql_async 0.37.0` | **Sudah di-yank** (dicek lewat API crates.io hari ini); 0.37.1 memperbaiki data race statement cache | Naikkan sekarang; CI gagal bila ada yang di-yank |
| `rustls 0.23.41` | Masuk rentang advisory GHSA-2mjx-qc3c-rqvc (moderate; 0.23.13–0.23.44) menurut laporan rust-daily | Verifikasi dengan `cargo audit` di VPS, lalu naikkan ke 0.23.45 |
| `tokio 1.52.3` | Bukan jalur LTS. 1.53.x didukung sampai Sep 2027 dan memperbaiki bug mpsc/timer | Naikkan ke 1.53.2 |
| `parquet` 54.3.1 + 58.4.0 | Dua versi dikompilasi (54 langsung, 58 dari iceberg) | Satukan ke 58 |
| `rust-version = "1.75"` | Salah: kode memakai `repeat_n` (1.82+); pyo3 0.29 butuh 1.83, parquet 58 butuh 1.85, iceberg 0.10 butuh 1.94 | Set ke nilai yang benar |
| Fork `vendor/sqlx-core` | 95 file untuk satu konstanta; upstream sqlx sudah 0.9 (MSRV 1.94, API query berubah) | Rencanakan jalan keluarnya |
| Lisensi | 415 komponen permisif; notice BSD/ISC/Zlib/Unicode wajib disertakan | PEP 639 `license-files` + cargo-about |
| Dari dunia Rust 6 bulan terakhir | `Allocator` trait distabilkan (Rust 1.100, ±12 Nov), sehingga arena per batch bisa dibuat di stable (relevan untuk L5/L6); `format_into` (1.98) bisa menggantikan `itoa`; parquet 59/60 mendukung zstd level negatif (lebih hemat CPU); pyo3 0.29 mendukung `abi3t` untuk Python free-threaded | Masukkan ke rencana L5/L6 dan encoder Parquet |

---

## 12. Koreksi atas catatan internal

Agar angka publik tetap jujur:

1. **Stress "0 window dalam 78 detik" adalah artefak harness, bukan perilaku engine** [dicek]. Di `bench-capped-pg-ch-cdc-stress-0.57.sh:640` writer dijalankan lewat `… 2>&1 | tee`. Grup `{ echo; cat sql; }` milik setiap sesi latar mewarisi stderr dari pipe `tee` itu (`:329-333`), sehingga `tee` baru selesai ketika writer hampir selesai. Akibatnya drain baru mulai setelah writer selesai, ketika slot sudah hangus:
   - log slice tidak memuat gauge `slot.wal`;
   - error `55000` adalah penolakan *saat start*.

   Perilaku engine di 500 rb change/s **belum terukur**. Ulangi dengan drain dijalankan duluan.
2. **"Transport <10%"** hanya menjumlahkan 7 simbol kernel. Pengelompokan penuh menunjukkan ±29% kerja jaringan di loopback. Kesimpulan "Unix socket ≈ +5%" tetap benar; kesimpulan "buffer/transport bukan tuas" tidak berlaku untuk biaya per kedatangan paket (§5.3 L2).
3. **"Allocator ~18%"** = seluruh libc. Keluarga malloc ±10,6%, dan 4,5% berada di satu alamat yang belum ter-resolve.
4. **"Walsender punya headroom 2,5×"** diukur di mode backlog. Di keep-up, B2.4 mengindikasikan 30–33 µs/change di walsender (§5.7 #1).
5. **Angka headline CDC memakai window 32/64 MiB lewat env**, sedangkan default user 14,5 MiB (§2).
6. **`bench-capped-my-ch-cdc-0.57.md` menyebut "writer was the bottleneck"**. Keliru: drain berhenti di stop-line yang ditangkap saat mulai, jadi 2.880/s adalah laju drain sendiri [agen].
7. **Drift dokumentasi** [agen]:
   - `failure-modes.md:51` ("recovers with a fresh bootstrap") melebih-lebihkan;
   - `usage.md:1151` ("memory budget divided by N") tidak berlaku untuk bootstrap;
   - `usage.md:959` ("checked loudly"): `wal_level` sebenarnya tidak di-precheck;
   - komentar "kept LOW" di `walsender.rs:1250-1251` bertentangan dengan default 1 GB;
   - README menunjuk file yang sudah tidak ada.

---

## 13. Yang sudah bagus (pertahankan)

- **Disiplin bukti.**
  - `gate.py --self-test` membuktikan gate bisa gagal, dan `--matrix` membuktikan setiap klaim punya leg.
  - Catatan benchmark mencantumkan md5 `.so` dan mempublikasikan kegagalannya sendiri.
  - Budaya ini yang membuat koreksi di §12 bisa dilakukan.
- **Kebenaran inti CDC Postgres.**
  - Handoff snapshot↔slot sesuai buku teks.
  - Confirm-after-commit.
  - Fence lease dikunci dalam transaksi yang sama dengan data dan watermark.
  - `TableWindow::seal` membuat wedge 0.56 mustahil direpresentasikan.
- **Refusal yang menyebut sebab dan perbaikannya**, di hampir semua jalur.
- **Memori bulk yang terbatas by design.**
  - Planner berbasis residensi terukur dengan bukti OOM.
  - Pipe "thin" otomatis.
  - Lane CDC yang sadar quota.
- **`unsafe` minim dan sound.**
  - Pembacaan unchecked di `arrowcol` didahului bukti batas.
  - Chaining sinyal SIGTERM menghormati `SA_SIGINFO`/`SIG_IGN`.
  - Ownership release di FFI Arrow benar.
  - Tidak ada `unsafe` di file hot path CDC.
- **Panic di bulk ditangkap** (`catch_unwind`, `JoinSet` yang selalu di-join, latch kegagalan), sehingga staging tetap dibersihkan.
- **Keputusan sulit yang benar.**
  - ClickHouse tidak me-retry exception statement (bisa menggandakan append).
  - Key table dipertahankan karena IN-list inline terukur 3,6× lebih lambat.
  - current_thread dipertahankan sebagai kemenangan memori.
- **Hot path sudah banyak yang benar.**
  - Sel disimpan sebagai rentang di dalam frame.
  - foldhash dengan satu `entry()` per event.
  - Escape SWAR.
  - Tidak ada timer per pesan.
  - Body diserahkan ke reqwest tanpa salinan.

---

## 14. Urutan kerja yang disarankan

| Rilis | Tema | Isi | Kriteria selesai |
|---|---|---|---|
| **0.59 "aman dulu"** | Tutup kehilangan data diam-diam + liveness | P1 (nama slot), H1 (Iceberg), R1/R2 (MySQL), P4 (FK), pushdown `!=`, G0.1–G0.9, downgrade auth, bind parameter di `dest_pg`, bump dependensi + MSRV | Setiap perbaikan punya tes yang terlihat MERAH tanpa perbaikannya; leg baru "satu source → dua destination", "MyISAM/Aria", "latin1", "FK cascade", "socket half-open", "transaksi 1 jt baris di 256 MB"; gate 80+N/80+N |
| **0.60 "struktural"** | Throughput murah + kemudahan | G1.1–G1.5 (window default, MySQL lewat `apply_windows`, follow mode, runtime di core), `check()`, exception + `reset()`, event JSON + metrik textfile, Ctrl-C/kill yang bersih | my→ch 30 tabel ≥10 rb/s TERUKUR (n≥3, 30/30 MATCH); pg→ch keep-up dengan default ≥28 rb/s; sizing guide terbit |
| **0.61 "CPU per change"** | Kejar 3 jt/menit | Ukur §5.7 dulu; lalu L1a → L1b → L3 → L4–L6 → A/B L2; tuas MySQL C–G; satu transaksi per window di pg; mode insert-only CH (opsional) | pg→ch catch-up ≤11 µs/change TERUKUR; target 50 rb/s dinilai dari angka, bukan estimasi |
| **0.62+** | Destination cloud | BigQuery Storage Write API + upsert CDC native; perbaikan encoder Parquet + rotasi file; kredensial yang bisa refresh; Iceberg: statistik file + merge manifest | BigQuery CDC ≥10 rb/s; S3 100 jt baris lolos di 256 MB |

---

## 15. Metode, batasan, dan skill yang dipakai

**Metode.**
- Tujuh reviewer paralel dengan effort max, ditambah satu laporan berita Rust 6 bulan terakhir.
- Saya lalu membaca ulang sendiri baris yang dikutip untuk temuan terberat (bertanda [dicek]): ±25 titik di `run.rs`, `myrun.rs`, `mysource.rs`, `mybinlog.rs`, `rowtext.rs`, `window.rs`, `dest_my.rs`, `walsender.rs`, `drain.rs`, `rowbinary.rs`, `iceberg.rs`, `__init__.py`, `py-apitap/src/lib.rs`, `Cargo.lock`, dan harness stress.

**Batasan.**
- **Tanpa build, tanpa run.** Sesuai aturan proyek, build hanya boleh di VPS.
- **Analisis berbasis LSP diganti grep.** rust-analyzer akan menjalankan cargo, yang dilarang di mesin ini.
- **Semua ESTIMASI adalah hipotesis.** Urutannya di §14 dirancang agar setiap hipotesis bertemu angka terukur sebelum dijanjikan.
- **Persentase profil berasal dari regime yang berbeda** dari total 15,35 µs (§4).

**Skill Rust yang dipakai dan apa yang dihasilkan masing-masing lensa.**

| Skill | Hasil |
|---|---|
| m10-performance | Memisahkan tembok terukur dari tuas; menemukan tebing residu TOAST dan biaya per kedatangan paket |
| m01-ownership | Promosi alokasi per frame hanya karena frame beremilik tunggal di-clone; churn clone TableMap/TableSchema per event di MySQL |
| m02-resource | `Bytes`/`Arc<str>` per change yang tidak perlu; antrean pump yang dibatasi jumlah frame; body upload yang di-clone per retry |
| m03-mutability | Semua `std::sync::Mutex` bebas `await`; counter `Cell` MySQL single-thread (aman) |
| m04-zero-cost | Lapisan terjemahan pgoutput di jalur MySQL; map drain masih SipHash sementara collapser sudah foldhash |
| m05-type-driven | `BulkMode` tanpa `LogBased`, newtype `PgLsn`/`BinlogPos`, opsi per destination berbentuk enum |
| m06-error-handling | Slice jaringan tanpa guard di MySQL; `PanicException` lolos dari `except Exception`; overflow `infinity` |
| m07-concurrency | Peta serialisasi pipeline (S0–S4), MySQL tanpa overlap, keepalive yang kelaparan saat apply |
| m09-domain | Memori source dan semantik collation sebagai aturan domain yang belum dimodelkan engine |
| m11-ecosystem | `mysql_async` di-yank, parquet ganda, fork sqlx, wheel hanya x86_64 |
| m12-lifecycle | Slot yatim setelah kill, sesi pemegang snapshot dan timeout-nya, churn temp table, lease yang hidup lebih lama dari socket mati |
| m13-domain-error | Error tidak membawa "aman di-retry"; "status tidak diketahui" pada commit Iceberg; budget retry object store |
| m14-mental-model | Asumsi "jalankan ulang = pemulihan" patah di tiga tempat |
| m15-anti-pattern | Fungsi kembar `run_group`/`run_group_mysql`, error berbasis String, cek `slots` ditulis tiga kali |
| unsafe-checker | Tidak ada `unsafe` di hot path; abort di callback `extern "C"`; celah read-then-set di `shutdown.rs` (bukan UB) |
| coding-guidelines | MSRV salah; fungsi raksasa (`drain` 307 baris, `drain_binlog` 300, `dest_state` 292) |
| rust-router | Merutekan cakupan keamanan ke lensa yang tepat |
| rust-refactor-helper | Rencana pemecahan tanpa perubahan perilaku: `WindowAcc` bersama untuk `drain`/`drain_binlog`, `run_group_with<L: CdcLane>`, `dest_state`/`finalize_inner` dipecah |
| rust-call-graph, rust-symbol-analyzer, rust-code-navigator, rust-trait-explorer | (versi grep) rantai panggilan hot path pg dan MySQL; ukuran tipe; peta runtime/spawn/join; dispatch `Source`/`Sink`/`Fence` |
| rust-deps-visualizer | 441 paket terkunci, 31 crate dalam lebih dari satu versi |
| rust-learner | Selisih versi upstream (tokio, rustls, reqwest, hyper, parquet, sqlx 0.9) |
| rust-daily | Advisory rustls, LTS tokio, stabilisasi `Allocator`, parquet 60, pyo3 `abi3t` |
| domain-embedded | Desain "batasi dengan byte, daur ulang buffer, nol alokasi per item" |
| domain-fintech | NUMERIC→Float64, `infinity`, DECIMAL tidak kanonik, TIME negatif, `money` ber-locale, FK cascade |
| domain-cloud-native | Flag dan role Postgres managed, pooler, failover slot, decoding dari standby, SIGTERM di K8s |
| domain-cli | Tidak ada CLI dan kontrak exit code; Ctrl-C |
| domain-web | Job batch sebaiknya *push* metrik; `/livez`/`/readyz` baru perlu untuk follow mode |
| domain-iot | Telemetri per window sudah tepat; decode 1 GB di source dan wheel x86-only bertentangan dengan "box kecil murah" |
| domain-ml | Hand-off zero-copy Arrow ke pandas/polars/DuckDB sudah benar; tebing offset i32 di jalur materialisasi |
| python-pro | Tidak ada `py.typed`/stub, `mode: str`, belum ada hierarki exception, logika engine di shim |
| meta-cognition-parallel | Dipakai dalam mode inline (file agen layer tidak terpasang), menghasilkan sintesis tiga lapis di §0 dan §5: mekanisme (budget 10 µs), desain (gelombang), domain/produk (janji yang jujur) |
| rust-skill-creator | Tidak relevan untuk review (fungsinya membuat skill baru) |
