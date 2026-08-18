# apitap-lib — Review Kesiapan Production

**Versi ditinjau:** workspace `0.42.0` (commit `34df16c`)
**Cakupan:** 37.356 LOC Rust (61 file) + 808 baris wrapper Python + docs + CI
**Metode:** review statis penuh atas seluruh source. **Tidak ada build maupun test yang dijalankan** — sesuai instruksi, eksekusi harus di VPS kamu, bukan di lokal/container ini.
**Tanggal:** 18 Agustus 2026

---

## 1. Verdict

Jawaban singkat: **belum, tapi lebih dekat daripada yang saya duga.**

Ini bukan review "kode jelek". Jalur Postgres di codebase ini — snapshot→stream handoff, disiplin ack-after-commit, penanganan TOAST, model memori — **lebih benar daripada kebanyakan tool CDC komersial yang saya pernah baca**, termasuk Debezium pada beberapa titik. Yang menahan production readiness bukan arsitektur, tapi hal-hal spesifik dan bisa diperbaiki.

Verdict per jalur:

| Jalur | Verdict | Blocker |
|---|---|---|
| **Bulk transfer dari Postgres** (`replace`/`append`/`merge`) → pg/ch/bq/gcs/s3/iceberg | 🟡 **Layak untuk pekerjaan yang bisa di-rerun** | Timeout HTTP (#8), cap per-value (#11) |
| **Bulk transfer dari MySQL** | 🟡 **Layak, dengan catatan** | Sama + TLS (#2) |
| **CDC `log_based` dari Postgres** | 🟠 **Hampir — jangan dulu untuk unattended** | Idle-WAL pin (#4), memori streamed-tx (#12) |
| **CDC `log_based` dari MySQL** | 🔴 **JANGAN dipasang** | Korupsi data diam-diam pada tipe kolom biasa (#1) |
| **Deployment melewati jaringan tak terpercaya** (cloud→cloud, lintas VPC, internet) | 🔴 **JANGAN dulu** | TLS (#2, #3) |

Menariknya, verdict untuk jalur bulk **sama persis dengan rekomendasi penulisnya sendiri** di `docs/stability.md`:

> *"treat apitap as excellent at work you can re-run — backfills, migrations, warehouse rebuilds, `read()` for analysis — and pin it if it sits anywhere you cannot re-run."*

Itu penilaian yang jujur dan akurat. Review ini pada dasarnya mengonfirmasinya, lalu menambahkan hal-hal yang belum masuk daftar 1.0 penulis: **TLS, timeout, dan korupsi tipe MySQL CDC.**

---

## 2. Ringkasan Temuan

Semua temuan di bawah **sudah saya verifikasi langsung ke source**, bukan hasil tebakan agent.

| # | Sev | Area | Ringkas | Lokasi |
|---|---|---|---|---|
| 1 | 🔴 **CRITICAL** | Korupsi data | MySQL CDC: `ENUM`/`SET`/`JSON`/`BIT`/`TIME` di-decode salah; berbeda dari bootstrap | `wire/mybinlog.rs:481-542` |
| 2 | 🔴 **CRITICAL** | Keamanan | MySQL raw plane: verifikasi sertifikat **dimatikan secara default**, bisa di-downgrade ke plaintext | `wire/mywire.rs:82, 285-330` |
| 3 | 🔴 **CRITICAL** | Keamanan | Postgres walsender/COPY: **plaintext saja**, `sslmode=prefer` diterima diam-diam, password cleartext | `wire/walsender.rs:12, 132, 277` |
| 4 | 🔴 **CRITICAL** | Ops / DB source | Tabel idle menahan WAL selamanya; menjalankan drain **tidak** membebaskannya | `logbased/drain.rs:127-139`, `run.rs:1145, 1259-1281` |
| 5 | 🟠 HIGH | Integritas | Watermark MySQL tanpa identitas server — guard posisional ada dan teruji, tapi bocor setelah server melewati posisi lama | `logbased/mysource.rs:28-35`, `myrun.rs:175-190` |
| 6 | 🟠 HIGH | Korupsi data | MySQL: nama kolom dari katalog **live**, nilai dari `TABLE_MAP` **historis**, tanpa cek jumlah kolom | `logbased/mysource.rs:301-319` |
| 7 | 🟠 HIGH | Korupsi data | Postgres: perubahan `Relation` di tengah window me-relabel seluruh window | `logbased/drain.rs:181-188, 247` |
| 8 | 🟠 HIGH | Ops | **Nol timeout** pada 10 HTTP client — koneksi menggantung selamanya | 10 lokasi (lihat §3.8) |
| 9 | 🟠 HIGH | Keamanan | BigQuery: identifier dibangun tanpa escaping sama sekali | `sink/bigquery.rs:432`, `logbased/dest_bq.rs:425` |
| 10 | 🟠 HIGH | Korupsi data | Apply MySQL pakai `sql_mode=''` → truncation diam-diam, permanen | `logbased/dest_my.rs:201-205` |
| 11 | 🟠 HIGH | Memori | Tidak ada cap byte per-value: satu baris lebar → OOM | `wire/walsender.rs:503`, `source/postgres.rs:900` |
| 12 | 🟠 HIGH | Memori | Transaksi streamed proto-v2 lolos dari budget memori | `logbased/drain.rs:61, 101` |
| 13 | 🟠 HIGH | Ops | ClickHouse **tanpa retry sama sekali** — `TOO_MANY_PARTS` fatal | `sink/clickhouse.rs`, `logbased/dest_ch.rs` (0 hit) |
| 14 | 🟠 HIGH | Robustness | Alokasi tak terbatas dari panjang wire + slicing tanpa bounds check | `wire/walsender.rs:98, 374-386, 803-813`; `wire/pgoutput.rs:481` |
| 15 | 🟡 MED | Memori | Deteksi cgroup hanya baca root cgroupfs → cap memori mati diam-diam | `pipeline/mod.rs:93-105` |
| 16 | 🟡 MED | Keamanan | Escaping literal watermark hanya `'` — salah untuk MySQL & ClickHouse | `pipeline/mod.rs:227` |
| 17 | 🟡 MED | Keamanan | Pagination GitHub mengikuti URL dari header `Link` sambil membawa token | `source/github_api.rs:506, 884` |
| 18 | 🟡 MED | Dokumentasi | `failure-modes.md` mengklaim atomicity yang tidak berlaku untuk ClickHouse & BigQuery | `docs/failure-modes.md:21` |
| 19 | 🟡 MED | Rilis | Wheel yang di-publish ke PyPI **bukan** wheel yang diuji CI | `.github/workflows/publish.yml:69-79` |
| 20 | 🟡 MED | Rilis | `Cargo.lock` tercatat `0.20.0` sementara manifest `0.42.0` | `Cargo.lock` vs `Cargo.toml:6` |
| 21 | 🟡 MED | Kualitas | Nol lint hardening; nol fuzzing/Miri atas decoder yang memakai pointer arithmetic | seluruh crate |
| 22 | 🟡 MED | Observability | Tidak ada `tracing`/`log`, tidak ada metrik yang bisa di-scrape | seluruh crate |
| 23 | 🟡 MED | Test | ~4.400 baris kode paling berisiko tanpa test sama sekali | lihat §5 |

---

## 3. Temuan Kritikal — Detail

### 🔴 #1 — MySQL CDC merusak `ENUM`, `SET`, `JSON`, `BIT`, dan `TIME` secara diam-diam

Ini temuan paling serius di seluruh review, karena **tidak menimbulkan error apa pun.**

Decoder binlog mengembalikan indeks numerik untuk `ENUM`/`SET`, dan byte biner mentah untuk `JSON`:

```rust
// wire/mybinlog.rs:494-497
MT_ENUM => {
    let v = if c.meta & 0xFF == 1 { r.u8()? as u64 } else { r.u16le()? as u64 };
    s(v.to_string())          // menulis "3", bukan "shipped"
}

// wire/mybinlog.rs:519-532
MT_JSON => {
    // Binary JSON — the caller re-renders; carrying the raw bytes
    // keeps this decoder pure ...
    Ok(r.take(n)?.to_vec())
}
```

Komentar itu bilang *"the caller re-renders"*. **Tidak ada caller yang me-render.** Saya grep seluruh tree: `MT_JSON` hanya muncul 3 kali, semuanya di dalam `mybinlog.rs` sendiri. Tidak ada renderer binary-JSON di mana pun.

Sementara itu jalur bootstrap (bulk load) memetakan tipe yang sama sebagai **teks**:

```rust
// source/mysql.rs:126-128
"char" | "varchar" | ... | "enum" | "set"
| "json" | "time" => (MyRb::Str, Delivered::Text),
"binary" | ... | "bit" => { /* hex → UNHEX */ }
```

Jadi bootstrap dan CDC menulis **nilai yang berbeda untuk kolom yang sama.**

**Skenario kegagalan konkret:**

`orders.status ENUM('new','paid','shipped')`.
Bootstrap memuat 10 juta baris dengan `status = 'shipped'`. Satu jam kemudian sebuah CDC window meng-update satu order. Baris itu sekarang berisi string **`"3"`**. Tabel tujuan kini campur `'shipped'` dan `'3'` untuk nilai logis yang sama. Setiap dashboard `WHERE status = 'shipped'` diam-diam kurang hitung, dan selisihnya bertambah setiap window. Tidak ada error di mana pun. Tidak ada log. Tidak ada yang tahu sampai ada yang mencocokkan angka secara manual.

`SET` lebih parah: `'read,write'` → `"3"` (bitmask).
`JSON`: `{"a":1}` saat bootstrap, lalu envelope biner MySQL (byte tipe, offset, NUL tertanam) saat CDC.
`BIT`: bootstrap simpan byte via `UNHEX`, CDC simpan `"5"`.

**`TIME` negatif menghasilkan sampah.** Saya verifikasi numerik terhadap encoding `TIME2` MySQL (bias `0x800000`, negatif = komplemen):

```rust
// wire/mybinlog.rs:444-448
let packed = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
let v = packed & 0x7F_FFFF;      // buang bit tanda, berhenti di situ
```

| Nilai sumber | packed | apitap decode jadi |
|---|---|---|
| `+01:00:00` | `0x801000` | `01:00:00` ✅ |
| `-01:00:00` | `0x7FF000` | **`1023:00:00`** ❌ |
| `-00:00:01` | `0x7FFFFF` | **`1023:63:63`** ❌ |
| `-838:59:59` | `0x4B9105` | **`185:04:05`** ❌ |

`push2()` punya `debug_assert!(v < 100)`, tapi di release build fallback ke `v % 100` — jadi tidak ada yang menangkapnya di production.

**Dan tidak ada precheck yang menolak tipe-tipe ini.** Saya cari; tidak ada.

**Perbaikan minimum sebelum jalur ini boleh dipakai:**
- `fetch_schema` (`mysource.rs:208`) **sudah membaca** `information_schema.COLUMN_TYPE`, yang berisi daftar label `ENUM`/`SET` — lalu membuangnya. Simpan dan pakai untuk resolusi label.
- Implementasikan renderer binary-JSON → teks, atau tolak kolom `JSON` di precheck.
- Perbaiki tanda `TIME2`, samakan `BIT` dengan jalur bootstrap.
- **Sampai itu selesai: tolak kolom `ENUM`/`SET`/`JSON`/`BIT`/`TIME` di precheck.** Gagal keras jauh lebih baik daripada korupsi diam-diam.

---

### 🔴 #2 & #3 — Transport: MITM-able secara default di kedua sumber

**MySQL raw plane** (`wire/mywire.rs`) — default-nya `SslPref::Preferred`, dan verifier-nya menerima sertifikat apa pun:

```rust
// mywire.rs:82
let mut ssl = SslPref::Preferred;

// mywire.rs:285-295
impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(...) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())   // terima sertifikat APA PUN
    }
```

Tiga masalah bertumpuk:
1. **Tidak ada autentikasi peer.** `ssl-mode=required` juga kena — hasilnya kanal terenkripsi ke pihak yang tidak terverifikasi.
2. **Downgrade ke plaintext.** `Preferred` memutuskan pakai TLS berdasarkan bit `CLIENT_SSL` di greeting server yang dibaca **di atas TCP polos**. Attacker on-path cukup membersihkan bit itu.
3. **Password cleartext.** Pada jalur `AuthMoreData(0x04)` ("full auth"), password dikirim polos jika `use_tls` — dan `use_tls` bisa true terhadap sertifikat palsu.

**Kredit yang pantas:** `verify_ca`/`verify_identity` ditolak dan jatuh ke jalur sqlx yang memverifikasi beneran (`mywire.rs:88-93`). Jadi konfigurasi aman itu **ada** — masalahnya dia bukan default, dan penurunan jalurnya senyap kecuali `APITAP_DEBUG` diset.

**Postgres walsender** (`wire/walsender.rs`) — tidak ada kode TLS sama sekali di file itu. Modul doc-nya jujur (*"TLS termination lands next"*), tapi:

```rust
// walsender.rs:132
if k == "sslmode" && v != "disable" && v != "prefer" {
    return Err(...);   // require/verify-* ditolak keras — bagus
}
// walsender.rs:277
3 => self.send_password(&ci.password).await?, // cleartext
```

`prefer` **adalah default libpq**, artinya "pakai TLS kalau server menawarkan". Di sini diterima lalu diimplementasikan sebagai TCP polos, tanpa peringatan. User yang menempelkan DSN yang jalan di `psql` mendapat koneksi cleartext dan tidak diberi tahu. Dan ini bukan cuma jalur CDC — `connect_sql()` (`walsender.rs:200`) adalah **data plane COPY OUT** untuk transfer bulk biasa.

Implementasi SCRAM-SHA-256-nya sendiri benar (saya cek terhadap RFC 7677, nonce 128-bit, prefix server dicek) — tapi SCRAM di atas plaintext tidak bisa mencegah downgrade ke kode auth 3/5.

**Perbaikan:** jadikan verifikasi sertifikat default di `mywire`; sediakan `NoVerify` hanya lewat opt-in eksplisit yang namanya keras (`ssl-mode=required-noverify`). Jangan pernah kirim password cleartext ke peer yang tidak terverifikasi. Untuk walsender: tolak `prefer` sekeras `require` sampai TLS ada.

---

### 🔴 #4 — Tabel idle menahan WAL selamanya, dan menjalankan drain tidak membebaskannya

Ini yang paling berbahaya buat **database sumber**, bukan buat apitap.

Channel `applied` diinisialisasi dengan watermark tersimpan:

```rust
// logbased/run.rs:1145
let (applied_tx, mut applied_rx) = tokio::sync::watch::channel::<u64>(wm);
```

Saat drain menemui keepalive, ia melaporkan LSN itu:

```rust
// logbased/drain.rs:127-133
Some(WalEvent::Keepalive { wal_end, reply_requested }) => {
    if reply_requested {
        ws.standby_status(*applied.borrow(), false).await?;
    }
    if wal_end >= stop_line && tx_buf.is_empty() && sess.streams.is_empty() {
        break;   // caught up
    }
}
```

Kalau tabel yang di-publish tidak ada perubahan, `end_lsn == start_lsn`, sehingga di `run.rs`: `if end > cur` false → tidak ada window dikirim → `pending` tetap `None` → **`standby_status` tidak pernah dipanggil dengan nilai baru sepanjang run.** `confirmed_flush_lsn` tidak bergerak.

**Skenario konkret:** `orders` di-publish dan di-drain tiap jam. Long weekend, `orders` tidak ada tulisan, tapi database lain di instance yang sama menghasilkan 40 GB WAL/hari. pgoutput hanya mengirim keepalive. Setiap run per jam connect, lihat "caught up", lapor sukses — dan **tidak meng-confirm apa pun**. Setelah 3 hari slot menahan ~120 GB WAL. Disk source penuh → **outage database production**, bukan cuma pipeline gagal.

Ini **bertentangan langsung** dengan saran pemulihan di dokumentasi sendiri (`docs/failure-modes.md:32`): *"Run the drain: a backlog is not a reason to refuse, it is a reason to run."* Dalam kasus ini menjalankan drain tidak menolong.

Yang bikin ini benar-benar layak diperbaiki: **desainnya sudah mengantisipasi ini.** `docs/design/log_based.md:53` menulis langkah 6:

> `heartbeat: INSERT into apitap._heartbeat (published, never synced)`

Saya grep seluruh `crates/`: kata `heartbeat` tidak muncul sekali pun di jalur logbased Postgres. Fitur yang dirancang, tidak diimplementasi.

**Kredit:** `slot_wal_report` (`run.rs:1407-1461`) sudah mengukur dan memperingatkan pertumbuhan WAL tiap drain, bahkan membaca `max_slot_wal_keep_size` dan menyarankan menyetelnya. Itu lebih dari yang dilakukan hampir semua tool. Masalahnya, peringatannya menyuruh melakukan hal yang tidak menyelesaikan masalah.

**Perbaikan (salah satu cukup):** (a) saat drain break pada keepalive caught-up **dan** semua window sebelumnya sudah applied, kirim `standby_status(wal_end)` — aman karena watermark tujuan sudah durable sampai `stop_line`; atau (b) implementasikan tabel heartbeat sesuai desain.

---

### 🟠 #5 — Watermark MySQL tanpa identitas server (guard posisional ada, tapi tidak lengkap)

**Koreksi: temuan ini saya turunkan dari CRITICAL ke HIGH setelah kamu menjalankan `e2e_cdc_retention.py`.** Awalnya saya laporkan bahwa `RESET MASTER` menyebabkan resume senyap ke stream yang salah. Itu **tidak benar** — ada guard eksplisit yang menangkapnya, dan test barumu membuktikannya lulus:

```rust
// logbased/myrun.rs:175
if start > stop_line {
    // "the stored position (…) is AHEAD of the server's current one (…).
    //  A binlog only grows, so this server's log was reset or rebuilt …"
```

Komentar di atasnya bahkan mencatat bahwa ini dulunya *adalah* lubang senyap, ditemukan oleh release smoke. Ditambah `binlog_file_present` untuk kasus purge, dua mode kegagalan paling umum sudah ditutup dengan pesan yang menyebut penyebab dan pemulihannya. Itu kerja yang bagus.

**Yang tersisa:** guard-nya **posisional, bukan identitas**. Ia hanya menyala kalau posisi tersimpan lebih *tinggi* dari posisi live. Kasus yang lolos adalah ketika server sudah **melewati** posisi lama dengan isi berbeda:

```rust
// logbased/mysource.rs:28-35 — tidak ada server_uuid, server_id, atau GTID set
pub(crate) fn pack_pos(file: &str, pos: u32) -> u64 {
    let idx: u64 = file.rsplit('.').next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    (idx << 32) | pos as u64
}
```

**Skenario yang masih lolos:** watermark = `binlog.000007 @ 91.234.112`. Server di-`RESET MASTER` (atau di-restore dari backup, atau URL diarahkan ke replika yang dibangun ulang). Kalau apitap dijalankan **segera**, guard menyala — benar. Tapi kalau tabel itu tidak di-drain selama beberapa hari sementara database menulis ~6 GB binlog, counter melewati `000007` posisi 91 M lagi dengan isi yang sama sekali berbeda. Sekarang `start > stop_line` **false**, `binlog_file_present("binlog.000007")` **true**, dan `COM_BINLOG_DUMP` mulai di byte 91.234.112 dari sejarah yang berbeda. Diam-diam.

Jendelanya lebih sempit daripada yang saya tulis pertama kali, tapi persis bertepatan dengan kondisi yang membuat orang melakukan `RESET MASTER` sejak awal: pemulihan, rebuild, migrasi — di mana pipeline memang sering tidak jalan selama beberapa hari.

**Perbaikan:** simpan `@@server_uuid` bersama posisi dan tolak kalau tidak cocok. Itu mengubah guard dari heuristik posisional jadi cek identitas, dan menutup sisa kasusnya tanpa menyentuh yang sudah jalan. Idealnya pindah ke GTID (`COM_BINLOG_DUMP_GTID`), yang membuat resume aman-failover mungkin sama sekali.

**Catatan terpisah dari run-mu:** leg `MariaDB: resuming from a vanished binlog` gagal karena harness-nya, bukan karena engine — `RESET MASTER` **membuat ulang** `mariadb-bin.000001`, jadi state berbahaya yang mau diuji tidak pernah tercipta dan `binlog_file_present` benar-benar menemukan file itu. Untuk melenyapkan file pertama secara spesifik: `FLUSH BINARY LOGS` beberapa kali sampai current ada di `.000004+`, pastikan tidak ada replika terhubung, lalu `PURGE BINARY LOGS TO 'mariadb-bin.000004'`. Kalau checkpoint InnoDB masih menahannya, dorong dulu dengan tulisan + `FLUSH LOGS` lagi sebelum PURGE. Sekarang leg itu melaporkan FAILED padahal yang gagal adalah pembuatan prasyaratnya — layak dibedakan dari kegagalan asli.

---

## 4. Temuan HIGH — Ringkas

**#6 — MySQL: schema live vs TABLE_MAP historis, tanpa cek arity.** `mysource.rs:301-319` mengambil schema dari `information_schema` **saat TABLE_MAP pertama terlihat**, lalu memasangkan nama-nama itu ke tuple yang di-decode dengan definisi kolom historis — tanpa cek `schema.names.len() == map.cols.len()`. Skenario: `ALTER TABLE orders DROP COLUMN region, ADD COLUMN currency CHAR(3)` (jumlah kolom tetap 3). Baris-baris pre-ALTER di-decode sebagai `[id, region, amount]` tapi dilabeli `[id, amount, currency]`. **Nilai `region` ("EMEA") masuk ke kolom `amount`.** Jumlah kolom cocok, jadi tidak ada error. Kalau jumlahnya beda, COPY gagal keras — itu keberuntungan, bukan desain. Di mode `changelog`, jaring pengaman itu hilang juga (`dest_ch.rs:879-900` diam-diam pad/truncate ke `ncols`).

**#7 — Postgres: `Relation` di tengah window me-relabel seluruh window.** `drain.rs:181-188` menimpa `wal_cols` tiap `Relation`, lalu `drain.rs:247` memotretnya **sekali di akhir window** untuk semua baris. DDL di tengah catch-up multi-jam → misalignment yang sama seperti #6.

**#8 — Nol timeout HTTP.** Sepuluh `reqwest::Client::new()` (gsheets, github ×2, clickhouse, gcs, s3 ×2, iceberg, bigquery ×2), **nol** `.timeout()`. Default reqwest = tanpa timeout request, read, maupun connect. Koneksi TCP yang stall — NAT mapping hilang, LB yang accept tapi tak pernah menjawab — menggantungkan transfer **selamanya**, tanpa error dan tanpa log. Karena binding Python memblokir thread pemanggil di `rt().block_on(...)`, ini jadi proses Python yang wedged, bukan job yang gagal. **Ini perbaikan ~15 baris dengan pengurangan risiko terbesar di seluruh repo.**

Kontras: jalur yang ditulis tangan justru punya deadline (`mywire.rs:353` connect 10s, `sink/mysql.rs:80` pool 30s). Jadi ini kelalaian, bukan filosofi.

**#9 — BigQuery: identifier tanpa escaping.** `format!("\`{}.{}.{table}\`")` di `bigquery.rs:432` dan `format!("\`{c}\`")` di `dest_bq.rs:425/539/924`. Berbeda dari semua dialek lain di repo ini yang punya helper benar — `dialect/postgres.rs:6` (doubling `"`), `dialect/mysql.rs:27` (doubling backtick). Aktor dengan hak `CREATE TABLE` di database **sumber** membuat kolom bernama `` x`,`y `` → MERGE yang di-generate keluar dari identifier dan mengeksekusi GoogleSQL arbitrer di warehouse tujuan, memakai kredensial service account apitap. Itu lintas trust boundary.

**#10 — `sql_mode=''` pada apply MySQL.** `dest_my.rs:201-205` mematikan strict mode. Untuk bulk replace itu bisa dibela; untuk CDC tidak, karena divergensi menumpuk dan tidak pernah diperbaiki. Kolom sumber dilebarkan `VARCHAR(64)→(255)`, tujuan masih 64 (tidak ada DDL sync). Nilai 200 karakter mendarat → **MySQL memotong jadi 64, warning tak terbaca, window commit, watermark maju.** Baris itu salah selamanya.

**#11 — Tidak ada cap byte per-value.** Semua buffer dibatasi oleh *ukuran chunk* atau *jumlah pesan*; tidak ada yang dibatasi ukuran satu nilai. Postgres mengirim **satu CopyData per baris**, jadi satu `bytea` 500 MB adalah satu pesan 500 MB. Cek ukuran di `walsender.rs:503` hanya menyala di **batas pesan**:

```rust
b'd' if buf.len() >= target => break,   // hanya dicek saat co_left == 0
```

Puncak ≈ 2× ukuran baris, per pipe. Satu dokumen 100 MB di tabel — hal yang benar-benar biasa — meng-OOM run di container 256 MB, padahal model memorinya bertahan di 170 MB untuk 100 GB. Yang bikin repot: **gagalnya tergantung data**, jadi tabel yang sama jalan lancar sampai ada yang mengunggah PDF besar.

**#12 — Transaksi streamed proto-v2 lolos dari budget.** `sess.streams` hidup lintas window (`drain.rs:57-61`) tapi counter byte-nya lokal per panggilan (`drain.rs:101`, `let mut buf_bytes = 0usize`). Byte yang dibebankan di window N dilupakan di window N+1 sementara memorinya tetap resident. Transaksi sumber 2 GB → beberapa ratus MB tertahan sambil window berikut buffer 24 MB lagi di atasnya.

**#13 — ClickHouse tanpa retry.** Saya hitung: `retry|attempt` muncul **0 kali** di `sink/clickhouse.rs` dan `logbased/dest_ch.rs`; **27 kali** di `sink/bigquery.rs`. `TOO_MANY_PARTS` (ClickHouse code 252) datang sebagai HTTP 500 dan jadi `Error::Transfer` terminal. Itu kegagalan transient paling kanonik di ClickHouse. Kodenya memang memitigasi *penyebabnya* (`min_insert_block_size_rows=1048576`) tapi tidak pernah me-retry *gejalanya*.

**#14 — Alokasi tak terbatas & slicing tanpa bounds check.** `walsender.rs:98` mengalokasikan `BytesMut::zeroed(len - 4)` dengan `len` dari wire, hanya dicek batas bawah — frame rusak yang mengklaim `0xFFFFFFFF` memaksa alokasi ter-zero ~4 GiB, instant OOM-kill di container 256 MB. `pgoutput.rs:481` `Vec::with_capacity(u32 dari wire)` → 16 GiB. `walsender.rs:803-813` (hot path CDC) `msg[1..9]` dan `msg.slice(25..)` tanpa cek panjang — padahal baris tepat di bawahnya pakai `msg.get(17).copied().unwrap_or(0)` yang benar. Penulisnya tahu; dua baris di atasnya terlewat.

---

## 5. Temuan MEDIUM — Ringkas

**#15 — Deteksi cgroup hanya root.** `pipeline/mod.rs:93-105` hanya membaca `/sys/fs/cgroup/memory.max` dan path v1-nya. Saya grep: **tidak ada** resolusi `/proc/self/cgroup` atau `mountinfo` di mana pun. Ini benar hanya kalau cgroup namespace dipakai (Docker default, k8s). Salah untuk `--cgroupns=host`, `systemd-run --property=MemoryMax=`, cgroup bersarang, banyak CI runner. Di kasus itu `mem_capped_parallel` mengembalikan `requested` apa adanya → di host 16-core dengan limit 256 MB di parent cgroup, 32 pipe × ~30 MB ≈ **1 GB proyeksi di kandang 256 MB.**

Yang menyelamatkan: `APITAP_MEM_BUDGET` ada sebagai override eksplisit dan didokumentasikan. Jadi ini bukan bug tanpa jalan keluar — tapi default-nya gagal senyap.

*Catatan penting:* saya **memeriksa dan menolak** hipotesis umum bahwa `num_cpus::get()` melaporkan core host di cgroup 0.5-CPU. Saya baca source `num_cpus 1.17.0` (pin lockfile): ia menyelesaikan lewat `/proc/self/cgroup` + `/proc/self/mountinfo`, menangani v1 dan v2, mengembalikan `min(ceil(quota/period), affinity)` = 1. **Sisi CPU sudah benar.** Hanya sisi memori yang punya lubang ini.

**#16 — Escaping literal watermark.** `pipeline/mod.rs:227` hanya menggandakan `'`. Itu cukup untuk Postgres (`standard_conforming_strings=on`), salah untuk MySQL dan ClickHouse yang menghormati `\` sebagai escape. Helper yang benar **sudah ada di repo** (`sink/mysql.rs:40`, `sink/clickhouse.rs:301`) tapi tidak dipakai di call site ini. Cabang non-quoted justru benar dan dikomentari dengan baik (memvalidasi `i128` sebelum embed) — jadi ini kelalaian di satu cabang, bukan pola.

**#17 — SSRF GitHub.** `github_api.rs:506` mem-parse header `Link: rel="next"` dan me-request URL-nya lagi dengan `Authorization: Bearer $TOKEN` dilampirkan (`:399`), tanpa allowlist host. Proxy TLS-inspecting korporat atau env `HTTPS_PROXY` cukup untuk mengambil PAT operator.

**#18 — Klaim atomicity di docs tidak berlaku universal.** `docs/failure-modes.md:21` menyatakan *"A CDC watermark advances only with its data, in the same transaction."* Itu benar untuk PG, MySQL, dan Iceberg. **Tidak benar** untuk ClickHouse (`dest_ch.rs:585-787` menjalankan TRUNCATE → DELETE → INSERT → state sebagai 5+ statement HTTP terpisah) dan BigQuery group apply (`dest_bq.rs:664-684` commit tiap chunk 256 KiB sebagai transaksi sendiri). Modul doc `dest_ch.rs:3-9` jujur soal ini; halaman failure-modes tingkat atas tidak. Kodenya baik-baik saja — yang salah adalah garansi yang didokumentasikan.

**#19 — Wheel yang dipublish bukan yang diuji.** `publish.yml:69-79` mengunggah `dist/*.whl`, wheel PGO yang di-build tangan oleh `benchmarks/pgo-build.sh`. Job `ci` yang menggerbanginya mem-build wheel **berbeda** dengan `maturin build --release`. Untuk crate dengan pointer arithmetic tak tercek, "kompilasi berbeda dari source yang sama" bukan formalitas.

**#20 — `Cargo.lock` basi.** Tercatat `apitap-core 0.20.0` dan `apitap-python 0.20.0`; manifest bilang `0.42.0`. `cargo build --locked` akan gagal, dan `cargo audit` mengaudit graph yang berbeda. (Catatan: `vendor/sqlx-core` **ada** dan berisi source lengkap — saya verifikasi di device. Klaim awal bahwa direktori itu hilang adalah artefak dari subset file yang saya salin, bukan masalah nyata.)

**#21 — Nol lint hardening, nol fuzzing.** Grep `^#!\[` di seluruh source: **0 hasil**. Tidak ada `#![deny(unsafe_op_in_unsafe_fn)]`, tidak ada `#![forbid(unsafe_code)]` di crate yang aman. Padahal ada ~34 `unsafe` (18 di core, 16 di `capsule.rs`), termasuk `arrowcol.rs:786-952` yang melakukan `data.as_ptr().add(offs[k]).cast::<[u8;8]>().read()` dengan offset yang di-stage dari COPY biner Postgres. Tidak ada fuzzing, tidak ada Miri, tidak ada sanitizer — atas decoder yang mem-parse **biner tak terpercaya** dan menyuapinya ke pointer arithmetic. `cargo-fuzz` di `walsender::read_message` dan `BatchBuilder::push` akan menemukan #14 dalam hitungan menit.

Juga tidak ada `cargo audit`/`cargo deny`/dependabot atas 30+ dependency langsung yang mengirim artefak terkompilasi ke PyPI. Tidak ada MSRV (`rust-version` tidak ada di manifest mana pun; CI mengambang di `stable`).

**#22 — Observability.** Tidak ada `tracing` maupun `log` di dependency mana pun; semua diagnostik adalah 33 `eprintln!` yang hampir semuanya di balik `APITAP_DEBUG`. Artinya debugging = mereproduksi kegagalan dengan env var diset ulang. Tidak ada span/correlation ID (di run `slots=4`, empat thread menulis ke satu stderr tanpa penanda). Tidak ada metrik yang bisa di-scrape: tidak ada lag, tidak ada ukuran slot, tidak ada counter error. Satu-satunya gauge operasional (peringatan retensi WAL) dicetak, bukan diekspor.

`progress.rs` sendiri **bagus** — auto-deteksi TTY/pipe/JSON, flush tiap baris, `rows_exact` hanya diset oleh lane yang benar-benar men-decode baris (komentarnya mendokumentasikan bug nyata yang diperbaiki). Kelemahannya: `transfer_schema()` dan `transfer_tables()` tidak memancarkan progress sama sekali — padahal itu panggilan terlama di library ini — dan tidak ada event `transfer.failed`.

**#23 — Cakupan test.** 197 test, tapi hanya **3,6% async** — sementara semua yang berisiko secara operasional adalah async. Distribusinya: 61 test di `wire/*` (bagian yang mudah dites), dan **nol** di:

| File | Baris | Apa itu |
|---|---|---|
| `logbased/run.rs` | 1.485 | **Seluruh orkestrator CDC** |
| `sink/mysql.rs` | 831 | Satu destination penuh |
| `lib.rs` | 515 | API publik |
| `py-apitap/src/lib.rs` | 388 | **Seluruh boundary PyO3** |
| `py-apitap/src/capsule.rs` | 377 | **FFI Arrow C Data Interface** (16 `unsafe`) |
| `logbased/drain.rs` | 351 | **State machine drain WAL** |
| `logbased/dest_pg.rs` / `dest_my.rs` | 819 | Apply CDC |

Tidak ada direktori `tests/` untuk integration test Rust. Satu-satunya gerbang E2E di CI adalah `benchmarks/ci_transfers.py` — nyata dan blocking, kredit untuk itu — tapi 6 dari 12 skema route (BigQuery, GCS, S3, Iceberg, gsheets, GitHub) hanya tercakup oleh run manual di bench box.

Juga: `_predicate_sql()` di `py-apitap/python/apitap/__init__.py:318-452` (~135 baris) **menghasilkan SQL** yang di-push ke server, mengonsumsi `predicate.meta.serialize(format="json")` — format internal polars yang docstring-nya sendiri sebut "not a stable format across polars versions" — dan **tidak punya test sama sekali**.

---

## 6. Yang Benar-Benar Bagus

Ini bukan basa-basi. Beberapa hal di codebase ini di atas rata-rata industri, dan penting untuk kalibrasi: masalah di atas adalah lubang di kerangka yang kuat, bukan gejala kerangka yang lemah.

**1. Snapshot→stream handoff Postgres persis benar.** Ini bug klasik yang dihindari sepenuhnya: slot dibuat **lebih dulu** dengan `EXPORT_SNAPSHOT` (`run.rs:941`), `consistent_point` dan `snapshot_name` diambil dari baris hasil yang sama, nama snapshot dibawa ke bulk loader, dan **setiap span COPY** membuka koneksi sendiri lalu menjalankan `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '<snap>'` (`source/postgres.rs:664-673`). Jadi semua range-pipe paralel melihat satu instan identik. Watermark ditulis sebagai `consistent_point`, streaming mulai persis dari situ. **Tidak ada celah, tidak ada overlap, tidak ada langkah manual operator.**

**2. Disiplin ack-after-commit benar, dan ini bagian tersulit.** `run.rs:1259-1264` memblokir di `applied_rx.wait_for` sebelum setiap `standby_status`, dan melewati konfirmasi final sepenuhnya kalau drain error. Balasan keepalive melaporkan LSN yang **applied**, bukan posisi drain — dengan komentar yang menjelaskan alasannya. Itu persis bug yang menjebak implementasi naif.

**3. Penanganan TOAST lebih baik daripada Debezium default.** `'u'` (unchanged TOAST) adalah bug CDC paling umum di dunia. Di sini ditangani di **enam lapisan**: decode menjaga `Null`/`Text`/`UnchangedToast` sebagai tiga varian terpisah dengan komentar bahwa menggabungkan salah satu pasangan = korupsi; collapse merutekan key ke residue terurut dan membuatnya *sticky*; apply Postgres pakai `UPDATE ... SET` bermasker kolom; ClickHouse membaca balik kolom yang hilang; BigQuery menyelesaikan mask di dalam MERGE dengan cabang `ERROR(...)`; dan setiap bulk renderer **error** alih-alih menulis NULL. Mode `changelog` bahkan menolak keras (*"the window is torn"*) daripada meng-NULL-kan sel — dengan test yang mengunci perilaku itu.

**4. Backpressure end-to-end nyata dan bisa ditelusuri.** Nol `unbounded_channel` di seluruh crate; semua 7 channel bounded, kebanyakan depth 2. Rantai empat hop dari ClickHouse yang stall sampai `COPY TO STDOUT` Postgres yang memblokir di kernel — saya telusuri dan benar. Ini yang paling sering salah di engine sejenis.

**5. Disiplin `JoinHandle` nyaris sempurna.** Setiap task yang di-spawn di-join atau di-abort; setiap `JoinError` jadi `Error`. Saya cari task yang mati diam-diam dan hanya menemukan satu thread detached yang cuma mencetak progress. Untuk codebase async sebesar ini, itu jarang.

**6. Tidak ada `std::sync::Mutex` yang dipegang melewati `.await`.** Saya audit semua site; dua yang paling dekat ditulis defensif dengan `mem::take` sebelum await. Ini kelas bug yang menghasilkan stall production misterius, dan tidak ada di sini.

**7. Guard resume MySQL menolak dua mode kegagalan senyap yang paling umum.** `myrun.rs:175` menolak watermark yang berada di depan posisi server (`RESET MASTER`, backup ter-restore, replika dibangun ulang), dan `binlog_file_present` menolak binlog yang sudah di-purge — masing-masing dengan pesan yang menyebut penyebab dan pemulihannya. Komentarnya mencatat bahwa jalur ini dulunya lubang senyap yang ditemukan release smoke, lalu ditutup. Saya sempat melaporkan kasus ini sebagai CRITICAL sampai run e2e-mu menunjukkan guard-nya lulus. Ini contoh yang tepat mengapa engineering yang ditopang harness beneran mengalahkan review statis.

**8. Precheck MySQL menolak setting yang merusak stream diam-diam** — `binlog_row_image != FULL`, `binlog_row_value_options`, `binlog_transaction_compression`, MariaDB `log_bin_compress` — masing-masing dengan alasan dan cara perbaikannya. Purge binlog dideteksi **sebelum** bertanya ke server, dengan pesan yang menyebut file, posisi, penyebab, dan satu-satunya pemulihan yang benar.

**9. Nol `todo!()`; semua `unimplemented!()` adalah mock di test.** Tidak ada fitur setengah jadi yang dikirim.

**10. Rekayasa performa yang nyata, bukan tebakan.** Buffer recycling dengan `mem::replace` (bukan `take`) dan alasannya ditulis; encoding NUMERIC di stack buffer `[i16; 32]` karena *"3 Vecs here was 60M allocs on a 10M-row × 2-col table"*; scan frame sinkron yang menggantikan await per-baris setelah diukur **23,7% dari 0.5-core**; `mallopt(M_MMAP_THRESHOLD)` yang **dimatikan di bawah 512 MB** karena retensi arena mengukur +20-30 MB puncak di tier itu. Memahami bahwa tuning yang sama adalah kemenangan di satu tier dan kerugian di tier lain, lalu mengkodekannya — itu level kehati-hatian yang jarang.

**11. Komentar dan dokumentasinya jujur, dan itu mengurangi risiko review secara material.** File CI menyatakan clippy bukan gerbang, **di dalam file CI itu sendiri**. `publish.yml` mendokumentasikan bahwa publish dulunya tidak digerbangi sama sekali. Profil release mencatat A/B fat-LTO yang **UNRESOLVABLE** dan mengembalikan ke setting konservatif alih-alih mengirim angka yang menyenangkan. `docs/failure-modes.md` punya bagian "What is NOT covered yet" yang menyebut tiga celah nyata tanpa diminta. `docs/stability.md` berkomitmen pada permukaan spesifik, mencabut lingkup yang tidak stabil, dan rekomendasinya sendiri adalah rekomendasi yang benar.

Beberapa komentar mendokumentasikan hal yang **dicoba lalu ditinggalkan** — LTO fat, fill_buf lease yang deadlock, sweep yang dibatalkan karena harness rusak, allocator swap yang mengukur lebih buruk. Itu jejak audit orang yang benar-benar melakukan performance engineering, bukan mempertunjukkannya.

---

## 7. Checklist Sebelum Production

Diurutkan berdasarkan rasio risiko/effort.

### Gerbang A — wajib sebelum deployment apa pun (perkiraan: 1-2 hari)

- [ ] **Timeout di semua HTTP client.** Satu builder bersama dengan `.timeout()` + `.connect_timeout()`. ~15 baris, pengurangan risiko terbesar di repo. *(#8)*
- [ ] **Cap panjang wire sebelum alokasi** di `walsender.rs:98`, `pgoutput.rs:481`, `mywire.rs:508/691`. Batas protokol Postgres sendiri 1 GB; satu perbandingan. *(#14)*
- [ ] **Bounds check** di `walsender.rs:803-813` dan `:374-386`. Polanya sudah benar di `pgoutput.rs:263-268`, tinggal disalin. *(#14)*
- [ ] **Escaping identifier BigQuery** — helper `bq_ident()` yang menggandakan backtick + menolak control char; rutekan `fq` dan tiga closure `bt`. *(#9)*

### Gerbang B — wajib sebelum CDC MySQL dipakai sama sekali (perkiraan: 3-5 hari)

- [ ] **Tolak `ENUM`/`SET`/`JSON`/`BIT`/`TIME` di precheck MySQL CDC** — hari ini juga, sebagai stop-gap. *(#1)*
- [ ] Resolusi label `ENUM`/`SET` dari `COLUMN_TYPE` (sudah dibaca `fetch_schema`, tinggal tidak dibuang). *(#1)*
- [ ] Renderer binary-JSON → teks. *(#1)*
- [ ] Perbaiki tanda `TIME2`; samakan `BIT` dengan jalur bootstrap. *(#1)*
- [ ] **Cek arity** `map.cols.len() == schema.names.len()`, tolak kalau beda. *(#6)*
- [ ] Simpan `@@server_uuid` bersama watermark; tolak kalau tidak cocok — melengkapi guard `AHEAD of the server` yang sudah ada agar tidak lagi bergantung pada urutan posisi. Idealnya pindah ke GTID. *(#5)*
- [ ] Bedakan "prasyarat test tidak tercipta" dari "test gagal" di `e2e_cdc_retention.py`, dan ganti `RESET MASTER` dengan `PURGE BINARY LOGS TO` untuk leg vanished-binlog. *(#5, catatan)*
- [ ] `sql_mode='STRICT_ALL_TABLES'` di sesi apply CDC; buang `unique_checks=0`. *(#10)*

### Gerbang C — wajib sebelum CDC Postgres unattended (perkiraan: 2-3 hari)

- [ ] **Perbaiki idle-WAL pin** — confirm `wal_end` saat caught-up, atau implementasikan tabel heartbeat yang sudah ada di desain. *(#4)*
- [ ] Pindahkan `buf_bytes` ke `DrainSession` agar transaksi streamed ikut terhitung budget. *(#12)*
- [ ] Retry ClickHouse dengan backoff untuk `TOO_MANY_PARTS` / 5xx. *(#13)*
- [ ] Validasi `REPLICA IDENTITY` di prepare, bukan saat `Relation` pertama tiba. *(#19 CDC)*

### Gerbang D — wajib sebelum melewati jaringan tak terpercaya (perkiraan: 3-5 hari)

- [ ] **Verifikasi sertifikat jadi default** di `mywire.rs`; `NoVerify` hanya lewat opt-in bernama keras. *(#2)*
- [ ] Jangan pernah kirim password cleartext ke peer tak terverifikasi. *(#2, #3)*
- [ ] Terminasi TLS di `walsender.rs`, **atau** tolak `sslmode=prefer` sekeras `require`. *(#3)*
- [ ] Allowlist host di `github_api::link_next` sebelum melampirkan token lagi. *(#17)*
- [ ] Escaping literal watermark per-dialek (helper sudah ada). *(#16)*

### Gerbang E — kualitas rilis (perkiraan: 3-5 hari, bisa paralel)

- [ ] `#![deny(unsafe_op_in_unsafe_fn)]` di kedua crate — satu baris, langsung menandai `capsule.rs:306-353`.
- [ ] Target `cargo-fuzz` di `walsender::read_message` dan `BatchBuilder::push`; jalankan Miri di test `arrowcol`/`capsule`.
- [ ] `cargo audit` + `cargo deny` sebagai gerbang CI; regenerasi `Cargo.lock`; tambahkan `--locked`.
- [ ] Deklarasikan MSRV (`rust-version`) dan uji di CI.
- [ ] Bangun wheel PGO **di CI**, atau cabut klaim byte-for-byte di README. *(#19)*
- [ ] Ganti `eprintln!` dengan `tracing`; ekspor counter minimal (rows, bytes, lag, ukuran slot, error).
- [ ] Resolusi cgroup bersarang di `mem_limit_bytes` (~40 baris, tiru `num_cpus`). *(#15)*
- [ ] Perbaiki `docs/failure-modes.md:21` dengan baris per-engine. *(#18)*
- [ ] Test untuk `logbased/run.rs`, `drain.rs`, `sink/mysql.rs`, `py-apitap/src/*`, dan `_predicate_sql`.
- [ ] Handler SIGTERM yang menyelesaikan window berjalan lalu keluar 0.

---

## 8. Catatan Metode & Batasan

**Tidak ada yang di-build atau dijalankan.** Review ini murni statis. Sesuai instruksimu, build dan test harus di VPS-mu — tapi saya tidak menemukan catatan akses VPS: satu-satunya folder yang ter-mount adalah `apitap-lib` (tidak ada `CLAUDE.md` di dalamnya), dan `device_bash` di mesinmu tidak punya akses jaringan. Kalau mau saya verifikasi temuan ini dengan eksekusi nyata (fuzz decoder, ukur puncak RSS pada baris lebar, reproduksi idle-WAL pin), kasih tahu cara menjangkau VPS-nya — atau tempel catatannya di sini.

**Klaim yang saya periksa lalu tolak** (supaya kamu tidak mengejar hantu):

- *`num_cpus::get()` melaporkan core host di cgroup 0.5-CPU* → **salah.** Saya baca source `num_cpus 1.17.0`: sadar cgroup v1 dan v2, mengembalikan `min(ceil(quota/period), affinity)`. Sisi CPU benar.
- *`vendor/sqlx-core` dan `benchmarks/` hilang, workspace tidak bisa di-build* → **salah**, artefak dari subset file yang saya salin. Keduanya ada di device (`benchmarks/` berisi 99 file).
- *`base64 = "0.23.0"` versi yang tidak ada* → **salah**, versi itu nyata dan tidak di-yank.
- *Kredensial bocor ke log/error* → **tidak ada.** Saya periksa 20 site `eprintln!`/`println!` dan semua varian error: hanya timing, row count, nama tabel. Tidak ada `#[derive(Debug)]` pada satu pun struct pembawa rahasia. `source_identity()` bahkan sengaja merekonstruksi identitas tanpa kredensial, dengan test khusus.
- *Quoting identifier Postgres/MySQL bisa di-bypass* → **tidak.** Semua 73 referensi lewat helper yang benar.
- *Decompression bomb dari respons gzip* → **tidak ada.** `flate2` hanya dipakai sebagai encoder; `reqwest` di-build tanpa fitur `gzip`/`brotli`/`deflate`.

**Catatan working tree:** `git status` menunjukkan `myrun.rs`, `publish.yml`, dan `e2e_cdc_retention.py` termodifikasi, plus `ci.yml`/`wheels.yml` belum di-track. Review ini membaca isi working tree, bukan HEAD.

---

## 9. Penutup

Kalau saya harus meringkas dalam satu kalimat: **arsitekturnya siap production, implementasinya belum di tiga tempat spesifik** — transport (TLS), tipe MySQL CDC, dan operabilitas (timeout, retry, observability).

Jarak dari posisi sekarang ke "aman dipasang untuk backfill dan migrasi yang bisa di-rerun" kira-kira **Gerbang A saja — 1-2 hari.** Jarak ke "aman untuk CDC Postgres unattended" adalah A + C + D, sekitar **satu setengah minggu.** Jarak ke "CDC MySQL boleh dipercaya" adalah semuanya, sekitar **dua sampai tiga minggu.**

Itu bukan rewrite. Dan untuk lima dari temuan terbesar — #1 (label ENUM sudah dibaca lalu dibuang), #4 (heartbeat sudah ada di dokumen desain), #11 dan #14 (pola bounds-check yang benar sudah ada di `pgoutput.rs`), #16 (escaper per-dialek sudah ada di repo) — **perbaikannya sudah setengah ditulis di codebase ini sendiri.** Itu hal paling menggembirakan yang bisa saya katakan tentang review sepanjang ini.
