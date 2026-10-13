# Production validation: 120k changes/s at 0.5 core

The bench VPS is per-core ~2.5x slower than production hardware and caps at
~48-49k/s at half a core (design §14.23/§14.27). The 120k@0.5-core mandate
is therefore only decidable on a production-class box. This kit produces
that number, end to end.

## Recipe

1. **Seed a source table** (any Postgres with `wal_level=logical`; ~10M rows
   of a normal shape is plenty):

   ```bash
   pgbench -i -s 200 mydb          # pgbench_accounts: 8 cols, int PK
   ```

2. **Catch up once** (bootstrap; also creates the destination table):

   ```bash
   APITAP_FOLLOW_SECS=0 apitap ... # or just: python3 validate_120k.py --no-cap
   ```

   The simplest is to run the validator once uncapped; the first in-run
   bootstrap covers this.

3. **Generate >130k changes/s** in a second shell (tune `-c`/`-j` until
   `tps` in the pgbench output clears 130,000 — on a large box `-c64 -j16`
   is a good start):

   ```bash
   pgbench -c 64 -j 16 -T 600 mydb
   ```

   `pgbench_accounts` updates are point UPDATEs — the exact shape apitap's
   collapse path optimises for.

4. **Run the validator** (cages itself to 0.5 core via `systemd-run`):

   ```bash
   python3 validate_120k.py \
       "postgres://user:pass@host:5432/mydb" \
       "clickhouse://default:pass@host:8123/default" \
       public.pgbench_accounts 300
   ```

   It prints `changes=... rate=.../s cpu/change=...us target<=4.17us ->
   PASS|FAIL` and exits non-zero on FAIL.

## Saturation requirement

The metric is only meaningful when the source OFFERS more than the target:
below ~130k changes/s the engine is wait-bound, the fixed park/wake cost per
window does not amortise over enough changes, and cpu/change inflates by
construction (a bench smoke at ~1.8k offered changes/s measured 49 µs/change
while sitting at 0.09 of the 0.5-core quota). Raise pgbench `-c/-j`, or
accept that the run is measuring the source.

## Reading the number

- **`cpu/change ≤ 4.17 µs` = PASS.** That is 120k/s at exactly half a core.
  The bench VPS measured ~7.0 µs (48k), GCP ~3 µs (167k equivalent).
- `rate` shows what the half-core cage actually sustained while the source
  offered ≥130k/s; if `rate` sits well above 120k, the source is the limit,
  not the engine — raise pgbench.
- Watch **`cap_frac` behaviour**: if the process pegs the quota, the engine
  is CPU-bound and `cpu/change` is the honest figure. If it idles below the
  quota, the source/destination latency, not CPU, is the limit.
- Known knobs that matter (all default-on in 0.61): coalescer 500 µs,
  `APITAP_PG_BINARY` auto-on for ClickHouse, insert-only detection on
  Replacing-family destinations. `APITAP_SLOTS=auto` scales the whole drain
  set to the quota; on a 0.5-core cage it stays at 1.
- TLS adds a few percent and is the production reality; measure with the
  real URLs, not loopback plaintext, if the source is remote.

## Note on `--no-cap`

`--no-cap` exists for smoke tests only: it prints the numbers but can never
report PASS, because an uncapped run cannot answer the 0.5-core question.
