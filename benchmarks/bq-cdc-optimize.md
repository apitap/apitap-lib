# BigQuery CDC + full-load optimization (2026-08-13, v0.33.0)

Profile-then-knife on `log_based` → BigQuery, all at **0.5 cpu / 256 MB**, heavy
15-column rows (`~576 B/row`: bigint PK, varchar/text, smallint/int/bigint,
float, numeric, bool, date, timestamp, timestamptz, jsonb). Harness on the VPS:
`zbqprof*.sh` + `prof_drain.py` + `group_e2e.py`.

## The profile

BigQuery CDC is **latency-bound, not CPU-bound** — the drain's CPU is 2–10% of
the half-core quota; the wall is BigQuery job round-trips. Per window:

    [bq apply] 25000 staging rows / 13MB · load ≈ 3s · merge ≈ 8–11s

The MERGE time is ~constant regardless of staging size → it is the target scan +
BigQuery's fixed job floor, NOT our data. Two consequences drove the knives:

1. **Fewer, bigger windows win** — each window is one load + MERGE round-trip, so
   halving the window count nearly halves the wall.
2. **A 10-table group applied SERIALLY** paid 10× the round-trip per window.

Memory ceiling: at 256 MB the usable window tops out ~33 MB (peak 204 MB); 64 MB
OOMs. The dominant term is two overlapped collapsed windows.

## The knives (each measured, all correctness-verified on real BigQuery)

| knife | what |
|---|---|
| **Bigger BQ window** | BigQuery uses the full per-window budget (no /2, no 24 MiB clamp the CPU-bound paths need). Runtime lever `APITAP_CDC_WINDOW_BYTES`. |
| **Concurrent group apply** | a group's tables apply CONCURRENTLY (distinct targets, no contention) instead of serially — BigQuery only; the CPU-bound paths stay serial. |
| **Parallel bootstrap** | a group's per-table full loads fan out (bounded 4-way) — BigQuery's bootstrap is job-bound, so serial paid 10× the latency. |
| **Clustering** | the target is rewritten `CLUSTER BY` its PK at bootstrap so a large table's MERGE prunes. Measured NEUTRAL below ~1M rows (the MERGE floor dominates a small scan), so small tables skip it and its rewrite cost. Lever `APITAP_BQ_CLUSTER=0`. |
| **Retry** | `cdc_script` retries concurrent-update / rate / 5xx with backoff — the concurrent group's transactions race on the shared `_apitap_state`. |

## Results (0.5 cpu / 256 MB, heavy 15-col rows)

| workload | before | after | |
|---|---|---|---|
| CDC, 1 table (200K changes) | 149.2 s · 80K/min | **78.6 s · 152K/min** | **1.9×** |
| CDC, 10-table group (500K changes) | 1095 s · 27K/min | **312 s · 96K/min** | **3.5×** |
| full-load, 10-table group (500K rows) | 225 s | **62.6 s** | **3.6×** |

Correctness: the single-table op-mix e2e (`e2e_bq_cdc.py`) and the 10-table
group e2e (`group_e2e.py`, 10/10 tables digest-matched to Postgres) both ALL
GREEN on the optimized wheel.

## The honest ceiling

35K changes/s (= 2.1M/min) is **not reachable at 0.5 cpu / 256 MB** via the
staging-load + MERGE architecture. The wall is BigQuery's ~10 s MERGE job floor ×
the window count, and the window count is bounded by the 256 MB memory cap —
Google's latency, not our CPU (which stays 6–10% of the quota). Getting past it
needs one of: a bigger memory tier (much bigger windows → far fewer MERGEs), the
BigQuery Storage Write API (gRPC streaming, skipping load jobs and the per-window
MERGE floor), or batching a group's `_apitap_state` writes to remove the
concurrent-update retries. Recorded here so the number is honest.
