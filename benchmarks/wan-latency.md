# Latency: what RTT costs a transfer (2026-08-18)

Every apitap benchmark before this one ran on loopback, which the ledgers admit
in passing. An outside review turned that admission into a prediction worth
testing: since apitap uses parallel range pipes, its advantage over a serial
pipeline *should widen* as latency rises.

It does not. It narrows. Here are the numbers.

## Rig

The client runs in its own container with `NET_ADMIN` and `tc qdisc … netem
delay` on its egress, so the delay lands on apitap's traffic and nothing else on
the box — the production containers sharing this machine are untouched. netem
delays outgoing packets only, so the figure below is the added **RTT**.

Source `postgres:16` and destination `clickhouse-server:24.8` are the ordinary
bench containers, reached on their container IPs (their published ports are
bound to the host's loopback, which another container cannot use). Table:
`bench_data_10m_cap`, 10,000,000 rows, ~3.8 GB on the wire. Mode `replace`,
apitap 0.41.0 from the release wheel. One pass per cell.

## The sweep

| added RTT | `parallel=1` | `parallel=8` | parallel's lead |
|---|---|---|---|
| 0 ms | 103,443 rows/s (96.7 s) | **609,057 rows/s (16.4 s)** | 5.9× |
| 25 ms | 119,034 rows/s (84.0 s) | 478,017 rows/s (20.9 s) | 4.0× |
| 50 ms | 99,474 rows/s (100.5 s) | **320,574 rows/s (31.2 s)** | 3.2× |

## What it means

**Latency does not break the transfer.** At 50 ms of added RTT — a
cross-continent link — 10 million rows still land in 31 seconds. Nothing here
falls off a cliff.

**The prediction was wrong, and in the more interesting direction.** The single
pipe is flat across the sweep while the eight-pipe run loses 47%. The reason is
that apitap's lanes are already *streaming*: a `COPY … TO STDOUT` is not a
request per row, and neither is a ClickHouse insert body, so a single pipe pays
RTT at handshake and then stops paying it. There was no round-trip stall for
parallelism to hide. What parallelism does add at distance is eight connections'
worth of setup and eight sets of HTTP responses to wait on, so the parallel path
is the one latency can charge.

**The lead survives anyway.** 3.2× at 50 ms is smaller than 5.9× on loopback and
still the difference between half a minute and a minute and a half.

## Disclosures

One pass per cell, n=1. The `parallel=1` row moves between 99K and 119K rows/s
with no monotonic trend, which is ±15% of noise and larger than any latency
effect on that line — read it as "flat within noise", not as "faster at 25 ms".
The `parallel=8` line falls monotonically and by far more than that noise band,
which is why it carries a conclusion and the other line does not.

netem models delay, not the loss, reordering or bandwidth ceiling of a real WAN;
a link with packet loss would treat the two shapes differently again. Harness:
`~/wan2.sh` on the bench box, reproduced in this repository's history.
