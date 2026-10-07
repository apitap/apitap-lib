# The WAL drain's TCP cost, measured (0.58.0)

B2.7 said "~53 % of the per-change cost is kernel TCP receive for a
one-packet-per-change WAL stream" and left a Unix-socket lane as the implied
next lever. This is the measurement that claim asked for, done on the released
0.58.0 wheel. The short version: the transport is NOT the lever — the stream
is already coalesced to ~0.6 packets per change, its syscalls cost ~12 % of a
core, and the TCP/nftables stack a Unix socket would remove is a single-digit
share. The report that follows has the numbers.

## Rig

- bench PostgreSQL `apitap-bench-pg-src` (:5544, logical replication), seed
  `public.prof_pg_m` (1,000,000 rows, the ingestr 15-column shape), destination
  ClickHouse :8124 `default`, drain = apitap **0.58.0 from PyPI**
  (`_apitap.abi3.so` md5 `00ecb13647c6ce791b6ec5be38c9a030`).
- The drain ran on the host (uncapped) so `strace`/`tcpdump` could attach to it;
  the writer (`benchmarks/cdc-steady-profile/writer.py`) paced 4 threads at
  **39,468 changes/s**, witnessed by `WRITER_TOTAL`/`WRITER_WAL_BYTES`:
  **469.9 WAL bytes per change**.
- One aligned 20.34 s window: `tcpdump -i lo -s 96` filtered to the drain's OWN
  connections to :5544 (ports found via `ss -tnp` by pid) and
  `strace -c -f -p <pid>` started 0.3 s later. `tcpdump` reports **0 packets
  dropped by kernel**; `sha` no.

## Numbers in the window

| quantity | value | per second | per change |
|---|---|---|---|
| server→client packets | 131,484 | 6,463/s | **0.61** |
| server→client wire bytes | 109,149,275 | 5.37 MB/s | ~204 B |
| TCP payload (wire − 54 B/pkt) | ~102.0 MB | 5.01 MB/s | ~190 B |
| payload per packet | 776 B | | |
| changes per packet | ~1.65 (WAL 469.9 B/change) | | |
| `recvfrom` | 72,029 (24 µs avg) | 3,541/s | **0.33** |
| `epoll_wait` | 78,108 (8.9 µs avg) | 3,840/s | **0.36** |
| `clock_nanosleep` | 39 × 200 ms | | the progress reporter's refresh thread, not the drain |

Derived: **recvfrom + epoll_wait ≈ 12 % of one core** at this rate
(1.76 s + 0.70 s over 20.34 s), and TCP already coalesces — 0.55 `recvfrom`
per packet, i.e. ~1.8 packets per read, so larger receive buffers have little
headroom left.

## What a Unix socket would remove

The B1 profile's kernel bucket (53 % of on-CPU samples) contains, by symbol:
`nft_do_chain` 2.65 %, `tcp_recvmsg_locked` 1.24 %, `__skb_datagram_iter`
0.80 %, `refill_stock` 1.12 %, `mod_memcg_state` 0.75 %, `__tcp_transmit_skb`
0.58 %, `tcp_clean_rtx_queue` 0.61 % — the loopback TCP + netfilter path is a
**~7-8 %** slice of the profile, and a Unix socket removes roughly that slice
(no TCP, no netfilter) while KEEPING the syscall (`recvfrom`/`epoll_wait`,
~12 %) and scheduler costs, which a transport swap does not touch. Expect
**≈ +5 % throughput on same-host deployments, not a multiplier**; on
cross-host deployments there is no Unix socket at all.

The correction to B2.7: the 53 % kernel figure is real as a profile bucket,
but it is mostly syscalls, scheduler and memory accounting — not the TCP
stack — and it is not removed by an alternative transport. The claim should
read: "~53 % of the drain's on-CPU time is kernel, of which the removable
transport slice (loopback TCP + nftables) is under 10 %."

## Verdict for the 2M/min chase

- Transport levers are exhausted at the measurement level: coalescing already
  happens, buffers have little to give, and Unix sockets buy single digits.
- The remaining per-change cost is userspace: allocator traffic (~18 % of
  samples in the B1 profile, 44 % on the MySQL lane), decode (6 % pg / 19 %
  my), and rendering. Those are the levers that could move a 30-table group
  past ~1.9M changes/min at 0.5 core; they are a separate campaign with its
  own RED/gate discipline, not a transport swap.
- No engine change accompanies this report: the measurement says none is
  justified yet.

## Raw artifacts

- `tcpdump` pcap and `tcpdump.log`, `strace.out`/`strace.err`, writer and drain
  logs on the bench VPS in `/tmp/tcpmeasure3/` (window summary in
  `/tmp/tcpmeasure3.out`), plus the syscall thread trace in `/tmp/nano/`.
- The earlier (idle-regime, wrong-filter) runs are in `/tmp/tcpmeasure*` and are
  superseded by the numbers above.

## Second, independent dataset (container-capped)

A parallel run used the same seed with the drain inside
`--cpus=0.5 --memory=256m` and a writer at 25,905 changes/s with a different
mix (**970.8 WAL bytes/change**): it applied 7,775,000 changes in 240 s =
**32.4k changes/s at cap_frac 0.87** with **MEMPEAK 42.3 MB** — the same
operating point the B2 report describes from the other side. Its raw artifacts
are the `benchmarks/wal-tcp-cost/` harness and `~/waltcp/logs/` on the bench
VPS (`main.drain.log`, `main.out`, `main.perf.data/symbols.txt`,
`main.strace.txt`). Two observations from them:

- the syscall mix repeats: `recvfrom` 115,293 / `epoll_wait` 125,624 over its
  trace window (a near-1:1 ratio like the aligned window's 72k/78k), so the
  coalescing result is not an artifact of one writer mix;
- its `perf` call graph shows `epoll_wait → do_epoll_wait → ep_poll →
  schedule_hrtimeout_range → schedule → finish_task_switch` (4.25 % of
  samples): the kernel bucket is dominated by **timed polling and scheduler
  work**, exactly the part a different transport does not remove.

Both datasets agree: the transport is not the lever; the remaining cost is
userspace per-change work plus timer/scheduler overhead in the poll loop.
