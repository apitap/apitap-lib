#!/usr/bin/env python3
"""Classify a `perf report --stdio` symbol dump into the CDC cost buckets."""
import re
import sys
import bisect

symfile, dynsyms, label = sys.argv[1], sys.argv[2], (sys.argv[3] if len(sys.argv) > 3 else "run")
rows = []
pat = re.compile(r"^\s*([\d.]+)%\s+\S+\s+(\S+)\s+\[.\] (.*)$")
for line in open(symfile):
    m = pat.match(line)
    if not m:
        continue
    pct = float(m.group(1))
    dso = m.group(2)
    sym = m.group(3).strip()
    rows.append((pct, dso, sym))

syms = []
for line in open(dynsyms):
    parts = line.split()
    if len(parts) >= 2:
        try:
            syms.append((int(parts[0], 16), parts[1]))
        except ValueError:
            pass
syms.sort()
addrs = [a for a, _ in syms]

def libc_name(off_hex):
    off = int(off_hex, 16)
    i = bisect.bisect_right(addrs, off) - 1
    return syms[i][1] if i >= 0 else "?"

def bucket(dso, sym):
    s = sym
    if "kernel.kallsyms" in dso:
        return "kernel"
    if "libc.so" in dso:
        return "libc"
    if "_apitap" in dso:
        if any(k in s for k in ("pgoutput::", "walsender::", "mybinlog::", "mywire::", "pgbindec::")):
            return "decode"
        if any(k in s for k in ("collapse::", "Collapse", "hashbrown", "key_of_row", "std::collections::hash", "hash_one")):
            return "collapse"
        if any(k in s for k in ("logbased::drain", "logbased::window", "logbased::replay", "changelog::", "tracked")):
            return "drain/window"
        if any(k in s for k in ("rowtext", "render", "tsv", "ch_str", "ch_ident")):
            return "render"
        if any(k in s for k in ("dest_ch", "dest_pg", "dest_my", "sink::clickhouse", "sink::postgres",
                                "sink::mysql", "reqwest", "hyper", "h2::", "http::", "sqlx",
                                "tokio::net", "rustls", "tls")):
            return "apply/io"
        if any(k in s for k in ("lease::", "guard::", "Tenure", "Keeper", "Fence", "lease")):
            return "lease/state"
        if any(k in s for k in ("bytes", "tokio::sync", "mpsc", "Semaphore")):
            return "buffers/channels"
        if any(k in s for k in ("tokio::runtime", "tokio::time", "Pin<P>", "future", "block_on", "poll")):
            return "runtime/sched"
        return "other-abi3"
    if "vdso" in dso:
        return "vdso"
    return "other"

tot = sum(p for p, _, _ in rows)
agg = {}
detail = {}
for pct, dso, sym in rows:
    b = bucket(dso, sym)
    agg[b] = agg.get(b, 0.0) + pct
    if b == "libc" and sym.startswith("0x"):
        key = "libc:" + libc_name(sym)
    else:
        key = f"{b}:{sym}" if b != "kernel" else "kernel:" + sym
    detail[key] = detail.get(key, 0.0) + pct

print(f"== {label}: total classified {tot:.2f}% ==")
for b, v in sorted(agg.items(), key=lambda kv: -kv[1]):
    print(f"{v:7.2f}%  {b}")
print()
print(f"== {label}: top 40 detail ==")
for k, v in sorted(detail.items(), key=lambda kv: -kv[1])[:40]:
    print(f"{v:7.2f}%  {k}")
