#!/usr/bin/env python3
"""Per-flow packet/byte census of a pcap captured on Linux (lo or any iface).

    pcap_flows.py FILE.pcap [dport]

Prints one line per flow (sorted by TCP payload bytes) plus a TOTAL line:
    flow src:port -> dst:port pkts=<n> ip_bytes=<n> payload_bytes=<n> \
         empty_pkts=<n> avg_payload=<n> first=<ts> last=<ts>

`empty_pkts` are TCP segments with zero payload (ACKs, keepalives). Link types
handled: 1 (Ethernet, what tcpdump writes for lo) and 113 (Linux cooked/SLL).
"""
import collections
import struct
import sys


def frames(path):
    with open(path, "rb") as f:
        magic = f.read(4)
        if magic in (b"\xd4\xc3\xb2\xa1", b"\x4d\x3c\xb2\xa1"):
            endian = "<"
        elif magic in (b"\xa1\xb2\xc3\xd4", b"\xa1\xb2\x3c\x4d"):
            endian = ">"
        else:
            raise SystemExit(f"not a pcap: {magic!r}")
        nano = magic in (b"\x4d\x3c\xb2\xa1", b"\xa1\xb2\x3c\x4d")
        _, _, _, _, _, link = struct.unpack(endian + "HHiIII", f.read(24))
        while True:
            hdr = f.read(16)
            if len(hdr) < 16:
                return
            ts, tu, cap, _ = struct.unpack(endian + "IIII", hdr)
            data = f.read(cap)
            yield link, ts + tu / (1e9 if nano else 1e6), data


def ip4(data, link):
    if link == 1:
        if len(data) < 14 or data[12:14] != b"\x08\x00":
            return None
        off = 14
    elif link == 113:
        if len(data) < 16 or data[14:16] != b"\x08\x00":
            return None
        off = 16
    else:
        raise SystemExit(f"unsupported linktype {link}")
    if len(data) < off + 20:
        return None
    vihl = data[off]
    if vihl >> 4 != 4:
        return None
    ihl = (vihl & 0xF) * 4
    total = struct.unpack(">H", data[off + 2:off + 4])[0]
    proto = data[off + 9]
    src = ".".join(str(b) for b in data[off + 12:off + 16])
    dst = ".".join(str(b) for b in data[off + 16:off + 20])
    return off, ihl, total, proto, src, dst


def main():
    path = sys.argv[1]
    dport = int(sys.argv[2]) if len(sys.argv) > 2 else None
    flows = collections.defaultdict(
        lambda: {"p": 0, "ip": 0, "pl": 0, "empty": 0, "t0": None, "t1": None})
    for link, ts, data in frames(path):
        r = ip4(data, link)
        if not r:
            continue
        off, ihl, total, proto, src, dst = r
        if proto != 6:
            continue
        th = off + ihl
        if len(data) < th + 20:
            continue
        sport, dport_ = struct.unpack(">HH", data[th:th + 4])
        if dport is not None and dport_ != dport:
            continue
        doff = (data[th + 12] >> 4) * 4
        payload = max(0, total - ihl - doff)
        key = (src, sport, dst, dport_)
        fl = flows[key]
        fl["p"] += 1
        fl["ip"] += total
        fl["pl"] += payload
        if payload == 0:
            fl["empty"] += 1
        fl["t0"] = ts if fl["t0"] is None else min(fl["t0"], ts)
        fl["t1"] = ts if fl["t1"] is None else max(fl["t1"], ts)
    tot = {"p": 0, "ip": 0, "pl": 0, "empty": 0}
    for key, fl in sorted(flows.items(), key=lambda kv: -kv[1]["pl"]):
        src, sport, dst, dport_ = key
        avg = fl["pl"] / max(1, fl["p"] - fl["empty"])
        print(f"flow {src}:{sport} -> {dst}:{dport_} pkts={fl['p']} "
              f"ip_bytes={fl['ip']} payload_bytes={fl['pl']} "
              f"empty_pkts={fl['empty']} avg_payload={avg:.1f} "
              f"first={fl['t0']:.6f} last={fl['t1']:.6f}")
        for k in tot:
            tot[k] += fl[k]
    print(f"TOTAL pkts={tot['p']} ip_bytes={tot['ip']} payload_bytes={tot['pl']} "
          f"empty_pkts={tot['empty']}")


if __name__ == "__main__":
    main()
