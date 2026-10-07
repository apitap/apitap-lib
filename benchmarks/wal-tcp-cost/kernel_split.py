#!/usr/bin/env python3
"""Split the kernel bucket of a `perf report --stdio` dump into net/sched/mem.

    kernel_split.py symbols.txt [label]

Input lines look like `  4.25%  python3  [kernel.kallsyms]  [k] symbol`. The
classifier this complements (classify.py) lumps every kernel symbol into one
"kernel" bucket; the wal-tcp-cost mission needs the TCP receive part separated
from scheduler/allocator/netfilter noise.
"""
import re
import sys

pat = re.compile(r"^\s*([\d.]+)%\s+\S+\s+(\S+)\s+\[.\] (.*)$")

CATS = [
    ("net-rx", ("tcp_recvmsg", "tcp_v4_rcv", "ip_rcv", "ip_local_deliver",
                "__netif_receive_skb", "netif_receive_skb", "napi_",
                "net_rx_action", "__skb_datagram_iter", "skb_copy_datagram",
                "tcp_data_queue", "tcp_ack", "tcp_rcv_established",
                "tcp_filter", "sk_filter", "sock_def_readable", "skb_release",
                "kfree_skb", "consume_skb", "build_skb", "napi_gro",
                "tcp_queue_rcv", "tcp_clean_rtx_queue", "loopback_xmit",
                "netif_skb_features", "validate_xmit_skb", "skb_clone")),
    ("net-tx", ("__tcp_transmit_skb", "tcp_sendmsg", "tcp_write_xmit",
                "tcp_push", "dev_queue_xmit", "sch_direct_xmit",
                "ip_finish_output", "ip_output", "tcp_transmit_skb",
                "sk_stream_alloc_skb", "__ip_finish_output", "dst_output",
                "tcp_wmem", "tcp_send_mss", "tcp_current_mss")),
    ("netfilter", ("nft_", "nf_hook", "nf_conntrack", "iptable", "ip_tables",
                   "br_netfilter", "xt_", "nf_nat", "nf_conn")),
    ("net-other", ("tcp_", "udp_", "sock_", "sk_", "net_", "inet_", "inet6_",
                   "ip_", "neigh_", "arp_", "fib_", "route", "rt_",
                   "tcp_gro", "tcp_mark", "tcp_orphan", "inet_csk",
                   "skb_", "dev_", "eth_", "veth_", "tun_", "unix_")),
    ("sched", ("finish_task_switch", "__schedule", "pick_next_task",
               "scheduler_tick", "ttwu", "try_to_wake_up", "wake_up",
               "pvclock", "kvm_clock", "clock_", "hrtimer", "timer_",
               "ktime", "syscall_", "do_syscall_64", "do_syscall",
               "entry_SYSCALL", "rcu_", "irq_", "softirq", "tasklet",
               "update_rq", "resched", "nohz", "idle_")),
    ("mem", ("kmem_cache_", "refill_stock", "mod_memcg", "memcg", "slab",
             "obj_cgroup", "rep_movs", "memcpy", "copy_", "check_heap",
             "free_unref", "get_page", "put_page", "__alloc_pages",
             "kfree", "kmalloc", "vmap", "unmap", "mm_", "page_")),
    ("locks", ("_raw_spin_", "_raw_write_", "_raw_read_", "mutex",
               "rwsem", "qspinlock", "osq_", "spin_", "preempt_",
               "rcuwait", "refcount")),
]


def cat_of(sym):
    for name, keys in CATS:
        if any(k in sym for k in keys):
            return name
    return "kernel-other"


def main():
    path = sys.argv[1]
    label = sys.argv[2] if len(sys.argv) > 2 else "run"
    rows = []
    for line in open(path):
        m = pat.match(line)
        if not m:
            continue
        pct, dso, sym = float(m.group(1)), m.group(2), m.group(3).strip()
        if "kernel.kallsyms" not in dso:
            continue
        rows.append((pct, sym))
    agg = {}
    for pct, sym in rows:
        c = cat_of(sym)
        agg.setdefault(c, []).append((pct, sym))
    tot = sum(p for p, _ in rows)
    print(f"== {label}: kernel symbols {tot:.2f}% of samples ==")
    for c, lst in sorted(agg.items(), key=lambda kv: -sum(p for p, _ in kv[1])):
        print(f"{sum(p for p, _ in lst):7.2f}%  {c}")
    print()
    for c, lst in sorted(agg.items(), key=lambda kv: -sum(p for p, _ in kv[1])):
        print(f"-- {c} top --")
        for pct, sym in sorted(lst, key=lambda kv: -kv[0])[:12]:
            print(f"{pct:7.2f}%  {sym}")


if __name__ == "__main__":
    main()
