#!/usr/bin/env python3
"""Stage split for ylat-harness runs (plugin built with the `ylat-trace` feature).

Usage: ylat_stages.py stages.csv probe.csv [msgs.csv from ylat_pcap.py YLAT_DUMP]

Stages per transaction_accounts message (ms):
  queue    created_at (plugin callback) -> geyser loop dequeued it
  fanout   geyser loop -> subscriber loop got the broadcast batch
  filter   subscriber loop -> tonic encoded it (HTTP/2 stream task)
  write    encoded -> server wrote it to the socket (needs the pcap dump)
  client   socket -> probe received (or encoded -> probe when no pcap)
"""
import csv
import sys


def pct(v):
    v = sorted(v)
    if not v:
        return "n=0"
    p = lambda q: v[min(len(v) - 1, int(q * len(v)))]
    return "n=%-6d p50 %7.3f  p90 %7.3f  p99 %7.3f  p99.9 %7.3f" % (
        len(v), p(0.5), p(0.9), p(0.99), p(0.999))


def main():
    stages = {r["signature"]: r for r in csv.DictReader(open(sys.argv[1]))}
    probe = {r["signature"]: int(r["recv_unix_ns"]) for r in csv.DictReader(open(sys.argv[2]))}
    wire = {}
    if len(sys.argv) > 3:
        wire = {r["signature"]: int(r["wire_last_ns"]) for r in csv.DictReader(open(sys.argv[3]))}
    cols = {k: [] for k in ("queue", "fanout", "filter", "write", "client", "total")}
    for sig, r in stages.items():
        c, l, cl, e = (int(r[k]) for k in ("created_ns", "loop_ns", "client_ns", "encode_ns"))
        if not (l and cl and sig in probe):
            continue
        cols["queue"].append((l - c) / 1e6)
        cols["fanout"].append((cl - l) / 1e6)
        cols["filter"].append((e - cl) / 1e6)
        if sig in wire:
            cols["write"].append((wire[sig] - e) / 1e6)
            cols["client"].append((probe[sig] - wire[sig]) / 1e6)
        else:
            cols["client"].append((probe[sig] - e) / 1e6)
        cols["total"].append((probe[sig] - c) / 1e6)
    for k, v in cols.items():
        print("%-7s %s" % (k, pct(v)))


if __name__ == "__main__":
    main()
