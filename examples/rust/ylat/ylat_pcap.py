#!/usr/bin/env python3
"""Split Yellowstone transaction_accounts delivery latency with a loopback packet capture.

Input: a nanosecond pcap of the server->client direction of ONE gRPC connection, captured
from before the connection opened (tcpdump -i lo --time-stamp-precision=nano -s 0
'tcp and src port 10000 and dst port <probe local port>'), plus the probe's CSV.

For every SubscribeUpdate carrying transaction_accounts (field 100) it recovers the
server created_at (field 11) and the capture time of the packet holding the message's
last byte (= when the server's write() handed it to the kernel), and joins the probe's
receive time by signature:

    server  = wire_last - created_at     (Yellowstone internal: queues, tasks, h2 encode)
    client  = recv - wire_last           (kernel loopback + client runtime + decode)

Usage: ylat_pcap.py capture.pcap probe.csv|- [geyserbench_run.csv]
       (YLAT_PORT=<client port> selects the connection; YLAT_DUMP=<csv> dumps per-message rows)
"""
import csv
import struct
import sys

B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58(b):
    n = int.from_bytes(b, "big")
    s = ""
    while n:
        n, r = divmod(n, 58)
        s = B58[r] + s
    pad = len(b) - len(b.lstrip(b"\0"))
    return "1" * pad + s


def varint(buf, i):
    v = shift = 0
    while True:
        c = buf[i]
        i += 1
        v |= (c & 0x7F) << shift
        if c < 0x80:
            return v, i
        shift += 7


def fields(buf):
    i = 0
    while i < len(buf):
        key, i = varint(buf, i)
        f, w = key >> 3, key & 7
        if w == 0:
            v, i = varint(buf, i)
            yield f, v
        elif w == 1:
            yield f, buf[i : i + 8]
            i += 8
        elif w == 2:
            n, i = varint(buf, i)
            yield f, buf[i : i + n]
            i += n
        elif w == 5:
            yield f, buf[i : i + 4]
            i += 4
        else:
            raise ValueError("wire type %d" % w)


def read_pcap(path):
    with open(path, "rb") as f:
        hdr = f.read(24)
        magic = struct.unpack("<I", hdr[:4])[0]
        if magic == 0xA1B23C4D:
            end, nano = "<", True
        elif magic == 0xA1B2C3D4:
            end, nano = "<", False
        else:
            raise SystemExit("unsupported pcap magic %x" % magic)
        linktype = struct.unpack(end + "I", hdr[20:24])[0]
        while True:
            rh = f.read(16)
            if len(rh) < 16:
                return
            sec, frac, incl, _orig = struct.unpack(end + "IIII", rh)
            data = f.read(incl)
            ts = sec * 1_000_000_000 + (frac if nano else frac * 1000)
            if linktype == 1:
                off = 14
            elif linktype == 113:
                off = 16
            elif linktype == 276:
                off = 20
            else:
                raise SystemExit("linktype %d" % linktype)
            ip = data[off:]
            if (ip[0] >> 4) != 4:
                continue
            ihl = (ip[0] & 15) * 4
            if ip[9] != 6:
                continue
            tcp = ip[ihl:]
            sport, dport, seq = struct.unpack(">HHI", tcp[:8])
            doff = (tcp[12] >> 4) * 4
            flags = tcp[13]
            yield ts, sport, dport, seq, flags, tcp[doff:]


def main():
    pcap, probe_csv = sys.argv[1], sys.argv[2]
    gb_csv = sys.argv[3] if len(sys.argv) > 3 else None

    # Reassemble the server->client byte stream; remember the capture time of each segment.
    # With several connections in the capture, YLAT_PORT picks one (client port); otherwise
    # the connection with the most transaction_accounts payload (field-100 tag) is used.
    import os
    conns = {}
    for ts, sport, dport, seq, flags, payload in read_pcap(pcap):
        c = conns.setdefault(dport, {"isn": None, "segs": {}, "hits": 0})
        if flags & 0x02:  # SYN(-ACK) from the server
            c["isn"] = (seq + 1) & 0xFFFFFFFF
            continue
        if payload and seq not in c["segs"]:
            c["segs"][seq] = (ts, payload)
            c["hits"] += payload.count(b"\xa2\x06")
    want = os.environ.get("YLAT_PORT")
    cands = [p for p, c in conns.items() if c["isn"] is not None]
    if want:
        port = int(want)
    elif cands:
        port = max(cands, key=lambda p: conns[p]["hits"])
    else:
        raise SystemExit("no SYN in capture: start tcpdump before the client connects")
    print("connection: client port", port, "of", sorted(conns))
    isn, segs = conns[port]["isn"], conns[port]["segs"]
    if isn is None:
        raise SystemExit("no SYN for port %d" % port)
    stream = bytearray()
    times = []  # (end_offset_exclusive, ts)
    pos = isn
    for seq in sorted(segs, key=lambda s: (s - isn) & 0xFFFFFFFF):
        ts, payload = segs[seq]
        rel = (seq - isn) & 0xFFFFFFFF
        if rel > len(stream):
            raise SystemExit("gap in capture at %d" % rel)
        if rel + len(payload) <= len(stream):
            continue
        payload = payload[len(stream) - rel :]
        stream += payload
        times.append((len(stream), ts))

    import bisect

    ends = [e for e, _ in times]

    def ts_at(offset):  # capture time of the segment carrying byte `offset`
        return times[bisect.bisect_right(ends, offset)][1]

    # h2 frames -> DATA payload of the (single) response stream, with source offsets.
    i = 0
    grpc = bytearray()
    goff = []  # absolute stream offset of each grpc byte chunk start: (grpc_len_before, abs_off)
    while i + 9 <= len(stream):
        ln = int.from_bytes(stream[i : i + 3], "big")
        ftype, fl = stream[i + 3], stream[i + 4]
        if i + 9 + ln > len(stream):
            break
        body_off = i + 9
        body = stream[body_off : body_off + ln]
        if ftype == 0:
            if fl & 0x08:  # padded
                pad = body[0]
                body = body[1 : len(body) - pad]
                body_off += 1
            goff.append((len(grpc), body_off))
            grpc += body
        i += 9 + ln
    gstarts = [g for g, _ in goff]

    def abs_off(goffset):
        k = bisect.bisect_right(gstarts, goffset) - 1
        return goff[k][1] + (goffset - goff[k][0])

    out = {}
    j = 0
    while j + 5 <= len(grpc):
        n = int.from_bytes(grpc[j + 1 : j + 5], "big")
        if j + 5 + n > len(grpc):
            break
        msg = bytes(grpc[j + 5 : j + 5 + n])
        last_ts = ts_at(abs_off(j + 5 + n - 1))
        first_ts = ts_at(abs_off(j))
        sig = created = None
        for f, v in fields(msg):
            if f == 100:
                for g, w in fields(v):
                    if g == 1:
                        sig = b58(w)
                        break
            elif f == 11:
                s = ns = 0
                for g, w in fields(v):
                    if g == 1:
                        s = w
                    elif g == 2:
                        ns = w
                created = s * 1_000_000_000 + ns
        if sig and created:
            out[sig] = (created, first_ts, last_ts, n)
        j += 5 + n

    probe = {}
    if probe_csv != "-":
        with open(probe_csv) as f:
            for r in csv.DictReader(f):
                probe[r["signature"]] = int(r["recv_unix_ns"])
    gb = {}
    if gb_csv:
        with open(gb_csv) as f:
            for r in csv.DictReader(f):
                if r["endpoint"] == "txacc":
                    gb[r["signature"]] = int(r["wallclock_unix_ns"])

    def pct(v, label):
        v = sorted(v)
        if not v:
            print("%-34s n=0" % label)
            return
        p = lambda q: v[min(len(v) - 1, int(q * len(v)))]
        print(
            "%-34s n=%-7d p50 %7.3f  p90 %7.3f  p99 %7.3f  p99.9 %7.3f  ms"
            % (label, len(v), p(0.5), p(0.9), p(0.99), p(0.999))
        )

    srv, cli, tot, gbt, gbcli = [], [], [], [], []
    for sig, (created, first_ts, last_ts, n) in out.items():
        srv.append((last_ts - created) / 1e6)
        if sig in probe:
            cli.append((probe[sig] - last_ts) / 1e6)
            tot.append((probe[sig] - created) / 1e6)
        if sig in gb:
            gbt.append((gb[sig] - created) / 1e6)
            if sig in probe:
                gbcli.append((gb[sig] - probe[sig]) / 1e6)
    import os
    dump = os.environ.get("YLAT_DUMP")
    if dump:
        with open(dump, "w") as f:
            f.write("signature,created_ns,wire_first_ns,wire_last_ns,bytes,probe_recv_ns,gb_recv_ns\n")
            for sig, (created, first_ts, last_ts, n) in sorted(out.items(), key=lambda kv: kv[1][0]):
                f.write("%s,%d,%d,%d,%d,%s,%s\n" % (sig, created, first_ts, last_ts, n, probe.get(sig, ""), gb.get(sig, "")))
    print("messages parsed from capture:", len(out), " probe rows:", len(probe))
    pct(srv, "server: created_at -> wire")
    pct(cli, "client(probe): wire -> recv")
    pct(tot, "total(probe): created_at -> recv")
    if gb_csv:
        gbw = [(gb[sig] - v[2]) / 1e6 for sig, v in out.items() if sig in gb]
        pct(gbt, "geyserbench: created_at -> recv")
        pct(gbw, "geyserbench: wire -> recv")
        pct(gbcli, "geyserbench recv - probe recv")


if __name__ == "__main__":
    main()
