#!/bin/bash
# usage: ylat_capture.sh TAG SECONDS [probe args...]
# Captures the server->probe direction of one fresh Yellowstone connection on lo and runs
# the probe; afterwards waits for the overlapping geyserbench run's CSV and splits.
set -u
TAG=$1; SECS=$2; shift 2
PORT=${PORT:-41234}
D=/dev/shm/ylat
mkdir -p $D
BIN=/home/sol/yellowstone-grpc-ylat/target/release/ylat-probe
sudo tcpdump -i lo -s 0 --time-stamp-precision=nano -B 262144 -U -w $D/$TAG.pcap \
  "tcp and src port 10000 and dst port $PORT" 2> $D/$TAG.tcpdump.log &
sleep 1.5
START=$(date +%s)
$BIN --local-port $PORT --delay-subscribe-ms 300 --seconds $SECS --out $D/$TAG.csv "$@" 2>&1 | tail -3
sleep 0.5
sudo pkill -INT -f "$D/$TAG.pcap"
sleep 1
tail -2 $D/$TAG.tcpdump.log
# overlapping geyserbench run CSV (goal-series keeps the last one)
for i in $(seq 1 150); do
  M=$(stat -c %Y /tmp/gbr/series_last.csv 2>/dev/null || echo 0)
  [ "$M" -gt "$((START + SECS))" ] && break
  sleep 2
done
cp /tmp/gbr/series_last.csv $D/$TAG.gb.csv
python3 $D/ylat_pcap.py $D/$TAG.pcap $D/$TAG.csv $D/$TAG.gb.csv
