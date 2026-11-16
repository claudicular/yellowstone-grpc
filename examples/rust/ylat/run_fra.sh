#!/bin/bash
# run_fra.sh TAG CONFIG [SECONDS] [PROBE_CPU]  -- harness A/B on FRA (user.slice CPUs)
cd /dev/shm/ylat
TAG=$1; CFG=$2; SECS=${3:-30}; PCPU=${4:-27}
B=/home/sol/yellowstone-grpc-ylat/target/release
$B/ylat-harness --config $CFG --trace trace_all.txt --seconds $SECS --warmup-s 4 \
  --stages-out stages_$TAG.csv > h_$TAG.log 2>&1 &
H=$!
sleep 2
$B/ylat-probe --endpoint http://127.0.0.1:10077 --seconds $((SECS + 4)) --cpu $PCPU --spin \
  --deshred --out probe_$TAG.csv 2>&1 | tail -1
wait $H
tail -2 h_$TAG.log | head -1
python3 ylat_stages.py stages_$TAG.csv probe_$TAG.csv
