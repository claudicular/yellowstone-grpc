#!/bin/bash
# run_local.sh TAG CONFIG
cd /private/tmp/claude-501/-Users-huan-ji-code-arb-bot/a2f5694e-4b22-46c4-a472-4e54472a2c85/scratchpad
Y=~/code/yellowstone-grpc/target/release
$Y/ylat-harness --config $2 --trace trace15.txt --warmup-s 3 --stages-out stages_$1.csv > h_$1.log 2>&1 &
H=$!
sleep 1.5
$Y/ylat-probe --endpoint http://127.0.0.1:10077 --seconds 18 --deshred --out probe_$1.csv 2>&1 | tail -1
wait $H
python3 ~/code/yellowstone-grpc/examples/rust/ylat/ylat_stages.py stages_$1.csv probe_$1.csv
