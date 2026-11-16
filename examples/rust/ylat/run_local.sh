#!/bin/bash
# run_local.sh TAG CONFIG
# Run from a directory holding trace15.txt (a ylat-probe --record trace) and the configs.
R=$(cd "$(dirname "$0")/../../.." && pwd)
Y=${YLAT_BIN:-$R/target/release}
$Y/ylat-harness --config $2 --trace trace15.txt --warmup-s 3 --stages-out stages_$1.csv > h_$1.log 2>&1 &
H=$!
sleep 1.5
$Y/ylat-probe --endpoint http://127.0.0.1:10077 --seconds 18 --deshred --out probe_$1.csv 2>&1 | tail -1
wait $H
python3 $R/examples/rust/ylat/ylat_stages.py stages_$1.csv probe_$1.csv
