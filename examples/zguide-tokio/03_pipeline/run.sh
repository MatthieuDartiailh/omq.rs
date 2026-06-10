#!/usr/bin/env bash
set -e
trap 'kill $(jobs -p) 2>/dev/null' EXIT

cargo build --bins 2>&1
BIN="$(dirname "$0")/../target/debug"

"$BIN/zg03_sink" &
SINK_PID=$!
"$BIN/zg03_ventilator" &
sleep 0.3
"$BIN/zg03_worker" ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 0 &
"$BIN/zg03_worker" ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 1 &
"$BIN/zg03_worker" ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 2 &
wait $SINK_PID
