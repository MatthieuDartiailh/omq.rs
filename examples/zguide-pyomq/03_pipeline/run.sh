#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

python sink.py &
sleep 0.3
python worker.py ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 0 &
python worker.py ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 1 &
python worker.py ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 2 &
sleep 0.3
python ventilator.py
wait
