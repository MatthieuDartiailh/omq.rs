#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

python broker.py &
sleep 0.3
python worker.py ipc://@omq-zguide-01-backend 0 &
python worker.py ipc://@omq-zguide-01-backend 1 &
python worker.py ipc://@omq-zguide-01-backend 2 &
sleep 0.3
python client.py
