#!/usr/bin/env bash
set -e
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

cargo run --bin zg01_broker &
sleep 0.3
cargo run --bin zg01_worker -- ipc://@omq-zguide-01-backend 0 &
cargo run --bin zg01_worker -- ipc://@omq-zguide-01-backend 1 &
cargo run --bin zg01_worker -- ipc://@omq-zguide-01-backend 2 &
sleep 0.3
cargo run --bin zg01_client
