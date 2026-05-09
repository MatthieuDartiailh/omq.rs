#!/usr/bin/env bash
set -e
trap 'kill $(jobs -p) 2>/dev/null' EXIT

cargo run --bin zg08_broker -- ipc://@omq-zguide-08c-frontend ipc://@omq-zguide-08c-backend 3 &
sleep 0.3
cargo run --bin zg08_worker -- ipc://@omq-zguide-08c-backend echo 0 &
cargo run --bin zg08_worker -- ipc://@omq-zguide-08c-backend echo 1 &
cargo run --bin zg08_worker -- ipc://@omq-zguide-08c-backend upper 0 &
sleep 0.3
cargo run --bin zg08_client -- ipc://@omq-zguide-08c-frontend
