#!/usr/bin/env bash
set -e
trap 'kill $(jobs -p) 2>/dev/null' EXIT

cargo run --bin zg03_sink &
sleep 0.3
cargo run --bin zg03_worker -- ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 0 &
cargo run --bin zg03_worker -- ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 1 &
cargo run --bin zg03_worker -- ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 2 &
sleep 0.3
cargo run --bin zg03_ventilator
wait
