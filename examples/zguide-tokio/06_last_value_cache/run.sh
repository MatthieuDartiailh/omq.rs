#!/usr/bin/env bash
set -e
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

cargo run --bin zg06_cache &
sleep 0.3
cargo run --bin zg06_publisher -- ipc://@omq-zguide-06-publisher 5
sleep 0.5
cargo run --bin zg06_subscriber
