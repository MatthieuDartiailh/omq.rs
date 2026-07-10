#!/usr/bin/env bash
set -e
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

cargo run --bin zg04_server &
sleep 0.3
cargo run --bin zg04_client
