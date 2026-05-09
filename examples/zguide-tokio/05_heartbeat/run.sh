#!/usr/bin/env bash
set -e
trap 'kill $(jobs -p) 2>/dev/null' EXIT

cargo run --bin zg05_publisher &
cargo run --bin zg05_monitor
