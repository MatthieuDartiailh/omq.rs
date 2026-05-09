#!/usr/bin/env bash
set -e
trap 'kill $(jobs -p) 2>/dev/null' EXIT

cargo run --bin zg10_primary &
PRIMARY=$!
cargo run --bin zg10_backup &
sleep 0.5

cargo run --bin zg10_client -- ipc://@omq-zguide-10-primary ipc://@omq-zguide-10-backup 2
sleep 0.5

kill $PRIMARY 2>/dev/null
echo "--- primary killed ---"
sleep 0.5

cargo run --bin zg10_client -- ipc://@omq-zguide-10-primary ipc://@omq-zguide-10-backup 2
