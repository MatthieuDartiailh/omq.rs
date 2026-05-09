#!/usr/bin/env bash
set -e
STORE=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$STORE"' EXIT

cargo run --bin zg09_frontend -- ipc://@omq-zguide-09c-frontend ipc://@omq-zguide-09c-dispatch "$STORE" &
sleep 0.3
cargo run --bin zg09_dispatcher -- ipc://@omq-zguide-09c-dispatch "$STORE" &
sleep 0.3
cargo run --bin zg09_client -- ipc://@omq-zguide-09c-frontend
