#!/usr/bin/env bash
# Compare zmq.rs vs omq-tokio vs omq-compio: single PUSH process -> single PULL
# process, loopback. Each cell: 3 s timed window after 500 ms warmup.
#
# zmq.rs (crate: zeromq) is a pure-Rust async ZMQ implementation built on
# tokio, making the omq-tokio comparison apples-to-apples (same runtime,
# same thread model). omq-compio runs on a single io_uring thread for contrast.
#
# Usage:
#   ./scripts/compare_zmqrs.sh                     # TCP, print to stdout
#   ./scripts/compare_zmqrs.sh --ipc               # IPC, print to stdout
#   ./scripts/compare_zmqrs.sh --update-benchmarks # TCP, update COMPARISONS.md
#   ./scripts/compare_zmqrs.sh --ipc --update-benchmarks
#   ./scripts/compare_zmqrs.sh [port]              # override base TCP port (default 15655)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$SCRIPT_DIR/.."
DURATION=3
BASE_PORT=15655
UPDATE_BENCHMARKS=false
TRANSPORT=tcp

for arg in "$@"; do
    case "$arg" in
        --update-benchmarks) UPDATE_BENCHMARKS=true ;;
        --ipc) TRANSPORT=ipc ;;
        [0-9]*) BASE_PORT="$arg" ;;
    esac
done

# ---------- build ----------

echo "==> building zmq.rs bench_peer..."
(cd "$SCRIPT_DIR/zmqrs_bench_peer" && cargo build --release -q)
ZMQRS_PEER="$SCRIPT_DIR/zmqrs_bench_peer/target/release/zmqrs_bench_peer"

echo "==> building omq-tokio bench_peer..."
cargo build --release -p omq-tokio --bin bench_peer_tokio -q
TOKIO_PEER="$REPO/target/release/bench_peer_tokio"

echo "==> building omq-compio bench_peer..."
cargo build --release -p omq-compio --bin bench_peer -q
COMPIO_PEER="$REPO/target/release/bench_peer"

# ---------- helpers ----------

addr_for() {
    local prefix="$1" idx="$2"
    if [ "$TRANSPORT" = "ipc" ]; then
        echo "ipc:///tmp/omq-bench-zmqrs-${prefix}-${idx}.sock"
    else
        echo "$((BASE_PORT + idx))"
    fi
}

run_cell() {
    local peer="$1" addr="$2" size="$3"

    "$peer" push "$addr" "$size" &
    local push_pid=$!

    sleep 0.15

    local result
    result=$("$peer" pull "$addr" "$size" "$DURATION")

    kill "$push_pid" 2>/dev/null || true
    wait "$push_pid" 2>/dev/null || true

    echo "$result"
}

fmt_msgs() {
    awk -v v="$1" 'BEGIN {
        if (v >= 1e6)      printf "%.2fM", v/1e6
        else if (v >= 1e3) printf "%.0fk", v/1e3
        else               printf "%.0f", v
    }'
}

fmt_bw() {
    awk -v v="$1" 'BEGIN {
        if (v >= 1000) printf "%.1f GB/s", v/1000
        else           printf "%.0f MB/s", v
    }'
}

fmt_size() {
    awk -v v="$1" 'BEGIN {
        if (v >= 1048576) printf "%g MiB", v/1048576
        else if (v >= 1024) printf "%g KiB", v/1024
        else printf "%d B", v
    }'
}

speedup_str() {
    awk -v o="$1" -v z="$2" 'BEGIN {
        r = o/z
        if (r >= 1.1) printf "**%.1fx**", r
        else          printf "%.2fx", r
    }'
}

# ---------- version strings ----------

ZMQRS_VERSION=$(cargo metadata --format-version 1 \
    --manifest-path "$SCRIPT_DIR/zmqrs_bench_peer/Cargo.toml" 2>/dev/null \
    | python3 -c 'import sys,json; pkgs=json.load(sys.stdin)["packages"]; \
      print(next(p["version"] for p in pkgs if p["name"]=="zeromq"))' \
    2>/dev/null || echo '?')

OMQ_VERSION=$(cargo metadata --no-deps --format-version 1 2>/dev/null \
    | python3 -c 'import sys,json; pkgs=json.load(sys.stdin)["packages"]; \
      print(next(p["version"] for p in pkgs if p["name"]=="omq-tokio"))' \
    2>/dev/null || echo '?')

# ---------- run ----------

SIZES=(8 32 128 512 2048 8192 32768 131072 524288 2097152 8388608 33554432)

echo ""
echo "zmq.rs (zeromq $ZMQRS_VERSION) vs omq $OMQ_VERSION - ${TRANSPORT^^} loopback, 2 processes, ${DURATION}s window + 500ms warmup"
echo ""
printf "%-10s  %20s  %22s  %22s\n" "" "zmq.rs" "omq-compio" "omq-tokio"
printf "%-10s  %20s  %22s  %22s\n" "msg size" "(msg/s  |  MB/s)" "(msg/s  |  MB/s  | x)" "(msg/s  |  MB/s  | x)"
echo "-----------------------------------------------------------------------------------------------------------"

declare -a RES_SIZES RES_ZMQRS_MSGS RES_ZMQRS_MB RES_TOKIO_MSGS RES_TOKIO_MB RES_COMPIO_MSGS RES_COMPIO_MB

idx=0
for size in "${SIZES[@]}"; do
    ADDR_Z=$(addr_for "z" "$idx")
    ADDR_T=$(addr_for "t" "$idx")
    ADDR_C=$(addr_for "c" "$idx")

    zmqrs_raw=$(run_cell  "$ZMQRS_PEER"  "$ADDR_Z" "$size")
    tokio_raw=$(run_cell  "$TOKIO_PEER"  "$ADDR_T" "$size")
    compio_raw=$(run_cell "$COMPIO_PEER" "$ADDR_C" "$size")

    zmqrs_msgs=$(echo "$zmqrs_raw"  | awk '{printf "%.0f", $1/$2}')
    zmqrs_mb=$(echo   "$zmqrs_raw"  | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
    tokio_msgs=$(echo "$tokio_raw"  | awk '{printf "%.0f", $1/$2}')
    tokio_mb=$(echo   "$tokio_raw"  | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
    compio_msgs=$(echo "$compio_raw" | awk '{printf "%.0f", $1/$2}')
    compio_mb=$(echo   "$compio_raw" | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')

    tokio_x=$(speedup_str  "$tokio_msgs"  "$zmqrs_msgs")
    compio_x=$(speedup_str "$compio_msgs" "$zmqrs_msgs")

    printf "  %7s    %9s msg/s  %6s MB/s    %9s msg/s  %6s MB/s  %6s    %9s msg/s  %6s MB/s  %6s\n" \
        "$(fmt_size "$size")" \
        "$zmqrs_msgs"  "$zmqrs_mb" \
        "$compio_msgs" "$compio_mb" "$compio_x" \
        "$tokio_msgs"  "$tokio_mb"  "$tokio_x"

    RES_SIZES[$idx]=$size
    RES_ZMQRS_MSGS[$idx]=$zmqrs_msgs;  RES_ZMQRS_MB[$idx]=$zmqrs_mb
    RES_TOKIO_MSGS[$idx]=$tokio_msgs;  RES_TOKIO_MB[$idx]=$tokio_mb
    RES_COMPIO_MSGS[$idx]=$compio_msgs; RES_COMPIO_MB[$idx]=$compio_mb
    idx=$((idx + 1))
done

echo ""

# ---------- --update-benchmarks ----------

if [ "$UPDATE_BENCHMARKS" = true ]; then
    BENCHMARKS="$REPO/COMPARISONS.md"
    if [ "$TRANSPORT" = "ipc" ]; then
        MARKER="zmqrs_comparison_ipc"
    else
        MARKER="zmqrs_comparison"
    fi

    MD=""
    MD+=$'\n'
    MD+="| Size | zmq.rs msg/s | zmq.rs MB/s | omq-compio msg/s | omq-compio MB/s | compio x | omq-tokio msg/s | omq-tokio MB/s | tokio x |"$'\n'
    MD+="|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|"$'\n'

    for i in "${!RES_SIZES[@]}"; do
        sz=${RES_SIZES[$i]}
        zmsg=${RES_ZMQRS_MSGS[$i]};  zmb=${RES_ZMQRS_MB[$i]}
        tmsg=${RES_TOKIO_MSGS[$i]};  tmb=${RES_TOKIO_MB[$i]}
        cmsg=${RES_COMPIO_MSGS[$i]}; cmb=${RES_COMPIO_MB[$i]}

        label=$(fmt_size "$sz")

        zmqrs_fmt=$(fmt_msgs "$zmsg");  zmqrs_bw=$(fmt_bw "$zmb")
        tokio_fmt=$(fmt_msgs "$tmsg");  tokio_bw=$(fmt_bw "$tmb")
        compio_fmt=$(fmt_msgs "$cmsg"); compio_bw=$(fmt_bw "$cmb")
        tokio_ratio=$(speedup_str  "$tmsg" "$zmsg")
        compio_ratio=$(speedup_str "$cmsg" "$zmsg")

        MD+="| $label | $zmqrs_fmt | $zmqrs_bw | $compio_fmt | $compio_bw | $compio_ratio | $tokio_fmt | $tokio_bw | $tokio_ratio |"$'\n'
    done
    MD+=$'\n'

    BEGIN_MARKER="<!-- BEGIN $MARKER -->"
    END_MARKER="<!-- END $MARKER -->"

    if ! grep -q "$BEGIN_MARKER" "$BENCHMARKS"; then
        echo "ERROR: marker '$BEGIN_MARKER' not found in $BENCHMARKS" >&2
        exit 1
    fi

    python3 - "$BENCHMARKS" "$BEGIN_MARKER" "$END_MARKER" "$MD" <<'EOF'
import sys
path, begin, end, content = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
text = open(path).read()
import re
new = re.sub(re.escape(begin) + r'.*?' + re.escape(end), begin + content + end, text, flags=re.DOTALL)
open(path, 'w').write(new)
print(f"Updated {path}")
EOF
fi
