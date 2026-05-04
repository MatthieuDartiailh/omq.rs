#!/usr/bin/env bash
# Compare zmq.rs vs omq-tokio vs omq-compio: single PUSH process → single PULL
# process, TCP loopback, one process each. Each cell: 3 s timed window after
# 500 ms warmup.
#
# zmq.rs (crate: zeromq) is a pure-Rust async ZMQ implementation built on
# tokio, making the omq-tokio comparison apples-to-apples (same runtime,
# same thread model). omq-compio runs on a single io_uring thread for contrast.
#
# Usage:
#   ./scripts/bench_compare_zmqrs.sh                   # print table to stdout
#   ./scripts/bench_compare_zmqrs.sh --update-benchmarks  # update BENCHMARKS.md section
#   ./scripts/bench_compare_zmqrs.sh [port]            # override base port (default 15655)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$SCRIPT_DIR/.."
DURATION=3
BASE_PORT=15655
UPDATE_BENCHMARKS=false

for arg in "$@"; do
    case "$arg" in
        --update-benchmarks) UPDATE_BENCHMARKS=true ;;
        [0-9]*) BASE_PORT="$arg" ;;
    esac
done

# ---------- build ----------

echo "==> building zmq.rs bench_peer..."
(cd "$SCRIPT_DIR/zmqrs_bench_peer" && cargo build --release 2>/dev/null)
ZMQRS_PEER="$SCRIPT_DIR/zmqrs_bench_peer/target/release/zmqrs_bench_peer"

echo "==> building omq-tokio bench_peer..."
cargo build --release -p omq-tokio --bin bench_peer_tokio 2>/dev/null
TOKIO_PEER="$REPO/target/release/bench_peer_tokio"

echo "==> building omq-compio bench_peer..."
cargo build --release -p omq-compio --bin bench_peer 2>/dev/null
COMPIO_PEER="$REPO/target/release/bench_peer"

# ---------- helpers ----------

# run_cell <peer_binary> <port> <size>
run_cell() {
    local peer="$1" port="$2" size="$3"

    "$peer" push "$port" "$size" &
    local push_pid=$!

    sleep 0.15

    local result
    result=$("$peer" pull "$port" "$size" "$DURATION")

    kill "$push_pid" 2>/dev/null || true
    wait "$push_pid" 2>/dev/null || true

    echo "$result"
}

# fmt_msgs <msgs_per_sec>
fmt_msgs() {
    awk -v v="$1" 'BEGIN {
        if (v >= 1e6) printf "%.2fM", v/1e6
        else          printf "%.0fk", v/1e3
    }'
}

# fmt_bw <MB_per_sec>
fmt_bw() {
    awk -v v="$1" 'BEGIN {
        if (v >= 1000) printf "%.1f GB/s", v/1000
        else           printf "%.0f MB/s", v
    }'
}

# speedup_str <omq_msgs> <zmqrs_msgs>
speedup_str() {
    awk -v o="$1" -v z="$2" 'BEGIN {
        r = o/z
        if (r >= 1.1) printf "**%.1f×**", r
        else          printf "%.2f×", r
    }'
}

# ---------- version strings ----------

ZMQRS_VERSION=$(cargo metadata --no-deps --format-version 1 \
    --manifest-path "$SCRIPT_DIR/zmqrs_bench_peer/Cargo.toml" 2>/dev/null \
    | python3 -c 'import sys,json; pkgs=json.load(sys.stdin)["packages"]; \
      print(next(p["version"] for p in pkgs if p["name"]=="zeromq"))' \
    2>/dev/null || echo '?')

OMQ_VERSION=$(cargo metadata --no-deps --format-version 1 2>/dev/null \
    | python3 -c 'import sys,json; pkgs=json.load(sys.stdin)["packages"]; \
      print(next(p["version"] for p in pkgs if p["name"]=="omq-tokio"))' \
    2>/dev/null || echo '?')

# ---------- run ----------

SIZES=(128 512 2048 8192 32768 131072)

echo ""
echo "zmq.rs (zeromq $ZMQRS_VERSION) vs omq $OMQ_VERSION — TCP loopback, 2 processes, ${DURATION}s window + 500ms warmup"
echo ""
printf "%-10s  %20s  %22s  %22s\n" "" "zmq.rs" "omq-tokio" "omq-compio"
printf "%-10s  %20s  %22s  %22s\n" "msg size" "(msg/s  |  MB/s)" "(msg/s  |  MB/s  | ×)" "(msg/s  |  MB/s  | ×)"
echo "-----------------------------------------------------------------------------------------------------------"

declare -a RES_SIZES RES_ZMQRS_MSGS RES_ZMQRS_MB RES_TOKIO_MSGS RES_TOKIO_MB RES_COMPIO_MSGS RES_COMPIO_MB

idx=0
for size in "${SIZES[@]}"; do
    PORT=$((BASE_PORT + idx))

    zmqrs_raw=$(run_cell  "$ZMQRS_PEER"  "$((PORT + 200))" "$size")
    tokio_raw=$(run_cell  "$TOKIO_PEER"  "$((PORT + 100))" "$size")
    compio_raw=$(run_cell "$COMPIO_PEER" "$PORT"           "$size")

    zmqrs_msgs=$(echo "$zmqrs_raw"  | awk '{printf "%.0f", $1/$2}')
    zmqrs_mb=$(echo   "$zmqrs_raw"  | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
    tokio_msgs=$(echo "$tokio_raw"  | awk '{printf "%.0f", $1/$2}')
    tokio_mb=$(echo   "$tokio_raw"  | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
    compio_msgs=$(echo "$compio_raw" | awk '{printf "%.0f", $1/$2}')
    compio_mb=$(echo   "$compio_raw" | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')

    tokio_x=$(speedup_str  "$tokio_msgs"  "$zmqrs_msgs")
    compio_x=$(speedup_str "$compio_msgs" "$zmqrs_msgs")

    printf "  %6dB    %9s msg/s  %6s MB/s    %9s msg/s  %6s MB/s  %6s    %9s msg/s  %6s MB/s  %6s\n" \
        "$size" \
        "$zmqrs_msgs"  "$zmqrs_mb" \
        "$tokio_msgs"  "$tokio_mb"  "$tokio_x" \
        "$compio_msgs" "$compio_mb" "$compio_x"

    RES_SIZES[$idx]=$size
    RES_ZMQRS_MSGS[$idx]=$zmqrs_msgs;  RES_ZMQRS_MB[$idx]=$zmqrs_mb
    RES_TOKIO_MSGS[$idx]=$tokio_msgs;  RES_TOKIO_MB[$idx]=$tokio_mb
    RES_COMPIO_MSGS[$idx]=$compio_msgs; RES_COMPIO_MB[$idx]=$compio_mb
    idx=$((idx + 1))
done

echo ""

# ---------- --update-benchmarks ----------

if [ "$UPDATE_BENCHMARKS" = true ]; then
    BENCHMARKS="$REPO/BENCHMARKS.md"
    MARKER="zmqrs_comparison"

    MD=""
    MD+=$'\n'
    MD+="| Size | zmq.rs msg/s | zmq.rs MB/s | omq-tokio msg/s | omq-tokio MB/s | tokio × | omq-compio msg/s | omq-compio MB/s | compio × |"$'\n'
    MD+="|-------|-------------|------------|----------------|---------------|---------|-----------------|----------------|---------|"$'\n'

    for i in "${!RES_SIZES[@]}"; do
        sz=${RES_SIZES[$i]}
        zmsg=${RES_ZMQRS_MSGS[$i]};  zmb=${RES_ZMQRS_MB[$i]}
        tmsg=${RES_TOKIO_MSGS[$i]};  tmb=${RES_TOKIO_MB[$i]}
        cmsg=${RES_COMPIO_MSGS[$i]}; cmb=${RES_COMPIO_MB[$i]}

        if   [ "$sz" -ge 131072 ]; then label="128 KiB"
        elif [ "$sz" -ge 32768  ]; then label="32 KiB"
        elif [ "$sz" -ge 8192   ]; then label="8 KiB"
        elif [ "$sz" -ge 2048   ]; then label="2 KiB"
        elif [ "$sz" -ge 512    ]; then label="512 B"
        else                            label="128 B"
        fi

        zmqrs_fmt=$(fmt_msgs "$zmsg");  zmqrs_bw=$(fmt_bw "$zmb")
        tokio_fmt=$(fmt_msgs "$tmsg");  tokio_bw=$(fmt_bw "$tmb")
        compio_fmt=$(fmt_msgs "$cmsg"); compio_bw=$(fmt_bw "$cmb")
        tokio_ratio=$(speedup_str  "$tmsg" "$zmsg")
        compio_ratio=$(speedup_str "$cmsg" "$zmsg")

        MD+="| $label | $zmqrs_fmt | $zmqrs_bw | $tokio_fmt | $tokio_bw | $tokio_ratio | $compio_fmt | $compio_bw | $compio_ratio |"$'\n'
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
