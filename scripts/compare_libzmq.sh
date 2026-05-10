#!/usr/bin/env bash
# Compare omq-compio + omq-tokio vs libzmq: single PUSH process → single PULL
# process, TCP loopback. Each cell: 3 s timed window after 500 ms warmup.
#
# Usage:
#   ./scripts/compare_libzmq.sh                   # print table to stdout
#   ./scripts/compare_libzmq.sh --update-benchmarks  # update COMPARISONS.md section
#   ./scripts/compare_libzmq.sh [port]            # override base port (default 15555)

set -euo pipefail

cleanup() {
    trap - INT TERM EXIT
    kill 0 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup INT TERM EXIT

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$SCRIPT_DIR/.."
DURATION=3
BASE_PORT=15555
UPDATE_BENCHMARKS=false

for arg in "$@"; do
    case "$arg" in
        --update-benchmarks) UPDATE_BENCHMARKS=true ;;
        [0-9]*) BASE_PORT="$arg" ;;
    esac
done

# ---------- build ----------

echo "==> building omq-compio bench_peer..."
cargo build --release -p omq-compio --bin bench_peer 2>/dev/null
OMQ_PEER="$REPO/target/release/bench_peer"

echo "==> building omq-tokio bench_peer..."
cargo build --release -p omq-tokio --bin bench_peer_tokio 2>/dev/null
TOKIO_PEER="$REPO/target/release/bench_peer_tokio"

echo "==> building libzmq bench_peer..."
gcc -O2 -o "$SCRIPT_DIR/libzmq_bench_peer" \
    "$SCRIPT_DIR/libzmq_bench_peer.c" -lzmq
LIBZMQ_PEER="$SCRIPT_DIR/libzmq_bench_peer"

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

# fmt_msgs <msgs_per_sec>  → e.g. "2,568k" or "540k"
fmt_msgs() {
    awk -v v="$1" 'BEGIN {
        if (v >= 1e6)      printf "%.2fM", v/1e6
        else if (v >= 1e3) printf "%.0fk", v/1e3
        else               printf "%.0f", v
    }'
}

# fmt_bw <MB_per_sec>  → e.g. "329" or "4.4 GB/s"
fmt_bw() {
    awk -v v="$1" 'BEGIN {
        if (v >= 1000) printf "%.1f GB/s", v/1000
        else           printf "%.0f MB/s", v
    }'
}

# fmt_size <bytes>
fmt_size() {
    awk -v v="$1" 'BEGIN {
        if (v >= 1048576) printf "%g MiB", v/1048576
        else if (v >= 1024) printf "%g KiB", v/1024
        else printf "%d B", v
    }'
}

# ratio_str <omq_msgs> <zmq_msgs>
ratio_str() {
    awk -v o="$1" -v z="$2" 'BEGIN {
        r = o/z
        if (r >= 1.1) printf "**%.1f×**", r
        else          printf "%.2f×", r
    }'
}

# ---------- run ----------

SIZES=(8 32 128 512 2048 8192 32768 131072 524288 2097152 8388608 33554432)
OMQ_VERSION=$(cargo metadata --no-deps --format-version 1 2>/dev/null \
    | python3 -c 'import sys,json; pkgs=json.load(sys.stdin)["packages"]; \
      print(next(p["version"] for p in pkgs if p["name"]=="omq-compio"))' \
    2>/dev/null || echo '?')
ZMQ_VERSION=$(pkg-config --modversion libzmq 2>/dev/null || echo '?')

echo ""
echo "omq $OMQ_VERSION vs libzmq $ZMQ_VERSION"
echo "TCP loopback, 2 processes, ${DURATION}s window + 500ms warmup"
echo ""
printf "%-10s  %20s  %22s  %22s\n" "" "libzmq" "omq-compio" "omq-tokio"
printf "%-10s  %20s  %22s  %22s\n" "msg size" "(msg/s  |  MB/s)" "(msg/s  |  MB/s  | ×)" "(msg/s  |  MB/s  | ×)"
echo "-----------------------------------------------------------------------------------------------------------"

declare -a RESULTS_SIZES RESULTS_OMQ_MSGS RESULTS_OMQ_MB RESULTS_TOKIO_MSGS RESULTS_TOKIO_MB RESULTS_ZMQ_MSGS RESULTS_ZMQ_MB

idx=0
for size in "${SIZES[@]}"; do
    # Use sequential ports to avoid overflow for large sizes.
    PORT=$((BASE_PORT + idx))

    omq_raw=$(run_cell   "$OMQ_PEER"    "$PORT"            "$size")
    tokio_raw=$(run_cell "$TOKIO_PEER"  "$((PORT + 100))"  "$size")
    lzq_raw=$(run_cell   "$LIBZMQ_PEER" "$((PORT + 200))"  "$size")

    omq_msgs=$(echo   "$omq_raw"   | awk '{printf "%.0f", $1/$2}')
    omq_mb=$(echo     "$omq_raw"   | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
    tokio_msgs=$(echo "$tokio_raw" | awk '{printf "%.0f", $1/$2}')
    tokio_mb=$(echo   "$tokio_raw" | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
    lzq_msgs=$(echo   "$lzq_raw"   | awk '{printf "%.0f", $1/$2}')
    lzq_mb=$(echo     "$lzq_raw"   | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')

    omq_ratio=$(ratio_str   "$omq_msgs"   "$lzq_msgs")
    tokio_ratio=$(ratio_str "$tokio_msgs" "$lzq_msgs")
    printf "  %7s    %9s msg/s  %6s MB/s    %9s msg/s  %6s MB/s  %6s    %9s msg/s  %6s MB/s  %6s\n" \
        "$(fmt_size "$size")" \
        "$lzq_msgs"   "$lzq_mb" \
        "$omq_msgs"   "$omq_mb"   "$omq_ratio" \
        "$tokio_msgs" "$tokio_mb" "$tokio_ratio"

    RESULTS_SIZES[$idx]=$size
    RESULTS_OMQ_MSGS[$idx]=$omq_msgs
    RESULTS_OMQ_MB[$idx]=$omq_mb
    RESULTS_TOKIO_MSGS[$idx]=$tokio_msgs
    RESULTS_TOKIO_MB[$idx]=$tokio_mb
    RESULTS_ZMQ_MSGS[$idx]=$lzq_msgs
    RESULTS_ZMQ_MB[$idx]=$lzq_mb
    idx=$((idx + 1))
done

echo ""

# ---------- --update-benchmarks ----------

if [ "$UPDATE_BENCHMARKS" = true ]; then
    BENCHMARKS="$REPO/COMPARISONS.md"
    MARKER="libzmq_comparison"

    # Build markdown table
    MD=""
    MD+=$'\n'
    MD+="| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |"$'\n'
    MD+="|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|"$'\n'

    for i in "${!RESULTS_SIZES[@]}"; do
        sz=${RESULTS_SIZES[$i]}
        omsg=${RESULTS_OMQ_MSGS[$i]}
        omb=${RESULTS_OMQ_MB[$i]}
        tmsg=${RESULTS_TOKIO_MSGS[$i]}
        tmb=${RESULTS_TOKIO_MB[$i]}
        zmsg=${RESULTS_ZMQ_MSGS[$i]}
        zmb=${RESULTS_ZMQ_MB[$i]}

        label=$(fmt_size "$sz")
        zmq_fmt=$(fmt_msgs "$zmsg");   zmq_bw=$(fmt_bw "$zmb")
        omq_fmt=$(fmt_msgs "$omsg");   omq_bw=$(fmt_bw "$omb")
        tokio_fmt=$(fmt_msgs "$tmsg"); tokio_bw=$(fmt_bw "$tmb")
        omq_ratio=$(ratio_str   "$omsg" "$zmsg")
        tokio_ratio=$(ratio_str "$tmsg" "$zmsg")

        MD+="| $label | $zmq_fmt | $zmq_bw | $omq_fmt | $omq_bw | $omq_ratio | $tokio_fmt | $tokio_bw | $tokio_ratio |"$'\n'
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
