#!/usr/bin/env bash
# Compare omq-compio vs libzmq: single PUSH process → single PULL process,
# TCP loopback, small messages. Each cell: 3 s timed window after 500 ms warmup.
#
# Usage:
#   ./scripts/bench_compare.sh                   # print table to stdout
#   ./scripts/bench_compare.sh --update-benchmarks  # update BENCHMARKS.md section
#   ./scripts/bench_compare.sh [port]            # override base port (default 15555)

set -euo pipefail

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
        if (v >= 1e6) printf "%.2fM", v/1e6
        else          printf "%.0fk", v/1e3
    }'
}

# fmt_bw <MB_per_sec>  → e.g. "329" or "4.4 GB/s"
fmt_bw() {
    awk -v v="$1" 'BEGIN {
        if (v >= 1000) printf "%.1f GB/s", v/1000
        else           printf "%.0f MB/s", v
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

SIZES=(128 512 2048 8192 32768 131072)
OMQ_VERSION=$(cargo metadata --no-deps --format-version 1 2>/dev/null \
    | python3 -c 'import sys,json; pkgs=json.load(sys.stdin)["packages"]; \
      print(next(p["version"] for p in pkgs if p["name"]=="omq-compio"))' \
    2>/dev/null || echo '?')
ZMQ_VERSION=$(pkg-config --modversion libzmq 2>/dev/null || echo '?')

echo ""
echo "omq-compio $OMQ_VERSION vs libzmq $ZMQ_VERSION"
echo "TCP loopback, 2 processes, ${DURATION}s window + 500ms warmup"
echo ""
printf "%-10s  %20s  %20s\n" "" "omq-compio" "libzmq"
printf "%-10s  %20s  %20s\n" "msg size" "(msg/s  |  MB/s)" "(msg/s  |  MB/s)"
echo "--------------------------------------------------------------------"

declare -a RESULTS_SIZES RESULTS_OMQ_MSGS RESULTS_OMQ_MB RESULTS_ZMQ_MSGS RESULTS_ZMQ_MB

idx=0
for size in "${SIZES[@]}"; do
    # Use sequential ports to avoid overflow for large sizes.
    PORT=$((BASE_PORT + idx))

    omq_raw=$(run_cell "$OMQ_PEER"    "$PORT"             "$size")
    lzq_raw=$(run_cell "$LIBZMQ_PEER" "$((PORT + 100))"   "$size")

    omq_msgs=$(echo "$omq_raw" | awk '{printf "%.0f", $1/$2}')
    omq_mb=$(echo   "$omq_raw" | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
    lzq_msgs=$(echo "$lzq_raw" | awk '{printf "%.0f", $1/$2}')
    lzq_mb=$(echo   "$lzq_raw" | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')

    printf "  %6dB    %9s msg/s  %6s MB/s    %9s msg/s  %6s MB/s\n" \
        "$size" "$omq_msgs" "$omq_mb" "$lzq_msgs" "$lzq_mb"

    RESULTS_SIZES[$idx]=$size
    RESULTS_OMQ_MSGS[$idx]=$omq_msgs
    RESULTS_OMQ_MB[$idx]=$omq_mb
    RESULTS_ZMQ_MSGS[$idx]=$lzq_msgs
    RESULTS_ZMQ_MB[$idx]=$lzq_mb
    idx=$((idx + 1))
done

echo ""

# ---------- --update-benchmarks ----------

if [ "$UPDATE_BENCHMARKS" = true ]; then
    BENCHMARKS="$REPO/BENCHMARKS.md"
    MARKER="libzmq_comparison"

    # Build markdown table
    MD=""
    MD+=$'\n'
    MD+="| Size | omq msg/s | omq MB/s | zmq msg/s | zmq MB/s | ratio |"$'\n'
    MD+="|-------|-----------|----------|-----------|----------|-------|"$'\n'

    for i in "${!RESULTS_SIZES[@]}"; do
        sz=${RESULTS_SIZES[$i]}
        omsg=${RESULTS_OMQ_MSGS[$i]}
        omb=${RESULTS_OMQ_MB[$i]}
        zmsg=${RESULTS_ZMQ_MSGS[$i]}
        zmb=${RESULTS_ZMQ_MB[$i]}

        # human size label
        if   [ "$sz" -ge 131072 ]; then label="128 KiB"
        elif [ "$sz" -ge 32768  ]; then label="32 KiB"
        elif [ "$sz" -ge 8192   ]; then label="8 KiB"
        elif [ "$sz" -ge 2048   ]; then label="2 KiB"
        elif [ "$sz" -ge 512    ]; then label="512 B"
        else                            label="128 B"
        fi

        omq_fmt=$(fmt_msgs "$omsg")
        omq_bw=$(fmt_bw "$omb")
        zmq_fmt=$(fmt_msgs "$zmsg")
        zmq_bw=$(fmt_bw "$zmb")
        ratio=$(ratio_str "$omsg" "$zmsg")

        MD+="| $label | $omq_fmt | $omq_bw | $zmq_fmt | $zmq_bw | $ratio |"$'\n'
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
