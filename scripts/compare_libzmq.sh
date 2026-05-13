#!/usr/bin/env bash
# Compare omq-compio + omq-tokio vs libzmq: single PUSH process -> single PULL
# process. Each cell: 3 s timed window after 500 ms warmup.
#
# By default runs inproc, ipc, and tcp in order. Pass a transport flag to
# limit to one transport.
#
# IPC uses Linux abstract-namespace sockets (ipc://@name); no socket files
# are created.
#
# Inproc requires both sockets in the same process, so each peer binary
# runs its own push+pull internally (bench_peer inproc / libzmq_bench_peer
# inproc).
#
# Usage:
#   ./scripts/compare_libzmq.sh                          # all transports
#   ./scripts/compare_libzmq.sh --inproc                 # inproc only
#   ./scripts/compare_libzmq.sh --ipc                    # IPC only
#   ./scripts/compare_libzmq.sh --tcp                    # TCP only
#   ./scripts/compare_libzmq.sh --update-benchmarks      # update COMPARISONS.md
#   ./scripts/compare_libzmq.sh --tcp --update-benchmarks
#   ./scripts/compare_libzmq.sh [port]                   # override base TCP port

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
TRANSPORT_FILTER=""

for arg in "$@"; do
    case "$arg" in
        --update-benchmarks) UPDATE_BENCHMARKS=true ;;
        --inproc) TRANSPORT_FILTER=inproc ;;
        --ipc)    TRANSPORT_FILTER=ipc ;;
        --tcp)    TRANSPORT_FILTER=tcp ;;
        -h|--help)
            echo "Usage: $0 [--inproc] [--ipc] [--tcp] [--update-benchmarks] [port]"
            echo "  --inproc            inproc only"
            echo "  --ipc               IPC only (abstract-namespace Unix socket)"
            echo "  --tcp               TCP only"
            echo "  --update-benchmarks update COMPARISONS.md"
            echo "  port                override base TCP port (default $BASE_PORT)"
            exit 0 ;;
        [0-9]*) BASE_PORT="$arg" ;;
    esac
done

if [ -n "$TRANSPORT_FILTER" ]; then
    TRANSPORTS=("$TRANSPORT_FILTER")
else
    TRANSPORTS=(inproc ipc tcp)
fi

# ---------- build ----------

echo "==> building omq-compio bench_peer..."
cargo build --release -p omq-compio --bin bench_peer -q
OMQ_PEER="$REPO/target/release/bench_peer"

echo "==> building omq-tokio bench_peer..."
cargo build --release -p omq-tokio --bin bench_peer_tokio -q
TOKIO_PEER="$REPO/target/release/bench_peer_tokio"

echo "==> building libzmq bench_peer..."
gcc -O2 -o "$SCRIPT_DIR/libzmq_bench_peer" \
    "$SCRIPT_DIR/libzmq_bench_peer.c" -lzmq -lpthread
LIBZMQ_PEER="$SCRIPT_DIR/libzmq_bench_peer"

# ---------- helpers ----------

# addr_for <transport> <peer_prefix> <idx>
#   peer_prefix: o=omq-compio  t=omq-tokio  z=libzmq
addr_for() {
    local transport="$1" prefix="$2" idx="$3"
    case "$transport" in
        tcp)
            local base
            case "$prefix" in
                o) base=$BASE_PORT ;;
                t) base=$((BASE_PORT + 100)) ;;
                z) base=$((BASE_PORT + 200)) ;;
                *) base=$BASE_PORT ;;
            esac
            echo "$((base + idx))" ;;
        ipc)
            echo "ipc://@omq-bench-lzq-${prefix}-${idx}" ;;
        inproc)
            echo "bench-lzq-${idx}" ;;
    esac
}

# run_cell <transport> <peer_binary> <addr_or_name> <size>
run_cell() {
    local transport="$1" peer="$2" addr="$3" size="$4"

    if [ "$transport" = "inproc" ]; then
        "$peer" inproc "$addr" "$size" "$DURATION"
        return
    fi

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

ratio_str() {
    awk -v o="$1" -v z="$2" 'BEGIN {
        r = o/z
        if (r >= 1.1) printf "**%.1f×**", r
        else          printf "%.2f×", r
    }'
}

update_section() {
    local benchmarks="$1" marker="$2" md="$3"
    local begin_marker="<!-- BEGIN $marker -->"
    local end_marker="<!-- END $marker -->"
    if ! grep -q "$begin_marker" "$benchmarks"; then
        echo "ERROR: marker '$begin_marker' not found in $benchmarks" >&2
        exit 1
    fi
    python3 - "$benchmarks" "$begin_marker" "$end_marker" "$md" <<'EOF'
import sys, re
path, begin, end, content = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
text = open(path).read()
new = re.sub(re.escape(begin) + r'.*?' + re.escape(end), begin + content + end, text, flags=re.DOTALL)
open(path, 'w').write(new)
print(f"Updated {path}")
EOF
}

# ---------- versions ----------

OMQ_VERSION=$(cargo metadata --no-deps --format-version 1 2>/dev/null \
    | python3 -c 'import sys,json; pkgs=json.load(sys.stdin)["packages"]; \
      print(next(p["version"] for p in pkgs if p["name"]=="omq-compio"))' \
    2>/dev/null || echo '?')
ZMQ_VERSION=$(pkg-config --modversion libzmq 2>/dev/null || echo '?')

# ---------- run ----------

SIZES=(8 32 128 512 2048 8192 32768 131072 524288 2097152 8388608 33554432)
BENCHMARKS="$REPO/COMPARISONS.md"

run_comparison() {
    local transport="$1"
    local marker="libzmq_comparison_${transport}"

    local transport_label
    case "$transport" in
        inproc) transport_label="inproc (same process)" ;;
        ipc)    transport_label="IPC (abstract namespace)" ;;
        tcp)    transport_label="TCP" ;;
    esac

    echo ""
    echo "omq $OMQ_VERSION vs libzmq $ZMQ_VERSION — ${transport_label}, ${DURATION}s window + 500ms warmup"
    echo ""
    printf "%-10s  %20s  %22s  %22s\n" "" "libzmq" "omq-compio" "omq-tokio"
    printf "%-10s  %20s  %22s  %22s\n" "msg size" "(msg/s  |  MB/s)" "(msg/s  |  MB/s  | x)" "(msg/s  |  MB/s  | x)"
    echo "-----------------------------------------------------------------------------------------------------------"

    local -a res_sizes res_omq_msgs res_omq_mb res_tokio_msgs res_tokio_mb res_zmq_msgs res_zmq_mb
    local idx=0

    for size in "${SIZES[@]}"; do
        local addr_o addr_t addr_z
        addr_o=$(addr_for "$transport" "o" "$idx")
        addr_t=$(addr_for "$transport" "t" "$idx")
        addr_z=$(addr_for "$transport" "z" "$idx")

        local omq_raw tokio_raw lzq_raw
        omq_raw=$(run_cell   "$transport" "$OMQ_PEER"    "$addr_o" "$size")
        tokio_raw=$(run_cell "$transport" "$TOKIO_PEER"  "$addr_t" "$size")
        lzq_raw=$(run_cell   "$transport" "$LIBZMQ_PEER" "$addr_z" "$size")

        local omq_msgs omq_mb tokio_msgs tokio_mb lzq_msgs lzq_mb
        omq_msgs=$(echo   "$omq_raw"   | awk '{printf "%.0f", $1/$2}')
        omq_mb=$(echo     "$omq_raw"   | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
        tokio_msgs=$(echo "$tokio_raw" | awk '{printf "%.0f", $1/$2}')
        tokio_mb=$(echo   "$tokio_raw" | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
        lzq_msgs=$(echo   "$lzq_raw"   | awk '{printf "%.0f", $1/$2}')
        lzq_mb=$(echo     "$lzq_raw"   | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')

        local omq_ratio tokio_ratio
        omq_ratio=$(ratio_str   "$omq_msgs"   "$lzq_msgs")
        tokio_ratio=$(ratio_str "$tokio_msgs" "$lzq_msgs")

        printf "  %7s    %9s msg/s  %6s MB/s    %9s msg/s  %6s MB/s  %6s    %9s msg/s  %6s MB/s  %6s\n" \
            "$(fmt_size "$size")" \
            "$lzq_msgs"   "$lzq_mb" \
            "$omq_msgs"   "$omq_mb"   "$omq_ratio" \
            "$tokio_msgs" "$tokio_mb" "$tokio_ratio"

        res_sizes[$idx]=$size
        res_omq_msgs[$idx]=$omq_msgs;   res_omq_mb[$idx]=$omq_mb
        res_tokio_msgs[$idx]=$tokio_msgs; res_tokio_mb[$idx]=$tokio_mb
        res_zmq_msgs[$idx]=$lzq_msgs;   res_zmq_mb[$idx]=$lzq_mb
        idx=$((idx + 1))
    done

    echo ""

    if [ "$UPDATE_BENCHMARKS" = true ]; then
        local md=$'\n'
        md+="| Size | libzmq msg/s | libzmq MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × |"$'\n'
        md+="|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|"$'\n'

        for i in "${!res_sizes[@]}"; do
            local sz omsg omb tmsg tmb zmsg zmb
            sz=${res_sizes[$i]}
            omsg=${res_omq_msgs[$i]};  omb=${res_omq_mb[$i]}
            tmsg=${res_tokio_msgs[$i]}; tmb=${res_tokio_mb[$i]}
            zmsg=${res_zmq_msgs[$i]};  zmb=${res_zmq_mb[$i]}

            local label zmq_fmt zmq_bw omq_fmt omq_bw tokio_fmt tokio_bw omq_r tokio_r
            label=$(fmt_size "$sz")
            zmq_fmt=$(fmt_msgs "$zmsg");   zmq_bw=$(fmt_bw "$zmb")
            omq_fmt=$(fmt_msgs "$omsg");   omq_bw=$(fmt_bw "$omb")
            tokio_fmt=$(fmt_msgs "$tmsg"); tokio_bw=$(fmt_bw "$tmb")
            omq_r=$(ratio_str   "$omsg" "$zmsg")
            tokio_r=$(ratio_str "$tmsg" "$zmsg")

            md+="| $label | $zmq_fmt | $zmq_bw | $omq_fmt | $omq_bw | $omq_r | $tokio_fmt | $tokio_bw | $tokio_r |"$'\n'
        done
        md+=$'\n'

        update_section "$BENCHMARKS" "$marker" "$md"
    fi
}

for transport in "${TRANSPORTS[@]}"; do
    run_comparison "$transport"
done
