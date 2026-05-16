#!/usr/bin/env bash
# Compare zmq.rs vs omq-tokio vs omq-compio: single PUSH process -> single PULL
# process. Each cell: 3 s timed window after 500 ms warmup.
#
# zmq.rs (crate: zeromq) is a pure-Rust async ZMQ implementation on tokio,
# making the omq-tokio comparison apples-to-apples. omq-compio runs on a
# single io_uring thread for contrast.
#
# By default runs ipc and tcp in order. zeromq 0.6 does not support inproc.
#
# IPC: omq peers use Linux abstract-namespace sockets (ipc://@name).
# zmq.rs does not support abstract namespaces and falls back to a socket
# file (/tmp/omq-bench-zmqrs-z-N.sock), which is cleaned up after each run.
#
# Usage:
#   ./scripts/compare_zmqrs.sh                     # ipc + tcp
#   ./scripts/compare_zmqrs.sh --ipc               # IPC only
#   ./scripts/compare_zmqrs.sh --tcp               # TCP only
#   ./scripts/compare_zmqrs.sh --update-benchmarks # update COMPARISONS.md
#   ./scripts/compare_zmqrs.sh --ipc --update-benchmarks
#   ./scripts/compare_zmqrs.sh [port]              # override base TCP port

set -euo pipefail

cleanup() {
    trap - INT TERM EXIT
    kill 0 2>/dev/null || true
    wait 2>/dev/null || true
    rm -f /tmp/omq-bench-zmqrs-z-*.sock
}
trap cleanup INT TERM EXIT

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$SCRIPT_DIR/.."
DURATION=3
BASE_PORT=15655
UPDATE_BENCHMARKS=false
TRANSPORT_FILTER=""

for arg in "$@"; do
    case "$arg" in
        --update-benchmarks) UPDATE_BENCHMARKS=true ;;
        --ipc) TRANSPORT_FILTER=ipc ;;
        --tcp) TRANSPORT_FILTER=tcp ;;
        -h|--help)
            echo "Usage: $0 [--ipc] [--tcp] [--update-benchmarks] [port]"
            echo "  --ipc               IPC only"
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
    TRANSPORTS=(ipc tcp)
fi

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

echo "==> building omq-zeromq bench_peer..."
cargo build --release -p omq-zeromq --bin bench_peer_zeromq -q
ZEROMQ_PEER="$REPO/target/release/bench_peer_zeromq"

# ---------- helpers ----------

# addr_for <transport> <peer_prefix> <idx>
#   peer_prefix: z=zmq.rs  t=omq-tokio  c=omq-compio
#
# zmq.rs IPC uses filesystem sockets (no abstract-namespace support).
# omq peers use abstract-namespace sockets.
addr_for() {
    local transport="$1" prefix="$2" idx="$3"
    case "$transport" in
        tcp)
            local base
            case "$prefix" in
                z) base=$BASE_PORT ;;
                t) base=$((BASE_PORT + 100)) ;;
                c) base=$((BASE_PORT + 200)) ;;
                q) base=$((BASE_PORT + 300)) ;;
                *) base=$BASE_PORT ;;
            esac
            echo "$((base + idx))" ;;
        ipc)
            case "$prefix" in
                z) echo "ipc:///tmp/omq-bench-zmqrs-z-${idx}.sock" ;;
                q) echo "ipc://@omq-bench-zmqrs-q-${idx}" ;;
                *) echo "ipc://@omq-bench-zmqrs-${prefix}-${idx}" ;;
            esac ;;
    esac
}

# run_cell <transport> <peer_binary> <addr> <size>
run_cell() {
    local transport="$1" peer="$2" addr="$3" size="$4"

    if [ "$transport" = "inproc" ]; then
        "$peer" inproc "$addr" "$size" "$DURATION"
        return
    fi

    # zmq.rs does not unlink stale IPC socket files before bind.
    case "$addr" in
        ipc:///tmp/*) rm -f "${addr#ipc://}" ;;
    esac

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
        if (v >= 1e6)       printf "%.2fM", v/1e6
        else if (v >= 10e3) printf "%.0fk", v/1e3
        else if (v >= 1e3)  printf "%.1fk", v/1e3
        else                printf "%.0f", v
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
BENCHMARKS="$REPO/COMPARISONS.md"

run_comparison() {
    local transport="$1"
    local marker="zmqrs_comparison_${transport}"

    local transport_label
    case "$transport" in
        ipc) transport_label="IPC (zmq.rs: socket file; omq: abstract namespace)" ;;
        tcp) transport_label="TCP" ;;
    esac

    echo ""
    echo "zmq.rs (zeromq $ZMQRS_VERSION) vs omq $OMQ_VERSION — ${transport_label}, ${DURATION}s window + 500ms warmup"
    echo ""
    printf "%-10s  %20s  %22s  %22s  %22s\n" "" "zmq.rs" "omq-compio" "omq-tokio" "omq-zeromq"
    printf "%-10s  %20s  %22s  %22s  %22s\n" "msg size" "(msg/s  |  MB/s)" "(msg/s  |  MB/s  | x)" "(msg/s  |  MB/s  | x)" "(msg/s  |  MB/s  | x)"
    echo "------------------------------------------------------------------------------------------------------------------------------"

    local -a res_sizes res_zmqrs_msgs res_zmqrs_mb res_tokio_msgs res_tokio_mb res_compio_msgs res_compio_mb res_zeromq_msgs res_zeromq_mb
    local idx=0

    for size in "${SIZES[@]}"; do
        local addr_z addr_t addr_c addr_q
        addr_z=$(addr_for "$transport" "z" "$idx")
        addr_t=$(addr_for "$transport" "t" "$idx")
        addr_c=$(addr_for "$transport" "c" "$idx")
        addr_q=$(addr_for "$transport" "q" "$idx")

        local zmqrs_raw tokio_raw compio_raw zeromq_raw
        zmqrs_raw=$(run_cell  "$transport" "$ZMQRS_PEER"  "$addr_z" "$size")
        tokio_raw=$(run_cell  "$transport" "$TOKIO_PEER"  "$addr_t" "$size")
        compio_raw=$(run_cell "$transport" "$COMPIO_PEER" "$addr_c" "$size")
        zeromq_raw=$(run_cell "$transport" "$ZEROMQ_PEER" "$addr_q" "$size")

        local zmqrs_msgs zmqrs_mb tokio_msgs tokio_mb compio_msgs compio_mb zeromq_msgs zeromq_mb
        zmqrs_msgs=$(echo  "$zmqrs_raw"  | awk '{printf "%.0f", $1/$2}')
        zmqrs_mb=$(echo    "$zmqrs_raw"  | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
        tokio_msgs=$(echo  "$tokio_raw"  | awk '{printf "%.0f", $1/$2}')
        tokio_mb=$(echo    "$tokio_raw"  | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
        compio_msgs=$(echo "$compio_raw" | awk '{printf "%.0f", $1/$2}')
        compio_mb=$(echo   "$compio_raw" | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
        zeromq_msgs=$(echo "$zeromq_raw" | awk '{printf "%.0f", $1/$2}')
        zeromq_mb=$(echo   "$zeromq_raw" | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')

        local tokio_x compio_x zeromq_x
        tokio_x=$(speedup_str  "$tokio_msgs"  "$zmqrs_msgs")
        compio_x=$(speedup_str "$compio_msgs" "$zmqrs_msgs")
        zeromq_x=$(speedup_str "$zeromq_msgs" "$zmqrs_msgs")

        printf "  %7s    %9s msg/s  %6s MB/s    %9s msg/s  %6s MB/s  %6s    %9s msg/s  %6s MB/s  %6s    %9s msg/s  %6s MB/s  %6s\n" \
            "$(fmt_size "$size")" \
            "$zmqrs_msgs"  "$zmqrs_mb" \
            "$compio_msgs" "$compio_mb" "$compio_x" \
            "$tokio_msgs"  "$tokio_mb"  "$tokio_x" \
            "$zeromq_msgs" "$zeromq_mb" "$zeromq_x"

        res_sizes[$idx]=$size
        res_zmqrs_msgs[$idx]=$zmqrs_msgs;   res_zmqrs_mb[$idx]=$zmqrs_mb
        res_tokio_msgs[$idx]=$tokio_msgs;   res_tokio_mb[$idx]=$tokio_mb
        res_compio_msgs[$idx]=$compio_msgs; res_compio_mb[$idx]=$compio_mb
        res_zeromq_msgs[$idx]=$zeromq_msgs; res_zeromq_mb[$idx]=$zeromq_mb
        idx=$((idx + 1))
    done

    echo ""

    if [ "$UPDATE_BENCHMARKS" = true ]; then
        local md=$'\n'
        md+="| Size | zmq.rs msg/s | zmq.rs MB/s | omq-compio msg/s | omq-compio MB/s | compio × | omq-tokio msg/s | omq-tokio MB/s | tokio × | omq-zeromq msg/s | omq-zeromq MB/s | zeromq × |"$'\n'
        md+="|-------|-------------|------------|-----------------|----------------|---------|----------------|---------------|---------|-----------------|----------------|---------|"$'\n'

        for i in "${!res_sizes[@]}"; do
            local sz zmsg zmb tmsg tmb cmsg cmb qmsg qmb
            sz=${res_sizes[$i]}
            zmsg=${res_zmqrs_msgs[$i]};  zmb=${res_zmqrs_mb[$i]}
            tmsg=${res_tokio_msgs[$i]};  tmb=${res_tokio_mb[$i]}
            cmsg=${res_compio_msgs[$i]}; cmb=${res_compio_mb[$i]}
            qmsg=${res_zeromq_msgs[$i]}; qmb=${res_zeromq_mb[$i]}

            local label zmqrs_fmt zmqrs_bw tokio_fmt tokio_bw compio_fmt compio_bw zeromq_fmt zeromq_bw tokio_r compio_r zeromq_r
            label=$(fmt_size "$sz")
            zmqrs_fmt=$(fmt_msgs "$zmsg");  zmqrs_bw=$(fmt_bw "$zmb")
            tokio_fmt=$(fmt_msgs "$tmsg");  tokio_bw=$(fmt_bw "$tmb")
            compio_fmt=$(fmt_msgs "$cmsg"); compio_bw=$(fmt_bw "$cmb")
            zeromq_fmt=$(fmt_msgs "$qmsg"); zeromq_bw=$(fmt_bw "$qmb")
            tokio_r=$(speedup_str  "$tmsg" "$zmsg")
            compio_r=$(speedup_str "$cmsg" "$zmsg")
            zeromq_r=$(speedup_str "$qmsg" "$zmsg")

            md+="| $label | $zmqrs_fmt | $zmqrs_bw | $compio_fmt | $compio_bw | $compio_r | $tokio_fmt | $tokio_bw | $tokio_r | $zeromq_fmt | $zeromq_bw | $zeromq_r |"$'\n'
        done
        md+=$'\n'

        update_section "$BENCHMARKS" "$marker" "$md"
    fi
}

for transport in "${TRANSPORTS[@]}"; do
    run_comparison "$transport"
done
