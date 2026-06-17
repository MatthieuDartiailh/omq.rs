#!/usr/bin/env bash
# Run every test in the workspace + bindings against every supported
# Cargo feature combination on both backends. Used as the "test
# everything" entry point. Exits non-zero on the first failing step.
#
# Knobs:
#   OMQ_FUZZ=1          opt in to the ~1 M-iter hand-rolled fuzz suites
#   OMQ_SKIP_PYOMQ=1    skip the pyomq build + pytest pass
#   OMQ_TEST_RETRIES=N  retry each step up to N times (default 1) -
#                       a few timing-sensitive tests may need one
#                       retry on heavily loaded runners.
#   OMQ_TEST_JOBS=N     max parallel test steps (default 4)
set -euo pipefail

cd "$(dirname "$0")/.."

retries="${OMQ_TEST_RETRIES:-1}"
jobs="${OMQ_TEST_JOBS:-4}"

run() {
    echo "::: $*"
    local attempt=0
    while true; do
        attempt=$((attempt + 1))
        if "$@"; then
            return 0
        fi
        if [[ $attempt -ge $retries ]]; then
            echo "::: FAILED after $attempt attempt(s): $*" >&2
            return 1
        fi
        echo "::: retry $attempt/$retries: $*" >&2
    done
}

# Run a function in the background, keeping at most $jobs parallel workers.
# Usage: par <func> [args...]
_par_pids=()
_par_rc=0

_kill_workers() {
    for pid in "${_par_pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${_par_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    _par_pids=()
}

par() {
    # Reap any finished workers.
    local new_pids=()
    for pid in "${_par_pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            new_pids+=("$pid")
        else
            wait "$pid" || _par_rc=$?
        fi
    done
    _par_pids=("${new_pids[@]}")

    if [[ $_par_rc -ne 0 ]]; then
        _kill_workers
        exit "$_par_rc"
    fi

    # Block until a slot is free.
    while [[ ${#_par_pids[@]} -ge $jobs ]]; do
        wait "${_par_pids[0]}" || _par_rc=$?
        _par_pids=("${_par_pids[@]:1}")
        if [[ $_par_rc -ne 0 ]]; then
            _kill_workers
            exit "$_par_rc"
        fi
    done

    "$@" &
    _par_pids+=($!)
}

par_wait() {
    for pid in "${_par_pids[@]}"; do
        wait "$pid" || {
            _par_rc=$?
            _kill_workers
            exit "$_par_rc"
        }
    done
    _par_pids=()
}

# ---------------------------------------------------------------- #
# 1) Default workspace: NULL mechanism + tcp/ipc/inproc/udp,
#    no compression. Smallest deploy shape.
#    No --workspace: uses default-members, which excludes the example
#    crates. zguide-compio and zguide-tokio depend on mutually
#    exclusive omq features and cannot be built in one invocation.
# ---------------------------------------------------------------- #
run cargo build --all-targets
run cargo clippy --all-targets --no-deps -- -D warnings
run cargo test


# ---------------------------------------------------------------- #
# 2) Feature-gated tests only. Step 1 already ran the full suite
#    with default features; mechanisms and compression transforms
#    only add handshake/transform code paths, so only the gated
#    test files need re-running. Step 3 (all features) catches
#    cross-feature interactions.
# ---------------------------------------------------------------- #
for feature in plain curve blake3zmq; do
    par run cargo test -p omq-tokio  --features "$feature" --test "$feature"
    par run cargo test -p omq-compio --features "$feature" --test "$feature"
done
par run cargo test -p omq-tokio  --features lz4 --test lz4_tcp --test lz4_pub_sub
par run cargo test -p omq-compio --features lz4 --test lz4_tcp
par run cargo test -p omq-tokio  --features plain --test interop_pyzmq_plain
par run cargo test -p omq-tokio  --features curve --test interop_pyzmq_curve
par run cargo test -p omq-compio --features curve --test interop_pyzmq_curve
par run cargo test -p omq-interop-tests --test tcp
par run cargo test -p omq-interop-tests --test ws --features ws
par_wait

# ---------------------------------------------------------------- #
# 3) All non-fuzz features at once, full backend test suite. Catches
#    cross-feature interactions and internal #[cfg(feature)] items
#    inside otherwise-ungated test files (connect_before_bind lz4).
# ---------------------------------------------------------------- #
all_features='plain curve blake3zmq lz4'
par run cargo test -p omq-proto  --features "$all_features"
par run cargo test -p omq-tokio  --features "$all_features"
par run cargo test -p omq-compio --features "$all_features"
par_wait

# ---------------------------------------------------------------- #
# 4) Hand-rolled fuzz suites (~1 M iters each; slow). Opt in with
#    `OMQ_FUZZ=1`.
# ---------------------------------------------------------------- #
if [[ "${OMQ_FUZZ:-}" == "1" ]]; then
    par run cargo test -p omq-tokio  --features fuzz --release
    par run cargo test -p omq-compio --features fuzz --release
    par_wait
fi

# ---------------------------------------------------------------- #
# 5) pyomq sync + asyncio + cross-impl interop. Built out-of-tree
#    (its own Cargo.lock + maturin); skip when the dev venv isn't
#    set up. `OMQ_SKIP_PYOMQ=1` overrides.
# ---------------------------------------------------------------- #
if [[ "${OMQ_SKIP_PYOMQ:-}" == "1" ]]; then
    echo "skip: OMQ_SKIP_PYOMQ=1"
elif [[ -d bindings/pyomq/.venv ]]; then
    pushd bindings/pyomq >/dev/null
    # shellcheck disable=SC1091
    source .venv/bin/activate
    run maturin develop --release
    run pytest -v
    deactivate
    popd >/dev/null
else
    echo "skip: bindings/pyomq/.venv not set up; see bindings/pyomq/README.md"
fi

echo "all tests passed"
