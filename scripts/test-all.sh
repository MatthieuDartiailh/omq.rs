#!/usr/bin/env bash
# Run every test in the workspace + bindings against every supported
# Cargo feature combination. Used as the "test
# everything" entry point. Exits non-zero on the first failing step.
#
# Knobs:
#   OMQ_FUZZ=1          opt in to the ~1 M-iter hand-rolled fuzz suites
#   OMQ_SKIP_PYOMQ=1    skip the pyomq build + pytest pass
#   OMQ_TEST_RETRIES=N  retry each step up to N times (default 2) -
#                       a few timing-sensitive tests may need one
#                       retry on heavily loaded runners.
#   OMQ_TEST_JOBS=N     max parallel test steps (default 2)
#   OMQ_SKIP_PERF=1     skip the local perf smoke/hardware gate
#   OMQ_STRESS_ROUNDS=N connect-before-bind stress rounds (default 40)
set -euo pipefail

cd "$(dirname "$0")/.."


retries="${OMQ_TEST_RETRIES:-2}"
jobs="${OMQ_TEST_JOBS:-2}"

run() {
    echo "::: $*"
    local attempt=0
    while true; do
        attempt=$((attempt + 1))
        "$@" &
        local child=$!
        _foreground_pid=$child
        local rc=0
        wait "$child" || rc=$?
        _foreground_pid=0
        if [[ $rc -eq 0 ]]; then
            return 0
        fi
        if [[ $rc -eq 130 || $rc -eq 143 ]]; then
            return "$rc"
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
_par_count=0
_par_rc=0
_foreground_pid=0

_par_has_pids() {
    [[ $_par_count -gt 0 ]]
}

_kill_tree() {
    local pid=$1
    local child
    for child in $(pgrep -P "$pid" 2>/dev/null || true); do
        _kill_tree "$child"
    done
    kill -TERM "$pid" 2>/dev/null || true
}

_kill_workers() {
    if _par_has_pids; then
        for pid in "${_par_pids[@]}"; do
            _kill_tree "$pid"
        done
        for pid in "${_par_pids[@]}"; do
            wait "$pid" 2>/dev/null || true
        done
    fi
    _par_pids=()
    _par_count=0
}

_handle_interrupt() {
    trap - INT TERM
    if [[ $_foreground_pid -ne 0 ]]; then
        _kill_tree "$_foreground_pid"
    fi
    _kill_workers
    exit 130
}

trap _handle_interrupt INT TERM

par() {
    # Reap any finished workers.
    local new_pids=()
    local new_count=0
    if _par_has_pids; then
        for pid in "${_par_pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                new_pids+=("$pid")
                new_count=$((new_count + 1))
            else
                wait "$pid" || _par_rc=$?
            fi
        done
    fi
    if [[ $new_count -gt 0 ]]; then
        _par_pids=("${new_pids[@]}")
    else
        _par_pids=()
    fi
    _par_count=$new_count

    if [[ $_par_rc -ne 0 ]]; then
        _kill_workers
        exit "$_par_rc"
    fi

    # Block until a slot is free.
    while _par_has_pids && [[ ${#_par_pids[@]} -ge $jobs ]]; do
        wait "${_par_pids[0]}" || _par_rc=$?
        if [[ ${#_par_pids[@]} -gt 1 ]]; then
            _par_pids=("${_par_pids[@]:1}")
            _par_count=$((_par_count - 1))
        else
            _par_pids=()
            _par_count=0
        fi
        if [[ $_par_rc -ne 0 ]]; then
            _kill_workers
            exit "$_par_rc"
        fi
    done

    "$@" &
    _par_pids+=($!)
    _par_count=$((_par_count + 1))
}

par_wait() {
    if _par_has_pids; then
        for pid in "${_par_pids[@]}"; do
            wait "$pid" || {
                _par_rc=$?
                _kill_workers
                exit "$_par_rc"
            }
        done
    fi
    _par_pids=()
    _par_count=0
}

# ---------------------------------------------------------------- #
# 1) Default workspace: NULL mechanism + tcp/ipc/inproc/udp,
#    no compression. Smallest deploy shape.
#    No --workspace: uses default-members, which excludes the example
#    crates.
# ---------------------------------------------------------------- #
# Clippy compiles all targets; a separate all-target build only duplicates
# that work before the test suite.
run cargo clippy --all-targets --no-deps -- -D warnings
run cargo clippy -p omq-libzmq --all-targets --no-deps -- -D warnings
run cargo test
run cargo test -p omq-libzmq

if [[ -n "${CI:-}" || -n "${GITHUB_ACTIONS:-}" ]]; then
    echo "skip: perf gate disabled on CI"
elif [[ "${OMQ_SKIP_PERF:-}" == "1" ]]; then
    echo "skip: OMQ_SKIP_PERF=1"
else
    run cargo run --release -q -p omq-tokio --bin omq_perf_verify
fi

# ---------------------------------------------------------------- #
# 2) Feature-gated tests only. Step 1 already ran the full suite
#    with default features; mechanisms and compression transforms
#    only add handshake/transform code paths, so only the gated
#    test files need re-running. Step 3 (all features) catches
#    cross-feature interactions.
# ---------------------------------------------------------------- #
for feature in plain curve; do
    par run cargo test -p omq-tokio  --features "$feature" --test "$feature"
done
par run cargo test -p omq-tokio  --features lz4 --test lz4_tcp --test lz4_pub_sub
par run cargo test -p omq-tokio  --features plain --test interop_pyzmq_plain
par run cargo test -p omq-tokio  --features curve --test interop_pyzmq_curve
par_wait

# ---------------------------------------------------------------- #
# 3) All non-fuzz features at once, full backend test suite. Catches
#    cross-feature interactions and internal #[cfg(feature)] items
#    inside otherwise-ungated test files (connect_before_bind lz4).
# ---------------------------------------------------------------- #
all_features='plain curve lz4'
par run cargo test -p omq-proto  --features "$all_features"
par run cargo test -p omq-tokio  --features "$all_features"
par_wait

# ---------------------------------------------------------------- #
# 4) Hand-rolled fuzz suites (~1 M iters each; slow). Opt in with
#    `OMQ_FUZZ=1`.
# ---------------------------------------------------------------- #
if [[ "${OMQ_FUZZ:-}" == "1" ]]; then
    par run cargo test -p omq-tokio  --features fuzz --release
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
    # The checked-in venv may have been copied from another worktree; invoke
    # pytest through the active interpreter so its path cannot escape here.
    run python -m pytest -v
    deactivate
    popd >/dev/null
else
    echo "skip: bindings/pyomq/.venv not set up; see bindings/pyomq/README.md"
fi

echo "all tests passed"
