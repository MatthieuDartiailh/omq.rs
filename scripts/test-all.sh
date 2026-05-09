#!/usr/bin/env bash
# Run every test in the workspace + bindings against every supported
# Cargo feature combination on both backends. Used as the "test
# everything" entry point. Exits non-zero on the first failing step.
#
# Knobs:
#   OMQ_SKIP_FUZZ=1     skip the ~1 M-iter hand-rolled fuzz suites
#   OMQ_SKIP_PYOMQ=1    skip the pyomq build + pytest pass
#   OMQ_TEST_RETRIES=N  retry each step up to N times (default 1) -
#                       a few inproc priority tests are sensitive to
#                       multi-thread scheduler timing on heavily loaded
#                       runners; one retry is usually enough.
set -euo pipefail

cd "$(dirname "$0")/.."

retries="${OMQ_TEST_RETRIES:-1}"

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

# ---------------------------------------------------------------- #
# 1) Default workspace: NULL mechanism + tcp/ipc/inproc/udp,
#    no compression, no priority. Smallest deploy shape.
#    No --workspace: uses default-members, which excludes the example
#    crates. zguide-compio and zguide-tokio depend on mutually
#    exclusive omq features and cannot be built in one invocation.
# ---------------------------------------------------------------- #
run cargo build --all-targets
run cargo clippy --all-targets --no-deps -- -D warnings
run cargo test


# ---------------------------------------------------------------- #
# 2) Each per-backend feature, full test suite for that backend.
#    Catches regressions in shared code that only surface under a
#    feature combination (e.g. priority swapping the routing
#    strategy alters the send-side data flow for every socket type,
#    not just the priority test file).
# ---------------------------------------------------------------- #
for feature in curve blake3zmq lz4 zstd priority; do
    run cargo test -p omq-proto  --features "$feature"
    run cargo test -p omq-tokio  --features "$feature"
    run cargo test -p omq-compio --features "$feature"
done

# ---------------------------------------------------------------- #
# 3) All non-fuzz features at once, full backend test suite. Catches
#    cross-feature interactions (e.g. CURVE + zstd + priority all
#    layered on the same connection).
# ---------------------------------------------------------------- #
all_features='curve blake3zmq lz4 zstd priority'
run cargo test -p omq-proto  --features "$all_features"
run cargo test -p omq-tokio  --features "$all_features"
run cargo test -p omq-compio --features "$all_features"

# ---------------------------------------------------------------- #
# 4) Facade crate, both backend choices. Verifies the public API
#    re-exports compile and the `BACKEND` const reflects the
#    selected backend.
# ---------------------------------------------------------------- #
run cargo test -p omq
run cargo test -p omq --no-default-features --features tokio-backend

# ---------------------------------------------------------------- #
# 5) Hand-rolled fuzz suites (~1 M iters each; slow). Skip with
#    `OMQ_SKIP_FUZZ=1` during fast iteration.
# ---------------------------------------------------------------- #
if [[ "${OMQ_SKIP_FUZZ:-}" != "1" ]]; then
    run cargo test -p omq-tokio  --features fuzz
    run cargo test -p omq-compio --features fuzz
fi

# ---------------------------------------------------------------- #
# 6) pyomq sync + asyncio + cross-impl interop. Built out-of-tree
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
