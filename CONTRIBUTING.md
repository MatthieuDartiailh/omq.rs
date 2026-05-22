# Contributing to omq

## Getting started

```sh
git clone https://github.com/paddor/omq.rs.git
cd omq.rs
cargo build --workspace
cargo clippy --workspace --all-targets
```

Run the full test suite (covers both backends):

```sh
./scripts/test-all.sh
```

MSRV is Rust 1.93, edition 2024.

## Making changes

Both backends (`omq-compio` and `omq-tokio`) share the same public `Socket`
API. Changes to one usually need a matching change in the other.

New socket types, transports, and mechanisms must be added to both backends.
See the Architecture section below for where each piece lives.

Before committing:

```sh
cargo clippy --workspace --all-targets   # pedantic warnings are enabled
cargo fmt                                # rustfmt.toml: edition 2024, max_width 100
```

Feature-gated code (`curve`, `blake3zmq`, `lz4`, `zstd`, `ws`, `priority`)
must be tested with the relevant feature enabled.

## Pull requests

- Keep PRs focused. One feature or fix per PR.
- Include tests for new functionality.
- Update the relevant crate's `CHANGELOG.md` under the `[Unreleased]` section.
  Extend only; never modify existing versioned sections.

## Architecture

The codebase uses a three-layer split: a sans-I/O codec (`omq-proto`), and
two async I/O backends (`omq-compio`, `omq-tokio`).

- [`doc/architecture.md`](doc/architecture.md) -- diagrams, two-queue model,
  transport and mechanism tables
- [`doc/compio.md`](doc/compio.md) -- compio backend internals
- [`doc/tokio.md`](doc/tokio.md) -- tokio backend internals

## License

Contributions are licensed under ISC, the same license as the project.
