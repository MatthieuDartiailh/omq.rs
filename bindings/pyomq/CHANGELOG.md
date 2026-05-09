# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1](https://github.com/paddor/omq.rs/compare/pyomq-v0.2.0...pyomq-v0.2.1) - 2026-05-09

### Changed

- *(deps)* replace `compio` git dep with `version = "0.19.0-rc.1"` and add
  `"time"` feature. Aligns with omq-compio's registry dep to avoid duplicate
  compio-runtime instances; `compio::time::sleep` was already used in the
  source but the feature was missing from Cargo.toml.

## [0.2.0](https://github.com/paddor/omq.rs/releases/tag/pyomq-v0.2.0) - 2026-05-04

### Added

- First PyPI release. Python binding for omq-compio (compio/io_uring backend).
  Linux x86_64 and aarch64 wheels; stable ABI covers Python 3.9+.
