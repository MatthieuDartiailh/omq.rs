# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-17

### Added

- Bounded SPSC ring with ypipe-style batched flush/prefetch.
- `Producer`: zero-atomic `push`, single-Release `flush`.
- `Consumer`: zero-atomic `pop`, single-Acquire `prefetch`.
- `FlushResult` with `was_empty` flag for waker integration.
