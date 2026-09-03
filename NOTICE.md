# Licensing Notice

## Actus

The actus source code (`src/`, `*.py`, `run.sh`, `Cargo.toml`) is licensed
under the Apache License 2.0 (see `LICENSE`).

Copyright (c) 2026 SSCCS Foundation.

## Agent processes

Actus executes agent processes that are built and distributed from their
own repositories. This repository vendors no agent source code.

- `telos` (the default ACP/WebSocket agent adapter) is a separate project
  that contains a Zed-derived core. When it is distributed, that component
  is subject to the GNU General Public License version 3, and its source is
  published by the telos project. Actus connects to it over a local
  WebSocket only; actus code is not linked with it.
- Other adapters, including the deterministic in-process `native`
  reference adapter, run without any external agent binary.

## Third-party code

Dependencies are declared in `Cargo.toml` and resolved by the normal Rust
toolchain; their respective licenses apply.
