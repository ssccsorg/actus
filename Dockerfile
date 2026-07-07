FROM ubuntu:24.04 AS base

ENV DEBIAN_FRONTEND=noninteractive

# Rust toolchain
RUN apt-get update && apt-get install -y build-essential curl git python3 \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

# Build actus
WORKDIR /workspace
COPY Cargo.toml Cargo.lock* ./
COPY src/ src/
RUN cargo build --release && cargo test && ./target/release/actus --help >/dev/null

# ── Integration target (optional) ─────────────────────────────────────

FROM base AS full

# Python CLI scripts and test harness
COPY *.py ./
COPY run.sh ./
COPY helix/patch/ helix/patch/
COPY helix/build.sh helix/build.sh

# Full integration test: builds Zed from source if binary missing
RUN apt-get update && apt-get install -y libasound2-dev libwayland-dev libxkbcommon-dev \
    && rm -rf /var/lib/apt/lists/* \
    && ./run.sh --test

ENTRYPOINT ["./target/release/actus"]

# Default target: lean binary only (full integration test requires --target full)
FROM base
