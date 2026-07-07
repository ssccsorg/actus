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

# Expects a prebuilt linux arm64 binary at this path.
# Build with: docker build --target full --build-arg ZED_BIN=./path/to/binary .
ARG ZED_BIN
COPY $ZED_BIN helix/.bin/helix-zed-headless-arm64

# Python CLI scripts
COPY *.py ./
COPY run.sh ./

RUN apt-get update && apt-get install -y libasound2-dev libwayland-dev libxkbcommon-dev \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m py_compile *.py \
    && bash -n run.sh \
    && ./run.sh --test

ENTRYPOINT ["./target/release/actus"]

# Default target: lean binary only (full integration test requires --target full)
FROM base
