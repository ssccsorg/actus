# ── Zed builder (separate stage for cache isolation) ─────────────────

FROM ubuntu:24.04 AS zed-builder

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    pkg-config \
    libx11-dev \
    libxkbcommon-dev \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace
COPY helix/build.sh helix/patch/ ./helix/
RUN mkdir -p helix/.bin && bash helix/build.sh --build-only --release

# ── Actus base ────────────────────────────────────────────────────────

FROM ubuntu:24.04 AS base

RUN apt-get update && apt-get install -y build-essential curl git python3 \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace
COPY Cargo.toml Cargo.lock* ./
COPY src/ src/
RUN cargo build --release && cargo test && ./target/release/actus --help >/dev/null

# ── Full integration ─────────────────────────────────────────────────

FROM base AS full

COPY --from=zed-builder /workspace/helix/.bin/ helix/.bin/
COPY *.py run.sh ./
RUN apt-get update && apt-get install -y libasound2-dev libwayland-dev libxkbcommon-dev \
    && rm -rf /var/lib/apt/lists/* \
    && ./run.sh --test

ENTRYPOINT ["./target/release/actus"]

# Default target: lean binary only
FROM base
