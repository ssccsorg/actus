# ── Zed builder (separate stage for cache isolation) ─────────────────

FROM ubuntu:24.04 AS zed-builder

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
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

# HTTP smoke test with /bin/true as fake Zed binary
RUN apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*
RUN set -x; \
    /workspace/target/release/actus \
        --bin /bin/true --server-only \
        --http-port 9090 --ws-port 9091 \
        --workdir /workspace & \
    ACTUS_PID=$!; \
    for i in $(seq 1 10); do \
        sleep 1; \
        curl -sf http://127.0.0.1:9090/health >/dev/null && break; \
    done; \
    curl -sf http://127.0.0.1:9090/health | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['status']=='ok'" && echo "  PASS health"; \
    curl -sf http://127.0.0.1:9090/v1/files?q=Cargo | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'count' in d" && echo "  PASS files"; \
    curl -sf http://127.0.0.1:9090/v1/git/status | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'branch' in d or 'files' in d or d.get('error')" && echo "  PASS git"; \
    curl -sf http://127.0.0.1:9090/v1/threads | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'threads' in d" && echo "  PASS threads"; \
    kill $ACTUS_PID 2>/dev/null || true; \
    wait $ACTUS_PID 2>/dev/null || true

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
