FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# System dependencies for Rust, Python, audio (Zed dependency)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    python3 \
    libasound2-dev \
    libwayland-dev \
    libxkbcommon-dev \
    && rm -rf /var/lib/apt/lists/*

# Rust toolchain
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustc --version

WORKDIR /workspace

# Copy source
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY helix/ helix/
COPY *.py ./
COPY run.sh ./

# Build actus binary
RUN cargo build --release

# Smoke test: static checks only (no Zed binary needed)
RUN cargo test

# Smoke test: run.sh --test requires a Zed headless binary.
# Build with: docker build --build-arg ZED_BINARY_URL=<url> .
# Or mount the binary at build time:
#   cp /path/to/helix-zed-headless-arm64 helix/.bin/ && docker build .
ARG ZED_BINARY_URL
RUN if [ -n "$ZED_BINARY_URL" ]; then \
        mkdir -p helix/.bin && \
        curl -sL "$ZED_BINARY_URL" -o helix/.bin/helix-zed-headless-arm64 && \
        chmod +x helix/.bin/helix-zed-headless-arm64; \
    fi

ENTRYPOINT ["./target/release/actus"]
