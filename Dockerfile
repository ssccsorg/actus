FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# System dependencies for Rust and Python
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Rust toolchain
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustc --version

WORKDIR /workspace

# Copy source (Cargo.lock may not exist in all clones)
COPY Cargo.toml ./
COPY Cargo.lock* ./
COPY src/ src/

# Build actus binary (no external runtime dependencies)
RUN cargo build --release && cargo test

# Smoke test: file search unit test
RUN ./target/release/actus --help >/dev/null

ENTRYPOINT ["./target/release/actus"]
