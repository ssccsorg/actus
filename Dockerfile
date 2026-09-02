# ── Actus ─────────────────────────────────────────────────────────────

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y build-essential curl git python3 \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /workspace
COPY Cargo.toml Cargo.lock* ./
COPY src/ src/
COPY *.py run.sh ./
RUN cargo build --release

ENTRYPOINT ["./target/release/actus"]
