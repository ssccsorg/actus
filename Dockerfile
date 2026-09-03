# ── Actus ─────────────────────────────────────────────────────────────
#
# Targets:
#   toolchain  system deps + Rust, the source tree, and a warm cargo
#              registry (cargo fetch is cached until Cargo.lock changes).
#   release    FROM toolchain: compiles the release binary.
#   test       FROM toolchain: adds python3, curl, and git for the
#              run.sh --test suite. The suite compiles and runs the Rust
#              tests (src unit tests plus the tests/*.rs integration
#              tests) and exercises the HTTP endpoints against a live
#              server, so pull requests never pay for a release build.
#   runtime    default target: the slim image published to ghcr. Holds
#              only the actus binary plus git (the /v1/git/* endpoints
#              spawn the git CLI). TLS comes from the bundled rustls/ring
#              stack; ca-certificates provides the root store.
#
# Actus delegates agent execution to the telos process, which is never
# bundled into any of these images; it is located via TELOS_BIN or the
# agent config, keeping the Apache-2.0 actus image free of GPL telos.

FROM ubuntu:24.04 AS toolchain

RUN apt-get update && apt-get install -y \
        build-essential \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /workspace

# Warm the registry before the sources are copied so the dependency
# download layer is only invalidated when Cargo.lock changes.
COPY Cargo.toml Cargo.lock ./
RUN cargo fetch --locked

COPY src/ src/
COPY tests/ tests/
COPY run.sh runner.py terminal.py ./

FROM toolchain AS release

RUN cargo build --release --locked

FROM toolchain AS test

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=release /workspace/target/release/actus /usr/local/bin/actus

ENTRYPOINT ["/usr/local/bin/actus"]
