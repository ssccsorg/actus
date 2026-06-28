# 

Helix remote_server (headless Zed) Unix socket client.

Spawns a headless Zed instance and connects to its Unix sockets,
providing an interactive coding prompt without GUI.

## Usage

```bash
# Using pre-built binary from .bin/
 --bin .bin/helix-remote-server-arm64 --workdir /path/to/repo

# Connecting to existing remote_server
 --stdin-sock /tmp/hl-stdin.sock --stdout-sock /tmp/hl-stdout.sock

# With default socket paths

```

## Build Helix remote_server

```bash
cd /path/to/helix
cargo build -p remote_server --release
cp target/release/remote_server /path/to/ssccs-nexus/.bin/helix-remote-server-arm64
```
