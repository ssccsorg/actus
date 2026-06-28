#!/usr/bin/env python3
"""
runner.py — Launches  Rust server and optionally the chat CLI.

Builds the Rust binary (incremental), starts the server, waits for it
to be ready, then either runs the terminal CLI or keeps running in
server-only mode.

Usage:
  ./runner.py                                    # server + CLI
  ./runner.py --server-only                      # server only
  ./runner.py --build-only                       # build Rust binary only
  ./runner.py --no-build                         # skip build (binary exists)

For production, run the Rust binary directly:
  ./target/debug/ --bin ... --server-only
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


# ── Paths ─────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent  # actus/
PROJECT_DIR = SCRIPT_DIR                        # actus/ = project root
ACTUS_BIN = PROJECT_DIR / "target" / "debug" / "actus"
ZED_BIN = SCRIPT_DIR / "helix" / ".bin" / "helix-zed-headless-arm64"
TERMINAL = SCRIPT_DIR / "terminal.py"


# ── Colors ────────────────────────────────────────────────────────────

class C:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    END = '\033[0m'


# ── Build ─────────────────────────────────────────────────────────────

def build_rust() -> Path:
    """Build actus Rust binary (incremental, quick if nothing changed)."""
    print("building actus...", end=" ", flush=True)
    subprocess.run(["cargo", "build", "-q"], cwd=PROJECT_DIR, check=True)
    print("ok")
    return ACTUS_BIN


def build_rust_cmd(bin_path: Path, args: argparse.Namespace) -> list[str]:
    """Build the Rust server command line."""
    cmd = [
        str(bin_path),
        "--workdir", args.workdir,
        "--http-port", str(args.http_port),
        "--ws-port", str(args.ws_port),
        "--server-only",
    ]
    if args.bin:
        cmd += ["--bin", args.bin]
    elif ZED_BIN.exists():
        cmd += ["--bin", str(ZED_BIN)]
    if args.api_key:
        cmd += ["--api-key", args.api_key]
    if args.provider:
        cmd += ["--provider", args.provider]
    if args.base_url:
        cmd += ["--base-url", args.base_url]
    return cmd


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=" runner: start server and optionally CLI")
    parser.add_argument("--workdir", default=os.getcwd(), help="Working directory")
    parser.add_argument("--http-port", type=int, default=int(os.environ.get("ACTUS_HTTP_PORT", "9090")), help="HTTP API port")
    parser.add_argument("--ws-port", type=int, default=int(os.environ.get("ACTUS_WS_PORT", "8080")), help="WebSocket port")
    parser.add_argument("--bin", help="Zed headless binary path")
    parser.add_argument("--api-key", help="LLM API key")
    parser.add_argument("--provider", default=os.environ.get("LLM_PROVIDER", ""), help="LLM provider")
    parser.add_argument("--base-url", default=os.environ.get("LLM_BASE_URL", ""), help="LLM base URL")
    parser.add_argument("--server-only", action="store_true", help="Server only, no CLI")
    parser.add_argument("--build-only", action="store_true", help="Build Rust binary only and exit")
    parser.add_argument("--no-build", action="store_true", help="Skip build (use existing binary)")
    args = parser.parse_args()

    if args.build_only:
        build_rust()
        return

    # Load .env
    env_file = SCRIPT_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

    # Resolve API key
    api_key = args.api_key or os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("LLM_API_KEY", "")
    if not api_key:
        print(f"{C.YELLOW}Warning: No API key set. Set DEEPSEEK_API_KEY or LLM_API_KEY.{C.END}")

    # Build Rust binary
    if not args.no_build:
        bin_path = build_rust()
    else:
        bin_path = ACTUS_BIN
        if not bin_path.exists():
            print(f"{C.RED}Binary not found: {bin_path}. Remove --no-build or build first.{C.END}")
            sys.exit(1)

    # Kill any stale server on our ports before starting
    import http.client as _hc
    for _port in [args.http_port, args.ws_port]:
        subprocess.run(
            ["lsof", "-ti", f":{_port}", "-sTCP:LISTEN"],
            capture_output=True, text=True, timeout=5
        )
        subprocess.run(["pkill", "-f", ""], capture_output=True, timeout=5)
    time.sleep(1)

    # Build and start Rust server
    rust_cmd = build_rust_cmd(bin_path, args)
    log_path = "/tmp/actus-server.log"
    log_file = open(log_path, "w")
    server_proc = subprocess.Popen(rust_cmd, cwd=args.workdir, stderr=log_file)
    print(f"server logs: {log_path}", file=sys.stderr)

    # Signal handler for clean shutdown
    def shutdown(signum, frame):
        print(f"\n{C.YELLOW}Shutting down...{C.END}", file=sys.stderr)
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_proc.kill()
            server_proc.wait()
        log_file.close()
        print(f"{C.GREEN}Done.{C.END}", file=sys.stderr)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    if args.server_only:
        try:
            server_proc.wait()
        except KeyboardInterrupt:
            shutdown(None, None)
    else:
        # Wait for server health
        import http.client as _hc
        import json as _j
        print(f"Waiting for server...", file=sys.stderr)
        for i in range(30):
            try:
                conn = _hc.HTTPConnection("127.0.0.1", args.http_port, timeout=2)
                conn.request("GET", "/health")
                r = conn.getresponse()
                if r.status == 200:
                    data = _j.loads(r.read())
                    if data.get("status") == "ok" and data.get("agent_ready"):
                        print(f"Server ready after {i+1}s", file=sys.stderr)
                        break
                conn.close()
            except Exception:
                pass
            time.sleep(1)

        # Run terminal CLI
        if TERMINAL.exists():
            term_env = os.environ.copy()
            term_env["TERMINAL_PORT"] = str(args.http_port)
            term_proc = subprocess.Popen(
                [sys.executable, str(TERMINAL), "--port", str(args.http_port)],
                stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr,
            )
            try:
                term_proc.wait()
            except KeyboardInterrupt:
                term_proc.kill()
                term_proc.wait()

        shutdown(None, None)


if __name__ == "__main__":
    main()
