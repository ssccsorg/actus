#!/usr/bin/env bash
# run.sh —  launcher + test suite
#
# Usage:
#   run.sh                  # default: start server + CLI
#   run.sh --test           # run all tests
#   run.sh --server-only    # start server only (no CLI)
#   run.sh --cli            # CLI only (connect to existing server)
#   run.sh --help           # show help
#
# Environment:
#   LLM_PROVIDER  LLM_BASE_URL  LLM_MODEL  LLM_API_KEY

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
RUNNER="$SCRIPT_DIR/runner.py"
TERMINAL="$SCRIPT_DIR/terminal.py"
SERVER_LOG="/tmp/actus-server.log"
HTTP_PORT="${ACTUS_HTTP_PORT:-9090}"
WS_PORT="${ACTUS_WS_PORT:-8080}"

# ── Colors ────────────────────────────────────────────────────────────
PASS="\033[92m✓\033[0m"
FAIL="\033[91m✗\033[0m"
INFO="\033[96m==>\033[0m"
WARN="\033[93m⚠\033[0m"
BOLD="\033[1m"
DIM="\033[2m"
END="\033[0m"

pass() { echo -e "  ${PASS} $*"; }
fail() { echo -e "  ${FAIL} $*"; exit 1; }
info() { echo -e "${INFO} $*"; }
warn() { echo -e "${WARN} $*" >&2; }
step() { echo -e "\n${INFO} ${BOLD}$*${END}"; }

cleanup() {
    pkill -f "target/debug/" 2>/dev/null || true
    pkill -f "helix-zed-headless" 2>/dev/null || true
    sleep 1
}

check_binary() {
    if [ ! -f "$BIN" ]; then
        warn "Rust binary not built — run 'cargo build -p ' first"
        return 1
    fi
    pass "Binary: $BIN"
}

check_zed_binary() {
    if [ ! -f "$ZED_BIN" ]; then
        warn "Zed headless binary not found at $ZED_BIN"
        return 1
    fi
    pass "Zed binary: $ZED_BIN ($(ls -lh "$ZED_BIN" | awk '{print $5}'))"
}

# ── Test: Health endpoint ─────────────────────────────────────────────

test_health() {
    step "Test: Health endpoint"
    local h
    h=$(curl -s http://127.0.0.1:$HTTP_PORT/health 2>/dev/null || echo '{"status":"error"}')
    if echo "$h" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('status')=='ok' else 1)" 2>/dev/null; then
        pass "Server health: ok"
        local zed agent threads
        zed=$(echo "$h" | python3 -c "import sys,json; print(json.load(sys.stdin).get('zed_connected',False))")
        agent=$(echo "$h" | python3 -c "import sys,json; print(json.load(sys.stdin).get('agent_ready',False))")
        threads=$(echo "$h" | python3 -c "import sys,json; print(json.load(sys.stdin).get('active_threads',0))")
        pass "Zed connected: $zed"
        pass "Agent ready: $agent"
        pass "Active threads: $threads"
    else
        fail "Server health check failed"
    fi
}

# ── Test: File search ─────────────────────────────────────────────────

test_files() {
    step "Test: File search"
    local r
    r=$(curl -s "http://127.0.0.1:$HTTP_PORT/v1/files?q=main&max=3" 2>/dev/null)
    local count
    count=$(echo "$r" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo "0")
    if [ "$count" -gt 0 ]; then
        pass "File search returned $count results"
    else
        warn "File search returned 0 results"
    fi
}

# ── Test: Thread listing ──────────────────────────────────────────────

test_threads() {
    step "Test: Thread listing"
    local r
    r=$(curl -s http://127.0.0.1:$HTTP_PORT/v1/threads 2>/dev/null)
    local count
    count=$(echo "$r" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('threads',[])))" 2>/dev/null || echo "0")
    pass "Threads: $count"
}

# ── Server start ──────────────────────────────────────────────────────

start_server() {
    step "Starting  server via runner.py"

    local api_key="${LLM_API_KEY:-${DEEPSEEK_API_KEY:-}}"
    if [ -z "$api_key" ] && [ -f "$SCRIPT_DIR/.env" ]; then
        api_key=$(grep -E '^LLM_API_KEY=' "$SCRIPT_DIR/.env" | head -1 | cut -d= -f2-)
    fi

    cleanup

    local runner_args=(
        "--workdir" "$PROJECT_DIR"
        "--http-port" "$HTTP_PORT"
        "--ws-port" "$WS_PORT"
        "--server-only"
        "--no-build"
    )
    [ -n "$api_key" ] && runner_args+=("--api-key" "$api_key")
    if [ -f "$ZED_BIN" ]; then
        runner_args+=("--bin" "$ZED_BIN")
    fi

    info "HTTP:  http://127.0.0.1:$HTTP_PORT"
    info "WS:    ws://127.0.0.1:$WS_PORT"
    info "Workdir: $PROJECT_DIR"
    [ -n "$api_key" ] && info "API key set"

    RUST_LOG="${RUST_LOG:-actus=info}" \
    python3 "$SCRIPT_DIR/runner.py" "${runner_args[@]}" &

    SERVER_PID=$!
    pass "Server started via runner.py (PID: $SERVER_PID)"

    # Wait for HTTP server to be ready
    for i in $(seq 1 15); do
        sleep 1
        if curl -s http://127.0.0.1:$HTTP_PORT/health >/dev/null 2>&1; then
            pass "HTTP server ready after ${i}s"
            return 0
        fi
    done

    warn "Server did not become ready within 15s"
    tail -10 "$SERVER_LOG"
    return 1
}

# ── All tests ─────────────────────────────────────────────────────────

run_tests() {
    step "Full test suite"

    start_server
    echo ""
    test_health
    test_files
    test_threads
    cleanup

    echo ""
    info "${BOLD}All tests passed.${END}"
}

# ── Interactive CLI ───────────────────────────────────────────────────

run_cli() {
    if [ ! -f "$TERMINAL" ]; then
        fail "terminal.py not found: $TERMINAL"
    fi
    info "Connecting to http://127.0.0.1:$HTTP_PORT"
    python3 "$TERMINAL" --port "$HTTP_PORT"
}

# ── Main ──────────────────────────────────────────────────────────────

show_help() {
    cat <<EOF
Usage: $0 [OPTIONS]

 launcher and test suite

Modes:
  (default)       Build, start server, then launch CLI
  --test          Run test suite (requires running server or starts one)
  --server-only   Start server only (background)
  --cli           CLI only (connect to already-running server)
  --help          Show this help

Environment:
  LLM_API_KEY      Provider API key
  DEEPSEEK_API_KEY Fallback API key
  LLM_PROVIDER     Provider name (default: deepseek)
  LLM_BASE_URL     API base URL
  LLM_MODEL        Model name
  ACTUS_HTTP_PORT    HTTP port (default: 9090)
  ACTUS_WS_PORT      WS port (default: 8080)
EOF
    exit 0
}

MODE="${1:-default}"

case "$MODE" in
    --test|-t)
        run_tests
        ;;
    --server-only|-s)
        info "Starting server only via runner.py"
        python3 "$RUNNER" --server-only --workdir "$PROJECT_DIR" &
        SERVER_PID=$!
        info "Server running in background (PID: $SERVER_PID)"
        info "Stop: pkill -f "
        ;;
    --cli|-c)
        run_cli
        ;;
    --help|-h)
        show_help
        ;;
    *)
        info "Starting  via runner.py"
        python3 "$RUNNER" --workdir "$PROJECT_DIR"
        ;;
esac
