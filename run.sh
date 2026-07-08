#!/usr/bin/env bash
# run.sh — Actus launcher + test suite
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
HELIX_DIR="$SCRIPT_DIR/helix"
RUNNER="$SCRIPT_DIR/runner.py"
TERMINAL="$SCRIPT_DIR/terminal.py"
# Respect pre-configured ZED_BIN (e.g. CI sets ZED_BIN=/bin/true)
if [ -z "${ZED_BIN:-}" ]; then
    ACTUS_ARCH="$(uname -m)"
    case "$ACTUS_ARCH" in
        x86_64|amd64) ACTUS_ARCH="amd64" ;;
        aarch64|arm64) ACTUS_ARCH="arm64" ;;
    esac
    ZED_BIN="$HELIX_DIR/.bin/helix-zed-headless-$ACTUS_ARCH"
fi
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

# ── Static checks (no server required) ────────────────────────────────

test_static_rust() {
    step "Static: Rust compile"
    if cargo check; then
        pass "cargo check"
    else
        fail "cargo check failed"
    fi
}

test_rust_unit() {
    step "Static: Rust unit tests"
    if cargo test 2>&1; then
        pass "cargo test"
    else
        fail "cargo test failed"
    fi
}

test_static_python() {
    step "Static: Python syntax"
    local ok=true
    for pyfile in "$SCRIPT_DIR"/*.py; do
        if [ -f "$pyfile" ]; then
            if python3 -m py_compile "$pyfile"; then
                pass "$(basename "$pyfile")"
            else
                warn "$(basename "$pyfile") failed"
                ok=false
            fi
        fi
    done
    $ok || fail "Python syntax check failed"
}

test_static_shell() {
    step "Static: Shell syntax"
    if bash -n "$0" 2>/dev/null; then
        pass "run.sh"
    else
        fail "run.sh syntax check failed"
    fi
}

# ── API endpoint tests (server required) ──────────────────────────────

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

test_files() {
    step "Test: File search"
    local r
    r=$(curl -s "http://127.0.0.1:$HTTP_PORT/v1/files?q=run.sh&max=3" 2>/dev/null)
    local count
    count=$(echo "$r" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo "0")
    if [ "$count" -gt 0 ]; then
        pass "File search returned $count results"
    else
        warn "File search returned 0 results"
    fi
}

test_file_mention() {
    step "Test: File mention"
    local r
    r=$(curl -s "http://127.0.0.1:$HTTP_PORT/v1/files/mention?q=run.sh" 2>/dev/null)
    local length
    length=$(echo "$r" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('mention','')))" 2>/dev/null || echo "0")
    if [ "$length" -gt 0 ]; then
        pass "File mention returned $length chars"
    else
        warn "File mention returned empty"
    fi
}

test_threads() {
    step "Test: Thread listing"
    local r
    r=$(curl -s http://127.0.0.1:$HTTP_PORT/v1/threads 2>/dev/null)
    local count
    count=$(echo "$r" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('threads',[])))" 2>/dev/null || echo "0")
    pass "Threads: $count"
}

test_git_status() {
    step "Test: Git status"
    local r
    r=$(curl -s http://127.0.0.1:$HTTP_PORT/v1/git/status 2>/dev/null)
    local ok
    ok=$(echo "$r" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if 'files' in d or 'ahead' in d or 'behind' in d else 1)" 2>/dev/null && echo "1" || echo "0")
    if [ "$ok" = "1" ]; then
        pass "Git status returned valid response"
    else
        warn "Git status: unexpected response (not a git repo?)"
    fi
}

test_git_log() {
    step "Test: Git log"
    local r
    r=$(curl -s http://127.0.0.1:$HTTP_PORT/v1/git/log?max=3 2>/dev/null)
    local count
    count=$(echo "$r" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('commits',[])))" 2>/dev/null || echo "0")
    if [ "$count" -gt 0 ]; then
        pass "Git log returned $count commits"
    else
        warn "Git log returned 0 commits (empty repo?)"
    fi
}

test_llm_chat() {
    step "Test: LLM chat (requires API key)"
    local api_key="${LLM_API_KEY:-}"
    if [ -z "$api_key" ] && [ -f "$SCRIPT_DIR/.env" ]; then
        api_key=$(grep -E '^LLM_API_KEY=' "$SCRIPT_DIR/.env" | head -1 | cut -d= -f2-)
    fi
    if [ -z "$api_key" ]; then
        warn "Skipped: no LLM_API_KEY set"
        return 0
    fi

    local r
    r=$(curl -s -X POST http://127.0.0.1:$HTTP_PORT/v1/chat/async \
        -H "Content-Type: application/json" \
        -d '{"message":"hello, respond with just ok","require_approval":false}' 2>/dev/null)
    local task_id
    task_id=$(echo "$r" | python3 -c "import sys,json; print(json.load(sys.stdin).get('task_id',''))" 2>/dev/null || echo "")
    if [ -n "$task_id" ]; then
        pass "Chat async returned task_id: ${task_id:0:12}..."
    else
        warn "Chat async did not return task_id"
    fi
}

# ── Server start ──────────────────────────────────────────────────────

ensure_zed_binary() {
    if [ -f "$ZED_BIN" ]; then
        pass "Zed binary: $ZED_BIN"
        return 0
    fi
    info "Zed binary not found at $ZED_BIN"
    if [ -f "$HELIX_DIR/build.sh" ]; then
        info "Building Zed from source via helix/build.sh..."
        bash "$HELIX_DIR/build.sh" --build-only --release || {
            warn "Zed build failed — integration tests will be skipped"
            return 1
        }
    else
        warn "helix/build.sh not found — integration tests will be skipped"
        return 1
    fi
}

start_server() {
    step "Starting Actus server via runner.py"

    ensure_zed_binary

    local api_key="${LLM_API_KEY:-}"
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

# ── Test runner ───────────────────────────────────────────────────────

run_tests() {
    info "${BOLD}Static checks${END}"
    test_static_rust
    test_rust_unit
    test_static_python
    test_static_shell

    info "\n${BOLD}Integration tests${END}"
    start_server
    echo ""
    test_health
    test_files
    test_file_mention
    test_threads
    test_git_status
    test_git_log
    test_llm_chat
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

Actus launcher and test suite

Modes:
  (default)       Build, start server, then launch CLI
  --test          Run static checks and integration tests
  --server-only   Start server only (background)
  --cli           CLI only (connect to already-running server)
  --help          Show this help

Environment:
  LLM_API_KEY      Provider API key
  LLM_PROVIDER     Provider name (default: deepseek)
  LLM_BASE_URL     API base URL
  LLM_MODEL        Model name
  ACTUS_HTTP_PORT  HTTP port (default: 9090)
  ACTUS_WS_PORT    WS port (default: 8080)
EOF
    exit 0
}

MODE="${1:-default}"

case "$MODE" in
    --test|-t)
        run_tests
        ;;
    --server-only|-s)
        ensure_zed_binary
        info "Starting server only via runner.py"
        python3 "$RUNNER" --server-only --workdir "$PROJECT_DIR" &
        SERVER_PID=$!
        info "Server running in background (PID: $SERVER_PID)"
        info "Stop: pkill -f actus"
        ;;
    --cli|-c)
        run_cli
        ;;
    --help|-h)
        show_help
        ;;
    *)
        ensure_zed_binary
        info "Building and starting Actus..."
        python3 "$RUNNER" --workdir "$PROJECT_DIR"
        ;;
esac
