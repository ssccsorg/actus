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
# Export local secrets/config from .env so the server and every child
# process (telos, ext_cli workers) inherit them.
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a
    . "$PROJECT_DIR/.env"
    set +a
fi
RUNNER="$SCRIPT_DIR/runner.py"
TERMINAL="$SCRIPT_DIR/terminal.py"
# Respect a pre-configured TELOS_BIN (e.g. CI sets TELOS_BIN=/bin/true);
# default to the sibling telos build, the only agent binary actus runs.
if [ -z "${TELOS_BIN:-}" ]; then
    TELOS_BIN="$SCRIPT_DIR/../telos/target/telos-release/tel"
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
    # Kill only the processes this repo's flows spawn. Scoping to the
    # explicit binary paths avoids clobbering sibling cargo builds whose
    # rustc command lines also contain "target/debug".
    pkill -f "$SCRIPT_DIR/target/debug/" 2>/dev/null || true
    # The scenario relauncher canonicalizes the sibling path (abspath), so
    # the launch-time /../ form never matches its argv0. Match the release
    # dir suffix directly to catch both the server-spawned and the
    # scenario-relaunched agent.
    pkill -f "target/telos-release/" 2>/dev/null || true
    pkill -f "$SCRIPT_DIR/runner.py" 2>/dev/null || true
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

# ── API auth header ────────────────────────────────────────────────────

# Bearer token for the authenticated endpoints. The Rust server persists
# its effective token to ~/.actus/api_token at startup, so this stays in
# sync even when runner.py started the server with a generated token.
# /health is exempt from auth, so readiness probes work without it.
AUTH_H=()

setup_auth_header() {
    local token="${ACTUS_API_TOKEN:-}"
    if [ -z "$token" ] && [ -f "$HOME/.actus/api_token" ]; then
        token="$(cat "$HOME/.actus/api_token")"
    fi
    if [ -n "$token" ]; then
        AUTH_H=(-H "Authorization: Bearer $token")
    else
        AUTH_H=()
    fi
}

# ── API endpoint tests (server required) ──────────────────────────────

test_health() {
    step "Test: Health endpoint"
    local h
    h=$(curl -s --max-time 5 http://127.0.0.1:$HTTP_PORT/health 2>/dev/null || echo '{"status":"error"}')
    if echo "$h" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('status')=='ok' else 1)" 2>/dev/null; then
        pass "Server health: ok"
        local telos agent threads
        telos=$(echo "$h" | python3 -c "import sys,json; print(json.load(sys.stdin).get('telos_connected',False))")
        agent=$(echo "$h" | python3 -c "import sys,json; print(json.load(sys.stdin).get('agent_ready',False))")
        threads=$(echo "$h" | python3 -c "import sys,json; print(json.load(sys.stdin).get('active_threads',0))")
        pass "Telos connected: $telos"
        pass "Agent ready: $agent"
        pass "Active threads: $threads"
    else
        fail "Server health check failed"
    fi
}

test_files() {
    step "Test: File search"
    local r
    r=$(curl -s --max-time 5 "${AUTH_H[@]+"${AUTH_H[@]}"}" "http://127.0.0.1:$HTTP_PORT/v1/files?q=run.sh&max=3" 2>/dev/null)
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
    r=$(curl -s --max-time 5 "${AUTH_H[@]+"${AUTH_H[@]}"}" "http://127.0.0.1:$HTTP_PORT/v1/files/mention?q=run.sh" 2>/dev/null)
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
    r=$(curl -s --max-time 5 "${AUTH_H[@]+"${AUTH_H[@]}"}" http://127.0.0.1:$HTTP_PORT/v1/threads 2>/dev/null)
    local count
    count=$(echo "$r" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('threads',[])))" 2>/dev/null || echo "0")
    pass "Threads: $count"

    # Thread detail (if any threads exist)
    local first_id
    first_id=$(echo "$r" | python3 -c "import sys,json; ts=json.load(sys.stdin).get('threads',[]); print(ts[0]['id'] if ts else '')" 2>/dev/null)
    if [ -n "$first_id" ]; then
        local detail
        detail=$(curl -s --max-time 5 "${AUTH_H[@]+"${AUTH_H[@]}"}" "http://127.0.0.1:$HTTP_PORT/v1/threads/$first_id" 2>/dev/null)
        local has_id
        has_id=$(echo "$detail" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if 'id' in d else 1)" 2>/dev/null && echo "1" || echo "0")
        if [ "$has_id" = "1" ]; then
            pass "Thread detail: $first_id"
        else
            warn "Thread detail returned unexpected response"
        fi
    else
        warn "No threads to test detail"
    fi
}

test_git_status() {
    step "Test: Git status"
    local r ok
    # The endpoint returns {\"ok\": true, \"status\": { ... }} with
    # ahead/behind nested inside status, so the check keys off the
    # top-level ok flag. Retry briefly: the server may still be settling
    # when the first request arrives.
    for _ in 1 2 3 4 5; do
        r=$(curl -s --max-time 5 "${AUTH_H[@]+"${AUTH_H[@]}"}" http://127.0.0.1:$HTTP_PORT/v1/git/status 2>/dev/null)
        ok=$(echo "$r" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('ok') else 1)" 2>/dev/null && echo "1" || echo "0")
        [ "$ok" = "1" ] && break
        sleep 1
    done
    if [ "$ok" = "1" ]; then
        pass "Git status returned valid response"
    else
        warn "Git status: unexpected response (not a git repo?)"
    fi
}

test_git_log() {
    step "Test: Git log"
    local r
    r=$(curl -s --max-time 5 "${AUTH_H[@]+"${AUTH_H[@]}"}" http://127.0.0.1:$HTTP_PORT/v1/git/log?max=3 2>/dev/null)
    local count
    count=$(echo "$r" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('commits',[])))" 2>/dev/null || echo "0")
    if [ "$count" -gt 0 ]; then
        pass "Git log returned $count commits"
    else
        warn "Git log returned 0 commits (empty repo?)"
    fi
}

test_git_diff() {
    step "Test: Git diff"
    local r
    r=$(curl -s --max-time 5 "${AUTH_H[@]+"${AUTH_H[@]}"}" http://127.0.0.1:$HTTP_PORT/v1/git/diff 2>/dev/null)
    local ok
    ok=$(echo "$r" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if 'diff' in d or 'error' in d else 1)" 2>/dev/null && echo "1" || echo "0")
    if [ "$ok" = "1" ]; then
        pass "Git diff returned valid response"
    else
        warn "Git diff: unexpected response"
    fi
}

test_llm_chat() {
    step "Test: LLM chat round trip (opt-in)"
    # Never spend LLM tokens from an automatic run. The round trip runs
    # only when LLM_CHAT=1 is set explicitly on top of a real key.
    if [ "${LLM_CHAT:-0}" != "1" ]; then
        warn "Skipped: set LLM_CHAT=1 to run the live LLM round trip"
        return 0
    fi
    local api_key="${LLM_API_KEY:-}"
    if [ -z "$api_key" ] && [ -f "$SCRIPT_DIR/.env" ]; then
        api_key=$(grep -E '^LLM_API_KEY=' "$SCRIPT_DIR/.env" | head -1 | cut -d= -f2-)
    fi
    if [ -z "$api_key" ] || [ "$api_key" = "ci-skip" ]; then
        warn "Skipped: no LLM_API_KEY set"
        return 0
    fi

    local r
    r=$(curl -s --max-time 10 -X POST http://127.0.0.1:$HTTP_PORT/v1/chat/async \
        "${AUTH_H[@]+"${AUTH_H[@]}"}" \
        -H "Content-Type: application/json" \
        -d '{"message":"hello, respond with just ok","require_approval":false}' 2>/dev/null)
    local task_id thread_id
    task_id=$(echo "$r" | python3 -c "import sys,json; print(json.load(sys.stdin).get('task_id',''))" 2>/dev/null || echo "")
    thread_id=$(echo "$r" | python3 -c "import sys,json; print(json.load(sys.stdin).get('thread_id',''))" 2>/dev/null || echo "")
    if [ -n "$task_id" ] && [ -n "$thread_id" ]; then
        pass "Chat async returned task_id: ${task_id:0:12}... thread: ${thread_id:0:12}..."
    else
        warn "Chat async did not return task_id/thread_id"
        return 0
    fi

    # Poll the thread until the turn completes. This verifies the full
    # loop end to end: actus -> agent -> LLM -> completion. The timeout
    # must be generous: a real provider turn routinely takes 10-60s.
    local timeout="${LLM_CHAT_TIMEOUT:-90}"
    local i
    for i in $(seq 1 "$timeout"); do
        local poll completed content
        poll=$(curl -s --max-time 5 "${AUTH_H[@]+"${AUTH_H[@]}"}" "http://127.0.0.1:$HTTP_PORT/v1/threads/$thread_id/poll" 2>/dev/null || echo "")
        completed=$(echo "$poll" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('completed') else 1)" 2>/dev/null && echo "1" || echo "0")
        if [ "$completed" = "1" ]; then
            content=$(echo "$poll" | python3 -c "import sys,json; print(json.load(sys.stdin).get('new_content','') or '')" 2>/dev/null || echo "")
            if [ -n "$content" ]; then
                pass "Turn completed after ${i}s: $(echo "$content" | head -c 60)..."
            else
                warn "Turn completed after ${i}s but content is empty"
            fi
            return 0
        fi
        sleep 1
    done

    warn "Chat did not complete within ${timeout}s (thread $thread_id)"
}

# ── Server start ──────────────────────────────────────────────────────

ensure_telos_binary() {
    if [ -f "$TELOS_BIN" ]; then
        pass "Agent binary: $TELOS_BIN"
        return 0
    fi
    info "Agent binary not found at $TELOS_BIN"
    warn "Build the sibling telos repo first (cargo build --profile telos-release -p telos), then retry. Agent integration tests are skipped until the binary exists."
    return 1
}

start_server() {
    step "Starting Actus server via runner.py"

    ensure_telos_binary

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
    )
    [ -n "$api_key" ] && runner_args+=("--api-key" "$api_key")
    if [ -f "$TELOS_BIN" ]; then
        runner_args+=("--bin" "$TELOS_BIN")
    fi

    info "HTTP:  http://127.0.0.1:$HTTP_PORT"
    info "WS:    ws://127.0.0.1:$WS_PORT"
    info "Workdir: $PROJECT_DIR"
    [ -n "$api_key" ] && info "API key set"

    RUST_LOG="${RUST_LOG:-actus=info}" \
    python3 "$SCRIPT_DIR/runner.py" "${runner_args[@]}" &

    SERVER_PID=$!
    pass "Server started via runner.py (PID: $SERVER_PID)"

    # Wait for the HTTP server AND the agent to come up. The agent takes a
    # few seconds to connect and report agent_ready (telos sends it ~5s
    # after the WebSocket connects), so gating on a bare HTTP response
    # would let the agent-dependent tests (health, chat) race the connect.
    # Use --max-time so a stalled server fails the readiness loop instead
    # of hanging the suite.
    for i in $(seq 1 20); do
        sleep 1
        local health
        health=$(curl -s --max-time 2 http://127.0.0.1:$HTTP_PORT/health 2>/dev/null || echo '')
        local ready
        ready=$(echo "$health" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('telos_connected') and d.get('agent_ready') else 1)" 2>/dev/null && echo 1 || echo 0)
        if [ "$ready" = "1" ]; then
            pass "Server and agent ready after ${i}s"
            return 0
        fi
    done

    warn "Server/agent did not become ready within 20s"
    tail -10 "$SERVER_LOG"
    # Stub agents (/bin/true in CI) never connect. The suite continues
    # with the server-only checks; agent-contract E2E runs against the
    # stub backend in the telos CI workflow.
    info "Agent not connected; running server-only integration checks"
    return 0
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
    setup_auth_header
    echo ""
    test_health
    test_files
    test_file_mention
    test_threads
    test_git_status
    test_git_log
    test_git_diff
    test_llm_chat
    cleanup

    echo ""
    info "${BOLD}All tests passed.${END}"
}

# ── Real-scenario tests ────────────────────────────────────────────────

run_scenarios() {
    local stub="${ACTUS_STUB:-0}"
    if [ "$stub" = "1" ]; then
        info "${BOLD}Deterministic contract scenarios (stub backend, no LLM)${END}"
        # The stub backend answers every prompt with a fixed string, so the
        # scenario checks are reproducible without a real API key. The actus
        # server still requires a non-empty api_key to launch a telos agent,
        # so export an empty value: the variables must stay exported even
        # when the caller never set them, otherwise the server sees them
        # unset and exits with "LLM API key required".
        export TELOS_STUB_BACKEND=1
        export ACTUS_STUB=1
        export LLM_API_KEY=""
        export DEEPSEEK_API_KEY=""
    else
        info "${BOLD}Real-scenario tests (tool turns, concurrency, reconnect, soak)${END}"
    fi
    start_server
    echo ""
    local soak="${SOAK_MINUTES:-2}"
    # Hard upper bound so a wedged scenario cannot pin CI or a local run
    # forever; override with SCENARIOS_DEADLINE_S.
    local deadline="${SCENARIOS_DEADLINE_S:-1200}"
    # -u: stream scenario progress unbuffered; the launcher redirects the
    # output to a log, and buffered prints would hide a long-running
    # scenario until it exits.
    ACTUS_HTTP_PORT="$HTTP_PORT" ACTUS_WS_PORT="$WS_PORT" \
        TELOS_BIN="$TELOS_BIN" SOAK_MINUTES="$soak" \
        SCENARIOS_DEADLINE_S="$deadline" \
        python3 -u "$SCRIPT_DIR/tests/scenarios.py" &
    local scenario_pid=$!
    local waited=0
    while kill -0 "$scenario_pid" 2>/dev/null; do
        sleep 5
        waited=$((waited + 5))
        if [ "$waited" -ge "$deadline" ]; then
            warn "Scenarios exceeded ${deadline}s deadline; killing the run"
            kill "$scenario_pid" 2>/dev/null || true
            wait "$scenario_pid" 2>/dev/null || true
            cleanup
            fail "Scenarios timed out after ${deadline}s"
            return
        fi
    done
    if wait "$scenario_pid"; then
        pass "Scenarios passed"
    else
        fail "Scenarios failed"
    fi
    cleanup
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
  --scenarios     Run deterministic contract scenarios (stub backend, no LLM)
  --scenarios-llm Run live scenarios against the real LLM (opt-in, consumes API)
  --server-only   Start server only (background)
  --cli           CLI only (connect to already-running server)
  --help          Show this help

Environment:
  LLM_API_KEY      API key for the OpenAI-compatible endpoint
  LLM_CHAT         Set to 1 to run the live LLM chat round trip in --test
  LLM_PROVIDER     Provider label for the OpenAI-compatible endpoint (default: openai-compatible)
  LLM_BASE_URL     Base URL of the OpenAI-compatible endpoint
  LLM_MODEL        Model name served by the endpoint
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
    --scenarios|-s)
        # Deterministic by default: never spend LLM tokens unless the
        # caller explicitly opts into the live tier with --scenarios-llm.
        ACTUS_STUB=1 run_scenarios
        ;;
    --scenarios-stub|-sf)
        ACTUS_STUB=1 run_scenarios
        ;;
    --scenarios-llm|-sl)
        ACTUS_STUB=0 run_scenarios
        ;;
    --server-only|-o)
        ensure_telos_binary
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
        ensure_telos_binary
        info "Building and starting Actus..."
        python3 "$RUNNER" --workdir "$PROJECT_DIR"
        ;;
esac
