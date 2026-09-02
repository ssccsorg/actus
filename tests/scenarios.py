#!/usr/bin/env python3
"""Live real-scenario tests for the actus <-> telos contract.

Covers the gaps the integration suite does not: tool-driven turns,
concurrent threads, WebSocket reconnect/resume, and a short soak.

Requires a running actus server whose agent is telos. The
reconnect scenarios locate the live agent process, kill it, and relaunch
it with the same launch contract (user-data-dir from the command line,
environment reconstructed from actus's launch_telos), so actus's accept
loop and pending-message resend are exercised for real.

Usage:
  ACTUS_HTTP_PORT=9090 SOAK_MINUTES=2 python3 tests/scenarios.py
"""

import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

PORT = int(os.environ.get("ACTUS_HTTP_PORT", "9090"))
BASE = f"http://127.0.0.1:{PORT}"
WS_PORT = int(os.environ.get("ACTUS_WS_PORT", "8080"))
TELOS_BIN = os.environ.get(
    "TELOS_BIN", "../telos/target/telos-release/tel"
)
SOAK_MINUTES = float(os.environ.get("SOAK_MINUTES", "2"))
# Deterministic contract mode: the agent runs with TELOS_FAKE_BACKEND=1 and
# answers every prompt with a fixed string. No LLM API key is involved, so
# the checks are reproducible in CI. LLM-intelligence checks are skipped.
FAKE_MODE = os.environ.get("ACTUS_FAKE", "0") == "1"
FAKE_RESPONSE = os.environ.get("TELOS_FAKE_RESPONSE", "OK")
PASS = 0
FAIL = 0

# The API requires a bearer token except on /health. Resolve it the same
# way the server does: env var, then the token file the server writes.
API_TOKEN = os.environ.get("ACTUS_API_TOKEN", "") or (
    (Path.home() / ".actus" / "api_token").read_text().strip()
    if (Path.home() / ".actus" / "api_token").exists()
    else ""
)


def http(method, path, body=None, timeout=10):
    url = f"{BASE}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if API_TOKEN:
        req.add_header("Authorization", f"Bearer {API_TOKEN}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {}
    except Exception as e:  # noqa: BLE001
        return None, {"error": str(e)}


def check(name, ok, detail=""):
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  [PASS] {name} {detail}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def wait_ready(timeout=40):
    for _ in range(timeout):
        s, h = http("GET", "/health", timeout=3)
        if s == 200 and h.get("telos_connected") and h.get("agent_ready"):
            return h
        time.sleep(1)
    return None


def health():
    s, h = http("GET", "/health", timeout=3)
    return h if s == 200 else None


def poll_thread(tid, timeout=150):
    for _ in range(timeout):
        s, p = http("GET", f"/v1/threads/{tid}/poll")
        if s == 200 and p.get("completed"):
            return p
        time.sleep(1)
    return None


def chat_async(message, thread_id=None):
    body = {"message": message, "require_approval": False}
    if thread_id:
        body["thread_id"] = thread_id
    s, r = http("POST", "/v1/chat/async", body, timeout=10)
    return r.get("thread_id") or (r.get("task_id") if s == 200 else None)


def get_thread(tid):
    s, t = http("GET", f"/v1/threads/{tid}")
    return t if s == 200 else None


def tool_call_in(thread):
    for m in thread.get("messages", []):
        if m.get("entry_type") == "tool_call" or m.get("tool_name"):
            return m
    return None


def assistant_content(thread):
    return " ".join(
        m.get("content", "") for m in thread.get("messages", [])
        if m.get("role") == "assistant" and m.get("entry_type") != "tool_call"
    )


def check_fake_response(thread):
    """In fake mode the assistant answer must be exactly the fixed string."""
    if not FAKE_MODE:
        return True
    content = assistant_content(thread)
    ok = FAKE_RESPONSE in content
    check("assistant answer is the deterministic fake response", ok, content[:60])
    return ok


# ── agent process management ───────────────────────────────────────────

def find_agent_pid():
    out = subprocess.run(
        ["pgrep", "-f", "telos --headless"],
        capture_output=True, text=True,
    ).stdout.split()
    return int(out[0]) if out else None


def kill_all_agents():
    """Kill every telos agent. The reconnect scenarios own the
    agent lifecycle; leaving relaunched agents running lets a stale one
    reconnect instantly and mask the disconnect window."""
    out = subprocess.run(
        ["pgrep", "-f", "telos --headless"],
        capture_output=True, text=True,
    ).stdout.split()
    for pid in out:
        subprocess.run(["kill", "-9", pid], capture_output=True)
    return len(out)


def agent_launch_contract(pid):
    """Extract the user-data-dir and workdir from the running agent, then
    rebuild the launch env the way actus's launch_telos does."""
    cmd = subprocess.run(
        ["ps", "-o", "command=", "-p", str(pid)],
        capture_output=True, text=True,
    ).stdout.strip()
    m = re.search(r"--user-data-dir\s+(\S+)", cmd)
    user_data_dir = m.group(1) if m else None
    tokens = [t for t in cmd.split() if not t.startswith("-")]
    workdir = tokens[-1] if tokens else os.getcwd()
    env = {
        "TELOS_EXTERNAL_SYNC_ENABLED": "true",
        "TELOS_WEBSOCKET_SYNC_ENABLED": "true",
        "TELOS_WS_URL": f"127.0.0.1:{WS_PORT}",
        "TELOS_WS_TOKEN": "test-token",
        "TELOS_STATELESS": "1",
        "TELOS_TOOL_APPROVAL": "always",
        "RUST_LOG": "info",
    }
    return env, user_data_dir, workdir


def relaunch_agent(env, user_data_dir, workdir):
    args = [
        os.path.abspath(TELOS_BIN),
        "--headless", "--allow-multiple-instances",
        "--user-data-dir", user_data_dir or "/tmp",
        workdir,
    ]
    full_env = dict(os.environ)
    full_env.update(env)
    subprocess.Popen(args, env=full_env,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"    relaunched agent (workdir={workdir})")


# ── scenarios ──────────────────────────────────────────────────────────

def scenario_feature_baseline():
    """Baseline runtime-feature probe run before any cut: file read and
    mention-format turns. Guards against cleanup silently breaking the
    agent's file tooling or mention handling."""
    print("== S0: runtime feature baseline (file read, mention)")
    tid = chat_async(
        "read the file run.sh and quote its very first line exactly"
    )
    check("file-read turn accepted", bool(tid), str(tid)[:18] if tid else "")
    if tid:
        check("file-read turn completed", bool(poll_thread(tid, timeout=150)))
        if not FAKE_MODE:
            thread = get_thread(tid) or {}
            content = assistant_content(thread)
            check("answer contains the file content",
                  "usr/bin/env" in content or "bash" in content,
                  "first line quoted")
        else:
            check_fake_response(get_thread(tid) or {})
    tid2 = chat_async(
        "@run.sh is mentioned; summarize what this script does in one line"
    )
    check("mention-format turn accepted", bool(tid2), str(tid2)[:18] if tid2 else "")
    if tid2:
        check("mention-format turn completed", bool(poll_thread(tid2, timeout=150)))
        if FAKE_MODE:
            check_fake_response(get_thread(tid2) or {})


def scenario_tool_turn():
    print("== S1: tool-driven turn")
    if FAKE_MODE:
        # The fake backend has no tools; the turn still completes with text.
        tid = chat_async("list the files in this workspace")
        check("async accepted a tool prompt", bool(tid), str(tid)[:18] if tid else "")
        if tid:
            check("tool turn completed", bool(poll_thread(tid, timeout=180)))
            check_fake_response(get_thread(tid) or {})
        return
    tid = chat_async(
        "list the files in this workspace with your file tool, then summarize the count"
    )
    check("async accepted a tool prompt", bool(tid), str(tid)[:18] if tid else "")
    if not tid:
        return
    poll = poll_thread(tid, timeout=180)
    check("tool turn completed", bool(poll))
    thread = get_thread(tid) or {}
    tool = tool_call_in(thread)
    check(
        "tool call recorded in the thread",
        bool(tool),
        f"{tool.get('tool_name')} [{tool.get('tool_status')}]" if tool else "none",
    )


def scenario_concurrent():
    print("== S2: concurrent threads")
    # Light text-only prompts: the agent processes turns serially, so the
    # goal is server-side concurrency handling, not LLM latency.
    prompts = [
        "reply with exactly the word alpha",
        "reply with exactly the word beta",
        "reply with exactly the word gamma",
    ]
    tids = [chat_async(p) for p in prompts]
    check("3 concurrent chats accepted", all(tids), f"{sum(1 for t in tids if t)}/3")
    if not all(tids):
        return
    results = [poll_thread(t, timeout=180) for t in tids]
    done = sum(1 for r in results if r)
    check("all 3 turns completed independently", done == 3, f"{done}/3")


def scenario_reconnect():
    print("== S3: reconnect after agent death")
    pid = find_agent_pid()
    check("live agent process found", bool(pid), f"pid={pid}" if pid else "")
    if not pid:
        return
    env, user_data_dir, workdir = agent_launch_contract(pid)
    kill_all_agents()
    print("    killed all agents, waiting for disconnect detection...")
    disconnected = None
    for _ in range(12):
        h = health()
        if h and not h.get("telos_connected"):
            disconnected = h
            break
        time.sleep(1)
    check("actus detected the disconnect", bool(disconnected))
    relaunch_agent(env, user_data_dir, workdir)
    h = wait_ready(timeout=40)
    check("agent reconnected and ready", bool(h))
    if h:
        tid = chat_async("reply with exactly the word ok")
        check("chat works after reconnect", bool(tid))
        if tid:
            check("post-reconnect turn completes", bool(poll_thread(tid, timeout=120)))


def scenario_mid_turn_resume():
    print("== S4: mid-turn disconnect resumes via the resend queue")
    pid = find_agent_pid()
    check("live agent process found", bool(pid), f"pid={pid}" if pid else "")
    if not pid:
        return
    env, user_data_dir, workdir = agent_launch_contract(pid)
    tid = chat_async(
        "list the files in this workspace, then read run.sh and summarize both"
    )
    check("chat accepted before the kill", bool(tid), str(tid)[:18] if tid else "")
    if not tid:
        return
    time.sleep(1.5)  # let the turn go in flight
    kill_all_agents()
    print("    killed agents mid-turn, relaunching...")
    relaunch_agent(env, user_data_dir, workdir)
    h = wait_ready(timeout=40)
    check("agent recovered after mid-turn kill", bool(h))
    poll = poll_thread(tid, timeout=180)
    check("in-flight turn completed after reconnect", bool(poll))


def scenario_multi_turn_resume():
    print("== S5: multi-turn thread resume after agent restart")
    pid = find_agent_pid()
    check("live agent process found", bool(pid), f"pid={pid}" if pid else "")
    if not pid:
        return
    env, user_data_dir, workdir = agent_launch_contract(pid)
    magic = str(int(time.time()) % 100000)
    tid = chat_async(f"remember this magic number: {magic}. reply with exactly the word ok")
    check("seed turn accepted", bool(tid), str(tid)[:18] if tid else "")
    if not tid:
        return
    check("seed turn completed", bool(poll_thread(tid, timeout=120)))
    kill_all_agents()
    print("    killed agents, relaunching for the follow-up...")
    relaunch_agent(env, user_data_dir, workdir)
    h = wait_ready(timeout=40)
    check("agent recovered before the follow-up", bool(h))
    tid2 = chat_async(f"what was the magic number I told you? reply with only the number.", thread_id=tid)
    check("follow-up accepted on the same thread", bool(tid2))
    poll = poll_thread(tid, timeout=120) if tid2 else None
    check("follow-up turn completed", bool(poll))
    if FAKE_MODE:
        # The fake backend keeps no context; only resume behavior is checked.
        check_fake_response(get_thread(tid) or {})
        return
    thread = get_thread(tid) or {}
    content = " ".join(
        m.get("content", "") for m in thread.get("messages", [])
        if m.get("role") == "assistant" and m.get("entry_type") != "tool_call"
    )
    check("follow-up answer carries the remembered context", magic in content,
          f"magic={magic}")


def ensure_agent():
    """Relaunch a single agent if none is running (used by the soak, which
    may follow scenarios that own the agent lifecycle)."""
    if find_agent_pid():
        return True
    pid = find_agent_pid()
    env, user_data_dir, workdir = agent_launch_contract(pid) if pid else (
        {
            "TELOS_EXTERNAL_SYNC_ENABLED": "true",
            "TELOS_WEBSOCKET_SYNC_ENABLED": "true",
            "TELOS_WS_URL": f"127.0.0.1:{WS_PORT}",
            "TELOS_WS_TOKEN": "test-token",
            "TELOS_STATELESS": "1",
            "TELOS_TOOL_APPROVAL": "always",
            "RUST_LOG": "info",
        },
        None,
        os.getcwd(),
    )
    relaunch_agent(env, user_data_dir, workdir)
    return bool(wait_ready(timeout=40))


def scenario_fetch_and_subagent():
    print("== S7: url fetch + sub-agent execution")
    if FAKE_MODE:
        # The fake backend has no fetch or spawn_agent tools; skip.
        print("    skipped in deterministic mode (LLM-only tools)")
        return
    tid = chat_async(
        "use the fetch tool to fetch http://127.0.0.1:9090/health and quote the status field exactly"
    )
    check("fetch turn accepted", bool(tid), str(tid)[:18] if tid else "")
    if tid:
        check("fetch turn completed", bool(poll_thread(tid, timeout=150)))
        thread = get_thread(tid) or {}
        content = " ".join(
            m.get("content", "") for m in thread.get("messages", [])
            if m.get("role") == "assistant" and m.get("entry_type") != "tool_call"
        )
        check("fetch answer reflects the fetched body",
              "ok" in content, "status quoted")
    tid2 = chat_async(
        "use the spawn_agent tool to start a subagent with the message 'reply with the single word done', then report its result"
    )
    check("sub-agent turn accepted", bool(tid2), str(tid2)[:18] if tid2 else "")
    if tid2:
        check("sub-agent turn completed", bool(poll_thread(tid2, timeout=180)))
        thread = get_thread(tid2) or {}
        content = " ".join(
            m.get("content", "") for m in thread.get("messages", [])
            if m.get("role") == "assistant" and m.get("entry_type") != "tool_call"
        )
        check("sub-agent result reported", bool(content), content[:80])


def scenario_thread_mention():
    """Thread mention: a follow-up turn references an earlier thread by its
    telos:///agent/thread/{id}?name=... URI. Guards the cross-thread
    information sharing surface that thread mentions provide. LSP
    diagnostics mentions are intentionally excluded from this probe."""
    print("== S8: thread mention (cross-thread context)")
    tid_a = chat_async(
        "reply with exactly one word: the base name of the current working directory"
    )
    check("seed thread accepted", bool(tid_a), str(tid_a)[:18] if tid_a else "")
    if not tid_a:
        return
    check("seed thread completed", bool(poll_thread(tid_a, timeout=180)))
    tid_b = chat_async(
        f"the thread mentioned at telos:///agent/thread/{tid_a}?name=seed "
        "is referenced as context; acknowledge it and continue"
    )
    check("thread-mention turn accepted", bool(tid_b), str(tid_b)[:18] if tid_b else "")
    if tid_b:
        check(
            "thread-mention turn completed",
            bool(poll_thread(tid_b, timeout=180)),
        )


def scenario_soak():
    print(f"== S6: soak ({SOAK_MINUTES} min)")
    ensure_agent()
    checks = 0
    drops = 0
    end = time.time() + SOAK_MINUTES * 60
    last_chat = 0.0
    while time.time() < end:
        h = health()
        checks += 1
        if not h or not h.get("telos_connected"):
            drops += 1
        if time.time() - last_chat > 60:
            last_chat = time.time()
            tid = chat_async("reply with exactly the word ok")
            if tid:
                poll_thread(tid, timeout=120)
        time.sleep(5)
    check("health stayed connected through the soak", drops == 0,
          f"{checks} checks, {drops} drops")
    h = health()
    check("agent ready at the end of the soak",
          bool(h and h.get("agent_ready")))


# ── main ───────────────────────────────────────────────────────────────

def main():
    print(f"scenarios: http {BASE}, agent {os.path.abspath(TELOS_BIN)}")
    h = wait_ready(timeout=40)
    check("server and agent ready", bool(h))
    if not h:
        print("aborting: agent not ready")
        sys.exit(1)

    scenario_feature_baseline()
    scenario_tool_turn()
    scenario_concurrent()
    scenario_reconnect()
    scenario_mid_turn_resume()
    scenario_multi_turn_resume()
    scenario_fetch_and_subagent()
    scenario_thread_mention()
    if SOAK_MINUTES > 0:
        scenario_soak()

    print(f"\nscenarios: {PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
