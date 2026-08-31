#!/usr/bin/env python3
"""Live real-scenario tests for the actus <-> telos contract.

Covers the gaps the integration suite does not: tool-driven turns,
concurrent threads, WebSocket reconnect/resume, and a short soak.

Requires a running actus server whose agent is telos-headless. The
reconnect scenarios locate the live agent process, kill it, and relaunch
it with the same launch contract (user-data-dir from the command line,
environment reconstructed from actus's launch_zed), so actus's accept
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

PORT = int(os.environ.get("ACTUS_HTTP_PORT", "9090"))
BASE = f"http://127.0.0.1:{PORT}"
WS_PORT = int(os.environ.get("ACTUS_WS_PORT", "8080"))
TELOS_BIN = os.environ.get(
    "TELOS_BIN", "../telos/target/telos-release/telos-headless"
)
SOAK_MINUTES = float(os.environ.get("SOAK_MINUTES", "2"))
PASS = 0
FAIL = 0


def http(method, path, body=None, timeout=10):
    url = f"{BASE}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
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
        if s == 200 and h.get("zed_connected") and h.get("agent_ready"):
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


# ── agent process management ───────────────────────────────────────────

def find_agent_pid():
    out = subprocess.run(
        ["pgrep", "-f", "telos-headless --headless"],
        capture_output=True, text=True,
    ).stdout.split()
    return int(out[0]) if out else None


def kill_all_agents():
    """Kill every telos-headless agent. The reconnect scenarios own the
    agent lifecycle; leaving relaunched agents running lets a stale one
    reconnect instantly and mask the disconnect window."""
    out = subprocess.run(
        ["pgrep", "-f", "telos-headless --headless"],
        capture_output=True, text=True,
    ).stdout.split()
    for pid in out:
        subprocess.run(["kill", "-9", pid], capture_output=True)
    return len(out)


def agent_launch_contract(pid):
    """Extract the user-data-dir and workdir from the running agent, then
    rebuild the launch env the way actus's launch_zed does."""
    cmd = subprocess.run(
        ["ps", "-o", "command=", "-p", str(pid)],
        capture_output=True, text=True,
    ).stdout.strip()
    m = re.search(r"--user-data-dir\s+(\S+)", cmd)
    user_data_dir = m.group(1) if m else None
    tokens = [t for t in cmd.split() if not t.startswith("-")]
    workdir = tokens[-1] if tokens else os.getcwd()
    env = {
        "ZED_EXTERNAL_SYNC_ENABLED": "true",
        "ZED_WEBSOCKET_SYNC_ENABLED": "true",
        "ZED_HELIX_URL": f"127.0.0.1:{WS_PORT}",
        "ZED_HELIX_TOKEN": "test-token",
        "HELIX_SESSION_ID": "ses_actus-reconnect-test",
        "ZED_STATELESS": "1",
        "ZED_WORK_DIR": workdir,
        "ZED_TOOL_APPROVAL": "always",
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

def scenario_tool_turn():
    print("== S1: tool-driven turn")
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
        if h and not h.get("zed_connected"):
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
            "ZED_EXTERNAL_SYNC_ENABLED": "true",
            "ZED_WEBSOCKET_SYNC_ENABLED": "true",
            "ZED_HELIX_URL": f"127.0.0.1:{WS_PORT}",
            "ZED_HELIX_TOKEN": "test-token",
            "HELIX_SESSION_ID": "ses_actus-reconnect-test",
            "ZED_STATELESS": "1",
            "ZED_WORK_DIR": os.getcwd(),
            "ZED_TOOL_APPROVAL": "always",
            "RUST_LOG": "info",
        },
        None,
        os.getcwd(),
    )
    relaunch_agent(env, user_data_dir, workdir)
    return bool(wait_ready(timeout=40))


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
        if not h or not h.get("zed_connected"):
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

    scenario_tool_turn()
    scenario_concurrent()
    scenario_reconnect()
    scenario_mid_turn_resume()
    scenario_multi_turn_resume()
    if SOAK_MINUTES > 0:
        scenario_soak()

    print(f"\nscenarios: {PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
