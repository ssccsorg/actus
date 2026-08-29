#!/usr/bin/env python3
"""
: REST chat client for the Rust  server.

Connects to the Rust HTTP server at localhost:9090 which already manages
Zed headless.  No subprocess, no WebSocket, no API key needed here.

Usage:
  ./terminal.py                              # default port 9090
  ./terminal.py --port 9091                  # custom port
  ./terminal.py --workdir /path/to/project   # no-zed fallback info
  ./terminal.py --no-zed                     # fallback mode

Commands:
  /exit, /quit    - exit
  /new            - start a new thread
  /thread         - show current thread ID
  /raw            - toggle raw JSON message display

Examples:
  > What's in this directory?
  > Read main.rs
  > Find TODO comments here
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

try:
    import httpx
except ImportError:
    import subprocess as _sp
    print("Installing httpx...")
    _sp.check_call([sys.executable, "-m", "pip", "install", "httpx"])
    import httpx

import aiohttp


# ── Globals ──────────────────────────────────────────────────────────────

current_thread_id = None
show_raw = False



# ── ANSI colors ──────────────────────────────────────────────────────────

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


# ── HTTP client ──────────────────────────────────────────────────────────

class NexClient:
    """Thin wrapper over the Rust server's REST API."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)

    async def close(self):
        await self.client.aclose()

    async def health(self) -> dict | None:
        try:
            r = await self.client.get("/health")
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    async def get_thread(self, thread_id: str) -> dict | None:
        try:
            r = await self.client.get(f"/v1/threads/{thread_id}")
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError:
            return None

    async def cancel(self) -> dict | None:
        try:
            r = await self.client.post("/v1/cancel")
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    async def search_files(self, q: str) -> list[dict] | None:
        try:
            params = {"q": q}
            r = await self.client.get("/v1/files", params=params)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):
                return data.get("files", data.get("results", []))
            return data if isinstance(data, list) else None
        except Exception:
            return None

    async def mention_files(self, q: str) -> str | None:
        try:
            params = {"q": q}
            r = await self.client.get("/v1/files/mention", params=params)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, str):
                return data
            if isinstance(data, dict):
                return data.get("mention", data.get("text", str(data)))
            return str(data)
        except Exception:
            return None

    async def list_threads(self) -> list[dict] | None:
        try:
            r = await self.client.get("/v1/threads")
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):
                return data.get("threads", data.get("results", []))
            return data if isinstance(data, list) else None
        except Exception:
            return None

    async def get_rules(self) -> list[dict] | None:
        try:
            r = await self.client.get("/v1/rules")
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):
                return data.get("rules", [])
            return data if isinstance(data, list) else None
        except Exception:
            return None

    async def search_symbols(self, q: str) -> list[dict] | None:
        try:
            r = await self.client.get("/v1/symbols", params={"q": q})
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):
                return data.get("symbols", [])
            return data if isinstance(data, list) else None
        except Exception:
            return None

    async def fetch_url(self, url: str) -> dict | None:
        try:
            r = await self.client.get("/v1/fetch", params={"url": url})
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    async def pending_tool_calls(self) -> list[dict]:
        try:
            r = await self.client.get("/v1/agents/tool-calls/pending")
            r.raise_for_status()
            data = r.json()
            return (data or {}).get("pending", []) if isinstance(data, dict) else []
        except Exception:
            return []

    async def resolve_tool_call(self, platform_thread_id: str, tool_call_id: str, allow: bool) -> dict | None:
        try:
            r = await self.client.post(
                "/v1/agents/tool-calls/resolve",
                json={
                    "platform_thread_id": platform_thread_id,
                    "tool_call_id": tool_call_id,
                    "allow": allow,
                },
            )
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    async def create_thread(self) -> str | None:
        """Create a fresh thread immediately (no message)."""
        try:
            r = await self.client.post("/v1/threads")
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):
                return data.get("thread_id")
            return None
        except Exception:
            return None


# ── Message display ──────────────────────────────────────────────────────

def print_banner(h: dict):
    print(f"\n{C.BOLD}{C.HEADER}╔══════════════════════════════════════╗{C.END}")
    print(f"{C.BOLD}{C.HEADER}║       : Server Chat           ║{C.END}")
    print(f"{C.BOLD}{C.HEADER}╚══════════════════════════════════════╝{C.END}")
    ok = h and h.get("status") == "ok"
    if ok:
        zed = h.get("zed_connected", False)
        agent = h.get("agent_ready", False)
        print(f"  {C.GREEN}✓{C.END} Server: {h.get('status', '?')}")
        print(f"  {C.GREEN}✓{C.END} Zed connected: {zed}")
        print(f"  {C.GREEN}✓{C.END} Agent ready: {agent}")
        print(f"  {C.DIM}Active threads: {h.get('active_threads', 0)}{C.END}")
        if not zed or not agent:
            print(f"\n{C.YELLOW}⚠ Waiting for Zed to connect...{C.END}")
    else:
        print(f"  {C.RED}✗{C.END} Server unreachable")
    print()


# ── Thread selection ─────────────────────────────────────────────────────

async def select_thread_prompt(client: NexClient, threads: list[dict]) -> str | None:
    """Let user select a thread from the list, or None for new thread.
    Returns "quit" to exit the program."""

    # Filter out ACP-internal threads (no user messages = no title)
    user_threads = [t for t in threads if t.get("title")]

    if not user_threads:
        print(f"{C.DIM}No existing conversations.{C.END}")
        return None

    print(f"\n{C.BOLD}Conversations:{C.END}")
    for i, t in enumerate(user_threads, 1):
        tid = t.get("thread_id", t.get("id", "?"))
        title_raw = t.get("title", "") or tid
        title = (title_raw[:40] + "...") if len(title_raw) > 40 else title_raw
        msg_count = t.get("message_count", t.get("num_messages", 0))
        created = (t.get("created_at", t.get("created", "")) or "")[:16]
        print(f"  [{i}] {title:<43} {msg_count:>4} msgs  {created}")

    default = 1 if len(user_threads) == 1 else 0
    print("  [0] New thread")
    print("  [q] Quit")

    prompt = f"Select thread [{default}]: "
    result = (await _readline(prompt) or "").strip().lower()

    if result == "q":
        return "quit"

    if not result:
        choice = default
    else:
        try:
            choice = int(result)
        except ValueError:
            choice = default

    if choice == 0:
        return None

    if 1 <= choice <= len(user_threads):
        return user_threads[choice - 1].get("thread_id", user_threads[choice - 1].get("id"))

    print(f"{C.YELLOW}Invalid choice, starting new thread.{C.END}")
    return None


# ── Interactive prompt ────────────────────────────────────────────────
# Prompts (mention picker, tool approval) fire inside send_chat. read_stdin
# never awaits send_chat inline, so it stays free to read stdin and route
# the next line into the active prompt's queue.
_prompt_q: "asyncio.Queue[str] | None" = None
_chat_queue: "asyncio.Queue[str] | None" = None
_stdin_reader: "asyncio.StreamReader | None" = None


async def _make_reader() -> "asyncio.StreamReader | None":
    """Set up a single StreamReader over stdin for the whole session."""
    global _stdin_reader
    if not sys.stdin.isatty() or not sys.__stdin__ or not sys.__stdin__.isatty():
        return None
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    try:
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    except (OSError, AttributeError):
        return None
    _stdin_reader = reader
    return reader


async def _readline(prompt: str = "") -> str | None:
    """Print the prompt and read one line from the shared stdin reader.
    Lines are routed to an active prompt queue (mention picker, tool
    approval). Returns None on EOF."""
    if _stdin_reader is None:
        return None
    if prompt:
        print(prompt, end="", flush=True)
    while True:
        line = await _stdin_reader.readline()
        if not line:
            return None
        text = line.decode().strip()
        if not text:
            continue
        if _prompt_q is not None:
            await _prompt_q.put(text)
            continue
        return text


async def _ask(prompt: str) -> str:
    global _prompt_q
    print(prompt, end="", flush=True)
    q: asyncio.Queue[str] = asyncio.Queue()
    _prompt_q = q
    try:
        return await q.get()
    finally:
        _prompt_q = None


async def _chat_worker(client: NexClient):
    """Send queued chat messages sequentially, keeping the stdin loop
    free so interactive prompts can receive input. A failure in one chat
    must not kill the worker, or every later message is silently lost."""
    while True:
        message = await _chat_queue.get()
        try:
            await send_chat(client, message)
        except Exception as e:  # noqa: BLE001 - keep the worker alive
            print(f"{C.RED}Chat error: {e}{C.END}", file=sys.stderr)


# ── Send message (SSE streaming) ─────────────────────────────────────────

async def resolve_mentions(client: NexClient, message: str) -> str:
    """Resolve @mentions to context before sending.

    Supported forms:
      @path/to/file    file paths auto-injected (top matches)
      @?query          interactive picker across files/symbols/threads
      @rules           project rule files (AGENTS.md, *.mdc)
      @symbol:query    definition-pattern symbol search
      @thread:query    conversation thread content
      @fetch:URL       fetched URL text
    """
    import re

    pattern = re.compile(r"@(\??[\w./_-]+(?::[^\s]+)?)")
    matches = list(pattern.finditer(message))
    if not matches:
        return message

    resolved = {}
    for m in matches:
        token = m.group(1)
        if token in resolved:
            continue
        resolved[token] = await _resolve_mention(client, token)

    result = message
    for m in reversed(matches):
        token = m.group(1)
        result = result[:m.start()] + resolved[token] + result[m.end():]
    return result


async def _mention_picker(client: NexClient, token: str, candidates: list) -> list | None:
    """Show a numbered menu of mention candidates; return the chosen ones
    (or all for 'a', or None to skip)."""
    icons = {"file": "📄", "symbol": "⚙", "thread": "💬"}
    print(f"\n{C.BOLD}Mentions for `{token}`:{C.END}")
    for i, (kind, label, _) in enumerate(candidates, 1):
        print(f"  [{i}] {icons.get(kind, '•')} {label}")
    print(f"  [a] all  [0] skip")
    answer = (await _ask("Select [0]: ")).strip()
    if answer.lower() == "a":
        return candidates
    try:
        n = int(answer)
    except ValueError:
        return None
    if 1 <= n <= len(candidates):
        return [candidates[n - 1]]
    return None


async def _thread_payload(client: NexClient, t: dict) -> str:
    detail = await client.get_thread(t["id"])
    msgs = (detail or {}).get("messages", [])
    body = "\n".join(
        f"{m.get('role', '?')}: {m.get('content', '')[:400]}" for m in msgs[-6:]
    )
    return f"[Thread: {t.get('title') or t['id']}]\n{body}"


async def _resolve_mention(client: NexClient, token: str) -> str:
    # Explicit picker: @?query opens the interactive menu across files,
    # symbols, and threads.
    if token.startswith("?"):
        q = token[1:]
        files = await client.search_files(q)
        syms = await client.search_symbols(q)
        threads = await client.list_threads()
        candidates = [
            ("file", f"{f.get('relative_path', f.get('path', '?'))}", f)
            for f in (files or [])[:6]
        ]
        candidates += [
            (
                "symbol",
                f"{s.get('file', '?')}:{s.get('line', '?')} {s.get('snippet', '').strip()[:44]}",
                s,
            )
            for s in (syms or [])[:6]
        ]
        if threads:
            candidates += [
                ("thread", f"{t.get('title') or t['id']}", t) for t in threads[:6]
            ]
        if not candidates:
            return f"@{token}"
        chosen = await _mention_picker(client, token, candidates)
        if not chosen:
            return f"@{token}"
        return "\n\n".join(await _mention_payloads(client, chosen))

    # Deterministic sources: rules and fetch inject directly.
    if token == "rules":
        rules = await client.get_rules()
        if not rules:
            return "@rules"
        return "\n\n".join(
            f"[Rules: {r.get('path', '?')}]\n{r.get('content', '')}" for r in rules[:3]
        )

    if token.startswith("fetch:"):
        url = token[len("fetch:"):]
        data = await client.fetch_url(url)
        content = (data or {}).get("content", "")
        if not content:
            return f"@{token}"
        return f"[Fetched: {url}]\n{content[:3000]}"

    # Explicit symbol search: unique match auto-injects, ambiguous opens
    # the picker.
    if token.startswith("symbol:"):
        q = token[len("symbol:"):]
        syms = await client.search_symbols(q)
        if not syms:
            return f"@{token}"
        candidates = [
            (
                "symbol",
                f"{s.get('file', '?')}:{s.get('line', '?')} {s.get('snippet', '').strip()[:44]}",
                s,
            )
            for s in syms
        ]
        chosen = candidates if len(candidates) == 1 else await _mention_picker(client, token, candidates)
        if not chosen:
            return f"@{token}"
        return "\n\n".join(await _mention_payloads(client, chosen))

    # Explicit thread search.
    if token.startswith("thread:"):
        q = token[len("thread:"):].strip().lower()
        threads = await client.list_threads()
        if not threads:
            return f"@{token}"
        hits = [t for t in threads if q in (t.get("title") or "").lower()]
        if not hits:
            return f"@{token}"
        candidates = [("thread", f"{t.get('title') or t['id']}", t) for t in hits]
        chosen = candidates if len(candidates) == 1 else await _mention_picker(client, token, candidates)
        if not chosen:
            return f"@{token}"
        return "\n\n".join(await _mention_payloads(client, chosen))

    # Default: file paths only, auto-injected without prompting so chat
    # stays smooth. Use @?query for an interactive picker.
    files = await client.search_files(token)
    if not files:
        return f"@{token}"
    return " ".join(f.get("path", "") for f in files[:3])


async def _mention_payloads(client: NexClient, chosen: list) -> list[str]:
    parts = []
    for kind, label, payload in chosen:
        if kind == "file":
            parts.append(payload.get("path", label))
        elif kind == "symbol":
            parts.append(
                f"{payload.get('file', '?')}:{payload.get('line', '?')} {payload.get('snippet', '').strip()}"
            )
        elif kind == "thread":
            parts.append(await _thread_payload(client, payload))
    return parts


async def send_chat(client: NexClient, message: str):
    """Send a message via /v1/chat/async + polling /v1/threads/:id/poll.

    Uses async submission (returns immediately) and polls for new content
    at 300ms intervals. This avoids the SSE streaming issues with the
    WebSocket event delivery from Zed.
    """
    global current_thread_id, show_raw

    if not message:
        return

    # Whether we were already in a thread. A fresh thread (no prior
    # current_thread_id) prints a header; a resumed thread already showed
    # its history, so repeating the header is noise.
    had_thread = bool(current_thread_id)

    # Resolve @mentions to file paths
    resolved = await resolve_mentions(client, message)
    if resolved != message:
        print(f"{C.DIM}@mentions resolved: {resolved[:120]}...{C.END}")
        message = resolved

    body = {"message": message}
    if current_thread_id:
        body["thread_id"] = current_thread_id

    if show_raw:
        print(f"\n{C.DIM}[ASYNC REQ] {json.dumps(body)}{C.END}")

    # Step 1: Capture the starting turn BEFORE submitting. Reading it
    # after the async submit races with a fast completion: if the turn
    # already finished, turn_completed is the new value and the poll below
    # would wait for a turn that never comes (or read past the message
    # array). Capturing pre-submit keeps known_turn one behind the turn we
    # are about to start, so `completed = turn_completed > known_turn`
    # fires exactly when this turn ends. The last assistant message is
    # seeded into `shown` so a resumed thread never re-prints the previous
    # turn's content.
    known_turn = 0
    shown = ""
    current_msg_id = None
    if current_thread_id:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                    f"{client.base_url}/v1/threads/{current_thread_id}",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        known_turn = data.get("turn_completed", 0)
                        for m in reversed(data.get("messages", [])):
                            if m.get("role") == "assistant":
                                shown = m.get("content", "")
                                current_msg_id = m.get("message_id")
                                break
            except Exception:
                pass

    # Step 2: Send message via async endpoint
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                f"{client.base_url}/v1/chat/async",
                json=body,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    print(f"{C.RED}Error {resp.status}: {text}{C.END}")
                    return
                result = await resp.json()
                tid = result.get("thread_id", "")
                if tid:
                    current_thread_id = tid
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"{C.RED}Request failed: {e}{C.END}")
            return

    if not had_thread and current_thread_id:
        print(f"\n{C.CYAN}Thread: {current_thread_id}{C.END}\n")

    content_len = 0
    poll_interval = 0.3
    max_wait = 120.0  # 2 minutes max
    waited = 0.0
    shown_prev = False
    seen_approvals = set()
    last_approval_check = 0.0
    tool_statuses: dict = {}  # message_id -> last status rendered

    # The session is recreated on a transport error: a stale keep-alive
    # connection can fail with EAGAIN/EPIPE on write (raw OSError, not
    # aiohttp.ClientError), and reusing it would kill the whole chat.
    session = aiohttp.ClientSession()
    try:
        while waited < max_wait:
            try:
                async with session.get(
                    f"{client.base_url}/v1/threads/{current_thread_id}/poll",
                    params={"since": content_len, "turn": known_turn},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(poll_interval)
                        waited += poll_interval
                        continue
                    data = await resp.json()

                    new_content = data.get("new_content")
                    content_len = data.get("content_len", content_len)
                    completed = data.get("completed", False)
                    msg_id = data.get("message_id")
                    entry_type = data.get("entry_type")
                    tool_name = data.get("tool_name")
                    tool_status = data.get("tool_status")

                    if new_content:
                        # A new message id means the model moved on to the
                        # next message in the turn: a tool call, a new
                        # thinking block, or the final answer.
                        if msg_id != current_msg_id:
                            current_msg_id = msg_id
                            shown = ""
                            if entry_type == "tool_call":
                                # Show tool activity compactly: name and
                                # status only. The full content (tool
                                # results, file dumps) is for the model,
                                # not the terminal.
                                tool_statuses[msg_id] = tool_status or "running"
                                print(f"\n{C.YELLOW}🔧 {tool_name or 'tool call'} [{tool_status or 'running'}]{C.END}", end="", flush=True)
                            elif new_content.lstrip().startswith("<thinking>"):
                                print(f"\n{C.DIM}💭 thinking{C.END}\n", end="", flush=True)
                            elif shown_prev:
                                print()

                        # Tool status transitions update the compact line
                        # instead of diff-printing the whole message.
                        if entry_type == "tool_call":
                            if tool_status != tool_statuses.get(msg_id):
                                tool_statuses[msg_id] = tool_status or "running"
                                print(f" [{tool_status or 'running'}]", end="", flush=True)
                            shown = new_content  # consume; never dump tool output
                            shown_prev = True
                        else:
                            # The server serves the full message content;
                            # diff locally so model edits replace instead
                            # of appending invalid slices.
                            if new_content.startswith(shown):
                                delta = new_content[len(shown):]
                                shown = new_content
                                if delta:
                                    print(delta, end="", flush=True)
                            else:
                                # The model rewrote its message mid-stream.
                                shown = new_content
                                print(f"\n{C.DIM}[revised]{C.END}\n", end="", flush=True)
                                print(shown, end="", flush=True)
                            shown_prev = True
                    else:
                        shown_prev = False

                    if completed:
                        print(f"\n{C.GREEN}✓ Complete{C.END}")
                        print()
                        return

            except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
                # Transport errors (EAGAIN/EPIPE on a stale connection)
                # are transient: drop the poisoned session and retry.
                await session.close()
                session = aiohttp.ClientSession()

            # Prompt for pending tool-call authorizations (ask mode).
            if waited - last_approval_check >= 1.0:
                last_approval_check = waited
                pending = await client.pending_tool_calls()
                for p in pending:
                    tid = p.get("tool_call_id", "")
                    if not tid or tid in seen_approvals:
                        continue
                    seen_approvals.add(tid)
                    name = p.get("tool_name", "?")
                    print(f"\n{C.YELLOW}🔧 [TOOL] {name}{C.END}", flush=True)
                    answer = (await _ask("  allow? [y/N] ")).strip()
                    allow = answer.lower() in ("y", "yes")
                    await client.resolve_tool_call(
                        p.get("platform_thread_id", ""), tid, allow
                    )

            await asyncio.sleep(poll_interval)
            waited += poll_interval
    finally:
        await session.close()

    print(f"\n{C.YELLOW}⚠ Timeout after 120s{C.END}\n")


# ── Stdin reader ─────────────────────────────────────────────────────────

async def chat_session(client: NexClient) -> str | None:
    """Interactive chat loop for the current thread.

    Returns "quit" to exit the program, "exit" to return to the thread
    selection screen, or None."""
    global current_thread_id, show_raw, _chat_queue

    while True:
        text = await _readline()
        if text is None:
            return None

        if text == "/exit":
            print("Returning to thread selection.")
            return "exit"

        if text == "/quit":
            print("Exiting.")
            return "quit"

        elif text == "/new":
            tid = await client.create_thread()
            if tid:
                current_thread_id = tid
                print(f"{C.CYAN}New thread created: {current_thread_id}{C.END}")
            else:
                print(f"{C.YELLOW}Could not create a new thread.{C.END}")

        elif text == "/thread":
            if current_thread_id:
                print(f"Current thread: {C.CYAN}{current_thread_id}{C.END}")
            else:
                print(f"{C.YELLOW}No current thread (a new one will be created){C.END}")

        elif text.startswith("/switch "):
            arg = text[8:].strip()
            if not arg:
                print(f"{C.YELLOW}Usage: /switch <thread_id> or /switch <number>{C.END}")
                continue
            if "-" in arg or arg.startswith("ses_"):
                target = arg
            else:
                try:
                    num = int(arg)
                except ValueError:
                    print(f"{C.RED}Invalid argument: {arg}. Use a thread ID or number.{C.END}")
                    continue
                threads = await client.list_threads()
                if not threads:
                    print(f"{C.YELLOW}No threads available.{C.END}")
                    continue
                # Filter to user threads (acp threads excluded from numbers)
                user_threads = [t for t in threads if t.get("title")]
                if not user_threads:
                    print(f"{C.YELLOW}No user conversations available.{C.END}")
                    continue
                if 1 <= num <= len(user_threads):
                    target = user_threads[num - 1].get("thread_id", user_threads[num - 1].get("id"))
                else:
                    print(f"{C.RED}Invalid thread number: {num}. Available: 1-{len(user_threads)}.{C.END}")
                    continue
            if target is None:
                print(f"{C.RED}No thread ID specified.{C.END}")
                continue
            t = await client.get_thread(target)
            if t is None:
                print(f"{C.RED}Thread '{target}' not found.{C.END}")
            else:
                current_thread_id = target
                print(f"{C.GREEN}Switched to thread: {current_thread_id}{C.END}")

        elif text == "/raw":
            show_raw = not show_raw
            print(f"Raw JSON display: {'ON' if show_raw else 'OFF'}")

        elif text == "/cancel":
            result = await client.cancel()
            if result is not None:
                msg = result.get("message", result.get("status", "Cancelled"))
                print(f"{C.GREEN}✓{C.END} {msg}")
            else:
                print(f"{C.RED}✗{C.END} Cancel request failed")

        elif text.startswith("/files "):
            query = text[7:].strip()
            results = await client.search_files(query)
            if results is None:
                print(f"{C.RED}✗{C.END} File search failed")
            elif not results:
                print(f"{C.YELLOW}No files found for query: {query}{C.END}")
            else:
                print(f"\n{C.BOLD}File search results for '{query}'{C.END}")
                print(f"{C.DIM}{'─' * 72}{C.END}")
                print(f"{C.BOLD}{'Path':<48} {'Size':>8} {'Type':<16}{C.END}")
                print(f"{C.DIM}{'─' * 72}{C.END}")
                for f in results:
                    path = f.get("path", f.get("name", "?"))
                    size = f.get("size", 0)
                    mime = f.get("mime_type", f.get("mime", ""))
                    if isinstance(size, int):
                        size_str = f"{size:>8}"
                    else:
                        size_str = f"{str(size):>8}"
                    print(f"{path:<48} {size_str} {mime:<16}")
                print(f"{C.DIM}{'─' * 72}{C.END}")
                print(f"{len(results)} file(s)\n")

        elif text.startswith("/mention "):
            query = text[9:].strip()
            mention = await client.mention_files(query)
            if mention is None:
                print(f"{C.RED}✗{C.END} File mention lookup failed")
            else:
                print(f"\n{C.BOLD}Mention:{C.END}")
                print(mention)
                print()

        elif text == "/history":
            threads = await client.list_threads()
            if threads is None:
                print(f"{C.RED}✗{C.END} Failed to retrieve threads")
            elif not threads:
                print(f"{C.YELLOW}No conversations found.{C.END}")
            else:
                # Separate user threads (have title = have user messages) from acp threads
                user_threads = [t for t in threads if t.get("title")]
                acp_count = len(threads) - len(user_threads)

                if not user_threads:
                    print(f"{C.YELLOW}No conversations found.{C.END}")
                else:
                    print(f"\n{C.BOLD}Conversations{C.END}")
                    print(f"{C.DIM}{'─' * 90}{C.END}")
                    print(f"{'':<3} {'ID':<15} {'Title':<42} {'Msgs':>5} {'Created':<19}")
                    print(f"{C.DIM}{'─' * 90}{C.END}")
                    for t in user_threads:
                        tid = t.get("thread_id", t.get("id", "?"))
                        short_id = (tid[:12] + "...") if len(tid) > 12 else tid
                        title_raw = t.get("title", "") or tid
                        title = (title_raw[:40] + "...") if len(title_raw) > 40 else title_raw
                        msg_count = t.get("message_count", t.get("num_messages", 0))
                        created = (t.get("created_at", t.get("created", "")) or "")[:19]
                        marker = f"{C.GREEN}▶{C.END}" if tid == current_thread_id else " "
                        print(f"{marker:<3} {short_id:<15} {title:<42} {str(msg_count):>5} {created:<19}")
                    print(f"{C.DIM}{'─' * 90}{C.END}")
                    extra = f"  ({acp_count} internal thread{'s' if acp_count != 1 else ''} not shown)" if acp_count > 0 else ""
                    print(f"{len(user_threads)} conversation(s){extra}  ({C.GREEN}▶{C.END} = current)\n")

        elif text == "/files":
            print(f"{C.YELLOW}Usage: /files <query>{C.END}")

        elif text == "/help":
            print(f"{C.BOLD}Commands:{C.END}")
            print("  /exit          - back to thread selection")
            print("  /quit          - exit the program")
            print("  /new           - create a new thread immediately")
            print("  /thread        - show current thread ID")
            print("  /switch <id>   - switch to a different thread")
            print("  /raw           - toggle raw JSON display")
            print("  /cancel        - cancel in-progress turn")
            print("  /files <query> - search files in workspace")
            print("  /mention <q>   - file mention lookup")
            print("  /history       - list recent threads")
            print("  /help          - show this help")

        elif text.startswith("/"):
            print(f"{C.YELLOW}Unknown command: {text}{C.END}")

        else:
            # Enqueue the message; a dedicated worker sends chats
            # sequentially so the stdin loop stays free for prompts.
            await _chat_queue.put(text)


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=": REST chat client for Rust  server")
    parser.add_argument("--port", type=int, default=9090,
                        help="Server port (default: 9090)")
    parser.add_argument("--workdir", default=os.getcwd(),
                        help="Working directory hint (for --no-zed fallback)")
    parser.add_argument("--no-zed", action="store_true",
                        help="Fallback mode: do not expect Zed to be managed")
    args = parser.parse_args()

    base_url = f"http://localhost:{args.port}"
    workdir = os.path.abspath(args.workdir)

    # Load .env file (informational only)
    env_file = Path(__file__).parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())

    async def async_main():
        global current_thread_id, _chat_queue
        client = NexClient(base_url)

        # Health check
        h = await client.health()
        if h is not None:
            print_banner(h)

        if h and h.get("status") == "ok":
            print(f"  {C.DIM}Server:{C.END} {base_url}")
            print(f"  {C.DIM}Workdir:{C.END} {workdir}")
            print()
            if h.get("zed_connected") and h.get("agent_ready"):
                print(f"{C.BOLD}Enter a message. /exit returns to thread selection.{C.END}")
                print(f"{C.DIM}Example: \"What's in this directory?\"{C.END}")

            _chat_queue = asyncio.Queue()
            asyncio.create_task(_chat_worker(client))
            await _make_reader()

            # Session loop: thread selection → chat → back to selection.
            while True:
                current_thread_id = None
                threads = await client.list_threads()
                choice = await select_thread_prompt(client, threads or [])
                if choice == "quit":
                    break
                if choice:
                    current_thread_id = choice
                    print(f"{C.CYAN}Resumed thread: {current_thread_id}{C.END}")
                    thread_data = await client.get_thread(choice)
                    if thread_data:
                        msgs = thread_data.get("messages", [])
                        for msg in msgs:
                            role = msg.get("role", "?")
                            content = msg.get("content", "")
                            entry_type = msg.get("entry_type", "")
                            tool_name = msg.get("tool_name", "")
                            tool_status = msg.get("tool_status", "")
                            if role == "user":
                                print(f"{C.BOLD}You:{C.END} {content}")
                            elif role == "assistant":
                                if entry_type == "tool_call":
                                    icon = {"completed": "✓", "error": "✗", "in_progress": "..."}.get(tool_status or "", "")
                                    print(f"{C.YELLOW}🔧 {tool_name} {icon}{C.END}")
                                else:
                                    print(f"{C.GREEN}Agent:{C.END} {content}")
                        print()
                else:
                    # New thread: create it immediately.
                    tid = await client.create_thread()
                    if tid:
                        current_thread_id = tid
                        print(f"{C.CYAN}New thread created: {current_thread_id}{C.END}")
                    else:
                        print(f"{C.YELLOW}Could not create a new thread.{C.END}")

                action = await chat_session(client)
                if action == "quit":
                    break
                print(f"\n{C.DIM}Returning to thread selection.{C.END}\n")

        await client.close()

    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print(f"\n{C.YELLOW}Shutdown{C.END}")
    finally:
        print(f"{C.GREEN}Done{C.END}")


if __name__ == "__main__":
    main()
