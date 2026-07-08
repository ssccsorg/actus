#!/usr/bin/env python3
"""
-server: REST API server for headless Zed AI agent.

Provides HTTP API for multi-thread, multi-agent conversations
with async task queue and approval gates.

Usage:
  export LLM_API_KEY="sk-xxx"
  python3 -server.py

API:
  POST /v1/chat              - Send message, stream response (SSE)
  POST /v1/chat/async        - Send message, return task_id
  GET  /v1/tasks/:id         - Get task status/result
  GET  /v1/threads           - List threads
  GET  /v1/threads/:id       - Get thread messages
  POST /v1/approve/:id       - Approve pending task
  GET  /health               - Health check
"""

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import uvicorn

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import StreamingResponse
    from pydantic import BaseModel
except ImportError:
    print("Installing fastapi/uvicorn...")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "fastapi", "uvicorn", "sse-starlette"]
    )
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import StreamingResponse
    from pydantic import BaseModel

try:
    import websockets
except ImportError:
    print("Installing websockets...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
    import websockets

# ── Config ──────────────────────────────────────────────────────────────

HOST = "127.0.0.1"
WS_PORT = 8080
HTTP_PORT = 9090

# ── Load .env ───────────────────────────────────────────────────────────

env_file = Path(__file__).parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

api_key = os.environ.get("LLM_API_KEY")
if not api_key:
    print("ERROR: LLM_API_KEY env var required")
    sys.exit(1)

# ── Thread Manager ──────────────────────────────────────────────────────
# Manages multiple independent Zed WebSocket sessions (threads).


class ThreadSession:
    """A single conversation thread connected to Zed via WebSocket."""

    def __init__(self, thread_id: str):
        self.thread_id = thread_id
        self.messages: list[dict] = []
        self.created_at = datetime.now(timezone.utc)
        self.ws: Any = None
        self._pending_requests: dict[str, asyncio.Future] = {}

    def add_message(self, role: str, content: str, msg_id: str = ""):
        entry = {
            "role": role,
            "content": content,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if msg_id:
            entry["message_id"] = msg_id
        self.messages.append(entry)


class ThreadManager:
    """Manages all active thread sessions."""

    def __init__(self):
        self._threads: dict[str, ThreadSession] = {}
        self._ws: Any = None
        self._ready = asyncio.Event()

    @property
    def ws(self):
        return self._ws

    @ws.setter
    def ws(self, value):
        self._ws = value

    def get_or_create(self, thread_id: str = "") -> ThreadSession:
        if not thread_id:
            thread_id = str(uuid.uuid4())
        if thread_id not in self._threads:
            self._threads[thread_id] = ThreadSession(thread_id)
        return self._threads[thread_id]

    def get(self, thread_id: str) -> Optional[ThreadSession]:
        return self._threads.get(thread_id)

    def list(self) -> list[dict]:
        return [
            {
                "id": t.thread_id,
                "messages": len(t.messages),
                "created_at": t.created_at.isoformat(),
            }
            for t in self._threads.values()
        ]

    def remove(self, thread_id: str):
        self._threads.pop(thread_id, None)


threads = ThreadManager()

# ── Task Queue + Approval Gate ─────────────────────────────────────────


class Task:
    PENDING = "pending"
    APPROVED = "approved"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    AWAITING_APPROVAL = "awaiting_approval"

    def __init__(self, task_type: str, params: dict):
        self.id = str(uuid.uuid4())
        self.type = task_type
        self.params = params
        self.status = self.PENDING
        self.result: Optional[str] = None
        self.error: Optional[str] = None
        self.created_at = datetime.now(timezone.utc)
        self.completed_at: Optional[datetime] = None


class TaskQueue:
    """Async task queue with approval gates."""

    def __init__(self):
        self._tasks: dict[str, Task] = {}
        self._approval_queue: asyncio.Queue = asyncio.Queue()

    def create(self, task_type: str, params: dict) -> Task:
        task = Task(task_type, params)
        self._tasks[task.id] = task
        return task

    def get(self, task_id: str) -> Optional[Task]:
        return self._tasks.get(task_id)

    def list(self, status: str = "") -> list[dict]:
        tasks = self._tasks.values()
        if status:
            tasks = [t for t in tasks if t.status == status]
        return [
            {
                "id": t.id,
                "type": t.type,
                "status": t.status,
                "created_at": t.created_at.isoformat(),
                "completed_at": t.completed_at.isoformat() if t.completed_at else None,
            }
            for t in sorted(tasks, key=lambda t: t.created_at, reverse=True)
        ]

    def approve(self, task_id: str) -> bool:
        task = self._tasks.get(task_id)
        if not task or task.status != Task.AWAITING_APPROVAL:
            return False
        task.status = Task.APPROVED
        return True

    async def process_approvals(self):
        """Background worker: processes approved tasks."""
        while True:
            task_id = await self._approval_queue.get()
            task = self._tasks.get(task_id)
            if task and task.status == Task.AWAITING_APPROVAL:
                task.status = Task.PENDING
                # Will be picked up by process_tasks


tasks_q = TaskQueue()

# ── WebSocket Handler (Zed 연결) ──────────────────────────────────────


async def handle_zed_connection(websocket):
    """Handle incoming WebSocket connection from Zed."""
    threads.ws = websocket
    peer = websocket.remote_address
    print(f"[-server] Zed connected ({peer})")

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
                await handle_zed_message(msg)
            except json.JSONDecodeError:
                pass
    except websockets.exceptions.ConnectionClosed:
        print("[-server] Zed connection closed")
    finally:
        threads.ws = None


async def handle_zed_message(msg: dict):
    """Process events from Zed."""
    event_type = msg.get("event_type", "")
    data = msg.get("data", {})

    if event_type == "ping":
        if threads.ws:
            await threads.ws.send(json.dumps({"type": "pong", "data": data}))

    elif event_type == "agent_ready":
        threads._ready.set()
        print(f"[-server] Agent ready ({data.get('agent_name', '?')})")

    elif event_type == "thread_created":
        acp_id = data.get("acp_thread_id", "")
        sess = threads.get_or_create(acp_id)
        print(f"[-server] Thread created: {acp_id}")

    elif event_type == "message_added":
        acp_id = data.get("acp_thread_id", "")
        content = data.get("content", "")
        role = data.get("role", "assistant")
        msg_id = data.get("message_id", "")
        sess = threads.get(acp_id)
        if sess:
            # Update latest message or append
            if sess.messages and sess.messages[-1].get("message_id") == msg_id:
                sess.messages[-1]["content"] = content
            else:
                sess.add_message(role, content, msg_id)

    elif event_type == "message_completed":
        acp_id = data.get("acp_thread_id", "")
        print(f"[-server] Message complete for thread {acp_id[:12]}")

    elif event_type == "chat_response_error":
        print(f"[-server] Error: {data.get('error', '?')}")


async def send_to_zed(message: str, thread_id: str = "") -> str:
    """Send a chat_message to Zed. Returns the acp_thread_id."""
    if not threads.ws:
        raise HTTPException(status_code=503, detail="Zed not connected")

    if not thread_id:
        thread_id = str(uuid.uuid4())

    request_id = str(uuid.uuid4())
    cmd = {
        "type": "chat_message",
        "data": {
            "message": message,
            "request_id": request_id,
            "acp_thread_id": thread_id if thread_id != uuid.UUID(thread_id).hex else None,
        },
    }

    # If thread_id is a new UUID, send null for new thread creation
    try:
        uuid.UUID(thread_id)
        cmd["data"]["acp_thread_id"] = None
    except ValueError:
        pass

    await threads.ws.send(json.dumps(cmd))
    return thread_id


# ── FastAPI App ────────────────────────────────────────────────────────

app = FastAPI(title="-server", version="0.1.0")


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "zed_connected": threads.ws is not None,
        "agent_ready": threads._ready.is_set(),
        "active_threads": len(threads._threads),
        "pending_tasks": len(tasks_q.list(status=Task.PENDING)),
    }


class ChatRequest(BaseModel):
    message: str
    thread_id: Optional[str] = None
    stream: bool = True


@app.post("/v1/chat")
async def chat(req: ChatRequest):
    """Send a message and stream the response (SSE)."""
    if not threads.ws:
        raise HTTPException(status_code=503, detail="Zed not connected")

    thread_id = req.thread_id or str(uuid.uuid4())
    is_new = req.thread_id is None

    # Register thread
    sess = threads.get_or_create(thread_id)
    sess.add_message("user", req.message)

    # Build command
    request_id = str(uuid.uuid4())
    cmd = {
        "type": "chat_message",
        "data": {
            "message": req.message,
            "request_id": request_id,
            "acp_thread_id": None if is_new else thread_id,
        },
    }

    await threads.ws.send(json.dumps(cmd))

    async def event_stream():
        # We poll the thread session for new message content
        last_len = 0
        yield f"data: {json.dumps({'event': 'thread_created', 'thread_id': thread_id})}\n\n"

        while True:
            await asyncio.sleep(0.1)
            if sess.messages:
                last_msg = sess.messages[-1]
                if last_msg.get("message_id", "") == request_id or True:
                    content = last_msg["content"]
                    if len(content) > last_len:
                        new_text = content[last_len:]
                        last_len = len(content)
                        yield f"data: {json.dumps({'event': 'message_added', 'content': new_text})}\n\n"

            # Check for completion (simplified: wait until we get a complete msg)
            if False:  # Placeholder
                break

    return StreamingResponse(event_stream(), media_type="text/event-stream")


class AsyncChatRequest(BaseModel):
    message: str
    thread_id: Optional[str] = None
    require_approval: bool = False


@app.post("/v1/chat/async")
async def chat_async(req: AsyncChatRequest):
    """Send a message asynchronously. Returns a task_id."""
    task = tasks_q.create(
        "chat",
        {
            "message": req.message,
            "thread_id": req.thread_id,
            "require_approval": req.require_approval,
        },
    )

    if req.require_approval:
        task.status = Task.AWAITING_APPROVAL
    else:
        task.status = Task.APPROVED
        # Start execution in background
        asyncio.create_task(execute_task(task))

    return {"task_id": task.id, "status": task.status, "thread_id": req.thread_id}


@app.get("/v1/tasks/{task_id}")
async def get_task(task_id: str):
    task = tasks_q.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return {
        "id": task.id,
        "type": task.type,
        "status": task.status,
        "result": task.result,
        "error": task.error,
        "created_at": task.created_at.isoformat(),
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
    }


@app.get("/v1/tasks")
async def list_tasks(status: str = ""):
    return {"tasks": tasks_q.list(status)}


@app.post("/v1/approve/{task_id}")
async def approve_task(task_id: str):
    if tasks_q.approve(task_id):
        task = tasks_q.get(task_id)
        assert task is not None
        asyncio.create_task(execute_task(task))
        return {"status": "approved"}
    raise HTTPException(status_code=404, detail="Task not found or not awaiting approval")


@app.get("/v1/threads")
async def list_threads():
    return {"threads": threads.list()}


@app.get("/v1/threads/{thread_id}")
async def get_thread(thread_id: str):
    sess = threads.get(thread_id)
    if not sess:
        raise HTTPException(status_code=404, detail="Thread not found")
    return {
        "id": sess.thread_id,
        "created_at": sess.created_at.isoformat(),
        "messages": sess.messages,
    }


async def execute_task(task: Task):
    """Execute a task by sending to Zed."""
    try:
        task.status = Task.RUNNING
        params = task.params
        thread_id = params.get("thread_id", "")
        message = params["message"]

        # Send message to Zed
        rid = str(uuid.uuid4())
        cmd = {
            "type": "chat_message",
            "data": {
                "message": message,
                "request_id": rid,
                "acp_thread_id": thread_id or None,
            },
        }

        if threads.ws:
            await threads.ws.send(json.dumps(cmd))
            task.result = f"Sent (request_id={rid})"
        else:
            task.error = "Zed not connected"
            task.status = Task.FAILED
            return

        task.status = Task.COMPLETED
    except Exception as e:
        task.status = Task.FAILED
        task.error = str(e)
    finally:
        task.completed_at = datetime.now(timezone.utc)


# ── Settings bootstrap ──────────────────────────────────────────────────


def ensure_zed_settings(data_dir: str, api_key: str):
    settings_dir = Path(data_dir) / "config"
    settings_dir.mkdir(parents=True, exist_ok=True)
    settings_file = settings_dir / "settings.json"

    settings = {}
    if settings_file.exists():
        try:
            settings = json.loads(settings_file.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    language_models = settings.setdefault("language_models", {})
    openai_compatible = language_models.setdefault("openai_compatible", {})

    if "deepseek" not in openai_compatible:
        openai_compatible["deepseek"] = {
            "api_url": "https://api.deepseek.com/v1",
            "available_models": [
                {
                    "name": "deepseek-chat",
                    "display_name": "DeepSeek V3",
                    "max_tokens": 65536,
                    "max_output_tokens": 8192,
                    "tool_use": True,
                }
            ],
        }

    creds_dir = Path(data_dir) / "credentials"
    creds_dir.mkdir(parents=True, exist_ok=True)
    creds_file = creds_dir / "credentials.json"

    creds = {}
    if creds_file.exists():
        try:
            creds = json.loads(creds_file.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    creds["provider/deepseek"] = {"api_key": api_key}

    settings_file.write_text(json.dumps(settings, indent=2))
    creds_file.write_text(json.dumps(creds, indent=2))


# ── Main ────────────────────────────────────────────────────────────────


async def main():
    import argparse

    parser = argparse.ArgumentParser(description="-server")
    parser.add_argument("--bin", default=None, help="helix-zed-headless binary path")
    parser.add_argument("--workdir", default=os.getcwd(), help="Working directory")
    parser.add_argument("--http-port", type=int, default=HTTP_PORT, help="HTTP API port")
    parser.add_argument("--ws-port", type=int, default=WS_PORT, help="WebSocket port")
    args = parser.parse_args()

    workdir = os.path.abspath(args.workdir)

    # Find binary
    bin_path = args.bin
    if not bin_path:
        candidates = [
            os.path.expanduser("~/.bin/helix-zed-headless-arm64"),
            os.path.join(os.path.dirname(__file__), "..", ".bin", "helix-zed-headless-arm64"),
        ]
        for p in candidates:
            if os.path.exists(p):
                bin_path = os.path.abspath(p)
                break
        if not bin_path:
            print("ERROR: helix-zed-headless binary not found")
            sys.exit(1)

    # Bootstrap Zed settings
    user_data_dir = tempfile.mkdtemp(prefix="-")
    assert api_key is not None
    ensure_zed_settings(user_data_dir, api_key)
    session_id = f"ses-actus-{uuid.uuid4().hex[:8]}"

    print("[-server] Starting...")
    print(f"  Binary:     {bin_path}")
    print(f"  Workdir:    {workdir}")
    print(f"  HTTP API:   http://{HOST}:{args.http_port}")
    print(f"  WebSocket:  ws://{HOST}:{args.ws_port}")

    # Start Zed
    subprocess.run(["pkill", "-f", "helix-zed-headless"], capture_output=True)
    time.sleep(1)

    env = os.environ.copy()
    env.update(
        {
            "ZED_EXTERNAL_SYNC_ENABLED": "true",
            "ZED_WEBSOCKET_SYNC_ENABLED": "true",
            "ZED_HELIX_URL": f"{HOST}:{args.ws_port}",
            "ZED_HELIX_TOKEN": "test-token",
            "HELIX_SESSION_ID": session_id,
            "ZED_STATELESS": "1",
            "RUST_LOG": "info",
        }
    )

    zed_proc = subprocess.Popen(
        [
            bin_path,
            "--headless",
            "--allow-multiple-instances",
            "--user-data-dir",
            user_data_dir,
            workdir,
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=open("/tmp/-headless.log", "w"),
    )
    print(f"  Zed PID:    {zed_proc.pid}")
    print()

    # Start WebSocket server for Zed to connect to
    ws_server = await websockets.serve(
        handle_zed_connection,
        HOST,
        args.ws_port,
        process_request=lambda path, headers: None,
    )

    # Start FastAPI server
    config = uvicorn.Config(
        app,
        host=HOST,
        port=args.http_port,
        log_level="info",
    )
    server = uvicorn.Server(config)

    print("[-server] Ready. Waiting for Zed to connect...")

    # Run both servers concurrently
    await asyncio.gather(server.serve(), ws_server.wait_closed())


if __name__ == "__main__":
    asyncio.run(main())
