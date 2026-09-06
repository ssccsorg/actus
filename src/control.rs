// Meta-agent control MCP proxy.
//
// `actus control` is a stdio MCP server that a sessionful agent such as
// telos runs as one of its context servers (a `[[agents.mcp]]` entry).
// Each MCP tool call is translated into an actus HTTP API call on the
// loopback port, carrying the bearer token and an identity header, so
// the actus server can apply the `[agent-control]` allowlist policy.
// The launcher exports ACTUS_AGENT_NAME, ACTUS_HTTP_PORT, and
// ACTUS_API_TOKEN to agent processes; the proxy inherits them.

use anyhow::{anyhow, Result};
use serde_json::{json, Value};

/// MCP protocol version spoken by this server.
pub const PROTOCOL_VERSION: &str = "2024-11-05";

/// Declared tool names, in listing order.
pub const TOOL_NAMES: [&str; 6] = [
    "agent_list",
    "agent_submit",
    "agent_poll",
    "agent_wait",
    "agent_cancel",
    "agent_thread",
];

fn tool_description(name: &str) -> &'static str {
    match name {
        "agent_list" => "List registered agents with their kind, readiness, and capabilities.",
        "agent_submit" => "Submit one message to another agent; returns the thread id and task id.",
        "agent_poll" => "Poll one turn of another agent; completed turns carry the new content.",
        "agent_wait" => {
            "Submit nothing: poll a running turn until it completes or the timeout expires."
        }
        "agent_cancel" => "Cancel the running turn of another agent, or one request by id.",
        "agent_thread" => "Read one thread of another agent with its full message history.",
        _ => "",
    }
}

fn tool_schema(name: &str) -> Value {
    let object = |required: &[&str], props: Value| json!({ "type": "object", "properties": props, "required": required });
    match name {
        "agent_list" => object(&[], json!({})),
        "agent_submit" => object(
            &["agent", "message"],
            json!({
                "agent": {"type": "string", "description": "target agent name"},
                "message": {"type": "string", "description": "prompt to run"},
                "thread_id": {"type": "string", "description": "resume an existing thread (optional)"},
                "parent_thread_id": {"type": "string", "description": "the meta agent's own thread id, recorded as the parent of the new thread"}
            }),
        ),
        "agent_poll" => object(
            &["agent", "thread_id"],
            json!({
                "agent": {"type": "string"},
                "thread_id": {"type": "string"}
            }),
        ),
        "agent_wait" => object(
            &["agent", "thread_id"],
            json!({
                "agent": {"type": "string"},
                "thread_id": {"type": "string"},
                "timeout_secs": {"type": "integer", "description": "max seconds to wait (default 300, max 3600)"}
            }),
        ),
        "agent_cancel" => object(
            &["agent"],
            json!({
                "agent": {"type": "string"},
                "request_id": {"type": "string", "description": "cancel one request instead of the whole turn"}
            }),
        ),
        "agent_thread" => object(
            &["agent", "thread_id"],
            json!({
                "agent": {"type": "string"},
                "thread_id": {"type": "string"}
            }),
        ),
        _ => object(&[], json!({})),
    }
}

/// MCP `tools/list` result.
pub fn tool_definitions() -> Value {
    let tools: Vec<Value> = TOOL_NAMES
        .iter()
        .map(|name| {
            json!({
                "name": name,
                "description": tool_description(name),
                "inputSchema": tool_schema(name)
            })
        })
        .collect();
    json!({ "tools": tools })
}

fn reply(id: &Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn error_reply(id: &Value, code: i64, message: String) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

/// Handle one MCP request without network access. Returns None for
/// notifications (no id). Tool calls go through `dispatch`, which maps a
/// tool name and its arguments to either a text result or an error text.
pub fn handle_request(
    req: &Value,
    dispatch: &mut dyn FnMut(&str, &Value) -> Result<String, String>,
) -> Option<Value> {
    let id = req.get("id")?;
    let method = req.get("method")?.as_str()?;
    let params = req.get("params").cloned().unwrap_or_else(|| json!({}));
    match method {
        "initialize" => Some(reply(
            id,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "actus-control", "version": env!("CARGO_PKG_VERSION") }
            }),
        )),
        "ping" => Some(reply(id, json!({}))),
        "tools/list" => Some(reply(id, tool_definitions())),
        "tools/call" => {
            let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let args = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            if !TOOL_NAMES.contains(&name) {
                return Some(error_reply(id, -32602, format!("unknown tool: {name}")));
            }
            let outcome = match dispatch(name, &args) {
                Ok(text) => {
                    json!({ "content": [{ "type": "text", "text": text }], "isError": false })
                }
                Err(text) => {
                    json!({ "content": [{ "type": "text", "text": text }], "isError": true })
                }
            };
            Some(reply(id, outcome))
        }
        other => Some(error_reply(
            id,
            -32601,
            format!("method not found: {other}"),
        )),
    }
}

/// Run the stdio MCP proxy until stdin closes. Called by the `actus
/// control` subcommand.
pub async fn run_control_proxy() -> Result<()> {
    let controller = std::env::var("ACTUS_AGENT_NAME").map_err(|_| {
        anyhow!("ACTUS_AGENT_NAME is not set; the actus launcher exports it to agent processes")
    })?;
    let port = std::env::var("ACTUS_HTTP_PORT").unwrap_or_else(|_| "9090".to_string());
    let token = match std::env::var("ACTUS_API_TOKEN") {
        Ok(t) if !t.is_empty() => t,
        _ => {
            let file = dirs::home_dir()
                .ok_or_else(|| anyhow!("cannot determine home directory"))?
                .join(".actus")
                .join("api_token");
            std::fs::read_to_string(&file)
                .map_err(|e| {
                    anyhow!(
                        "ACTUS_API_TOKEN unset and cannot read {}: {}",
                        file.display(),
                        e
                    )
                })?
                .trim()
                .to_string()
        }
    };
    if token.is_empty() {
        return Err(anyhow!("empty API token; cannot reach the actus server"));
    }
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
    let mut line = String::new();
    loop {
        line.clear();
        let read = tokio::io::AsyncBufReadExt::read_line(&mut stdin, &mut line)
            .await
            .map_err(|e| anyhow!("reading stdin: {e}"))?;
        if read == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let req: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("actus-control: skipping unparseable line: {e}");
                continue;
            }
        };
        let response = if req.get("id").is_some()
            && req.get("method").and_then(|m| m.as_str()) == Some("tools/call")
        {
            let params = req.get("params").cloned().unwrap_or_else(|| json!({}));
            let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let args = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            if !TOOL_NAMES.contains(&name) {
                Some(error_reply(
                    req.get("id").unwrap(),
                    -32602,
                    format!("unknown tool: {name}"),
                ))
            } else {
                let id = req.get("id").unwrap().clone();
                match call_tool(&client, &base, &token, &controller, name, &args).await {
                    Ok(text) => Some(reply(
                        &id,
                        json!({ "content": [{ "type": "text", "text": text }], "isError": false }),
                    )),
                    Err(text) => Some(reply(
                        &id,
                        json!({ "content": [{ "type": "text", "text": text }], "isError": true }),
                    )),
                }
            }
        } else {
            let mut unreachable =
                |_: &str, _: &Value| Err("network dispatch unreachable".to_string());
            handle_request(&req, &mut unreachable)
        };
        if let Some(resp) = response {
            let mut out = serde_json::to_string(&resp).map_err(|e| anyhow!("encoding: {e}"))?;
            out.push('\n');
            tokio::io::AsyncWriteExt::write_all(&mut stdout, out.as_bytes())
                .await
                .map_err(|e| anyhow!("writing stdout: {e}"))?;
            tokio::io::AsyncWriteExt::flush(&mut stdout)
                .await
                .map_err(|e| anyhow!("flushing stdout: {e}"))?;
        }
    }
    Ok(())
}

fn str_arg(args: &Value, name: &str) -> Result<String, String> {
    args.get(name)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("missing or empty argument: {name}"))
}

fn thread_arg(args: &Value) -> Result<String, String> {
    let tid = str_arg(args, "thread_id")?;
    if tid.contains('/') || tid.contains('?') || tid.contains('#') {
        return Err(format!("invalid thread id: {tid}"));
    }
    Ok(tid)
}

/// One authenticated API call with the controller identity header.
async fn api(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    controller: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> Result<String, String> {
    let mut request = client
        .request(method, format!("{base}{path}"))
        .bearer_auth(token)
        .header("x-actus-controller", controller);
    if let Some(b) = body {
        request = request.json(&b);
    }
    let response = request
        .send()
        .await
        .map_err(|e| format!("request to actus failed: {e}"))?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if status.is_success() {
        Ok(text)
    } else {
        Err(format!("actus returned HTTP {}: {}", status.as_u16(), text))
    }
}

/// Execute one control tool against the actus HTTP API.
async fn call_tool(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    controller: &str,
    name: &str,
    args: &Value,
) -> Result<String, String> {
    match name {
        "agent_list" => {
            api(
                client,
                base,
                token,
                controller,
                reqwest::Method::GET,
                "/health",
                None,
            )
            .await
        }
        "agent_submit" => {
            let agent = str_arg(args, "agent")?;
            let message = str_arg(args, "message")?;
            let mut body = json!({ "agent": agent, "message": message });
            if let Some(tid) = args.get("thread_id").and_then(|v| v.as_str()) {
                body["thread_id"] = json!(tid);
            }
            if let Some(pid) = args.get("parent_thread_id").and_then(|v| v.as_str()) {
                body["parent_thread_id"] = json!(pid);
            }
            api(
                client,
                base,
                token,
                controller,
                reqwest::Method::POST,
                "/v1/chat/async",
                Some(body),
            )
            .await
        }
        "agent_poll" | "agent_thread" => {
            let agent = str_arg(args, "agent")?;
            let tid = thread_arg(args)?;
            let path = if name == "agent_poll" {
                format!("/v1/threads/{tid}/poll?agent={agent}")
            } else {
                format!("/v1/threads/{tid}?agent={agent}")
            };
            api(
                client,
                base,
                token,
                controller,
                reqwest::Method::GET,
                &path,
                None,
            )
            .await
        }
        "agent_wait" => {
            let agent = str_arg(args, "agent")?;
            let tid = thread_arg(args)?;
            let requested = args
                .get("timeout_secs")
                .and_then(|v| v.as_u64())
                .unwrap_or(300)
                .clamp(1, 3600);
            let path = format!("/v1/threads/{tid}/poll?agent={agent}");
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(requested);
            loop {
                let text = api(
                    client,
                    base,
                    token,
                    controller,
                    reqwest::Method::GET,
                    &path,
                    None,
                )
                .await?;
                if let Ok(body) = serde_json::from_str::<Value>(&text) {
                    if body.get("completed").and_then(|v| v.as_bool()) == Some(true) {
                        return Ok(text);
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!("agent_wait timed out after {requested}s"));
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
        "agent_cancel" => {
            let agent = str_arg(args, "agent")?;
            let mut body = json!({ "agent": agent });
            if let Some(rid) = args.get("request_id").and_then(|v| v.as_str()) {
                body["request_id"] = json!(rid);
            }
            api(
                client,
                base,
                token,
                controller,
                reqwest::Method::POST,
                "/v1/cancel",
                Some(body),
            )
            .await
        }
        other => Err(format!("unknown tool: {other}")),
    }
}
