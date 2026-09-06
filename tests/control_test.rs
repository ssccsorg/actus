// MCP framing tests for the `actus control` proxy (issue #19). The
// network dispatch half is covered by the HTTP policy tests in
// `ext_cli_server_test.rs`; here the request/response framing of the
// stdio MCP server is pinned without a server.

use actus::control::{handle_request, tool_definitions, PROTOCOL_VERSION, TOOL_NAMES};
use serde_json::{json, Value};

/// Stub dispatcher recording calls; returns an error for agent_submit
/// without a message argument so both outcome paths are reachable.
fn stub_dispatch(
    calls: &mut Vec<String>,
) -> impl FnMut(&str, &Value) -> Result<String, String> + '_ {
    move |name: &str, args: &Value| {
        calls.push(name.to_string());
        if name == "agent_submit" && args.get("message").is_none() {
            Err("missing message".to_string())
        } else {
            Ok(format!("{{\"tool\": \"{name}\"}}"))
        }
    }
}

#[test]
fn initialize_returns_protocol_and_server_info() {
    let req = json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {} }
    });
    let mut calls = Vec::new();
    let resp = handle_request(&req, &mut stub_dispatch(&mut calls)).unwrap();
    assert_eq!(resp["id"], 1);
    assert_eq!(resp["result"]["protocolVersion"], PROTOCOL_VERSION);
    assert_eq!(resp["result"]["serverInfo"]["name"], "actus-control");
    assert!(calls.is_empty());
}

#[test]
fn ping_returns_empty_result() {
    let req = json!({ "jsonrpc": "2.0", "id": 2, "method": "ping" });
    let mut calls = Vec::new();
    let resp = handle_request(&req, &mut stub_dispatch(&mut calls)).unwrap();
    assert_eq!(resp["result"], json!({}));
}

#[test]
fn tools_list_declares_all_control_tools() {
    let req = json!({ "jsonrpc": "2.0", "id": 3, "method": "tools/list" });
    let mut calls = Vec::new();
    let resp = handle_request(&req, &mut stub_dispatch(&mut calls)).unwrap();
    let tools = tool_definitions();
    assert_eq!(resp["result"], tools);
    let names: Vec<&str> = tools["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, TOOL_NAMES.to_vec());
    for t in tools["tools"].as_array().unwrap() {
        assert!(t["description"].as_str().unwrap().len() > 10);
        assert!(t["inputSchema"]["type"] == "object");
    }
}

#[test]
fn notification_without_id_is_ignored() {
    let req = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
    let mut calls = Vec::new();
    assert!(handle_request(&req, &mut stub_dispatch(&mut calls)).is_none());
}

#[test]
fn tools_call_success_and_error_paths() {
    let mut calls = Vec::new();
    let ok = json!({
        "jsonrpc": "2.0", "id": 4, "method": "tools/call",
        "params": { "name": "agent_list", "arguments": {} }
    });
    let resp = handle_request(&ok, &mut stub_dispatch(&mut calls)).unwrap();
    assert_eq!(resp["result"]["isError"], false);
    assert_eq!(
        resp["result"]["content"][0]["text"],
        r#"{"tool": "agent_list"}"#
    );

    let err = json!({
        "jsonrpc": "2.0", "id": 5, "method": "tools/call",
        "params": { "name": "agent_submit", "arguments": { "agent": "ante" } }
    });
    let resp = handle_request(&err, &mut stub_dispatch(&mut calls)).unwrap();
    assert_eq!(resp["result"]["isError"], true);
    assert_eq!(resp["result"]["content"][0]["text"], "missing message");
    assert_eq!(
        calls,
        vec!["agent_list".to_string(), "agent_submit".to_string()]
    );
}

#[test]
fn unknown_tool_and_method_are_protocol_errors() {
    let mut calls = Vec::new();
    let tool = json!({
        "jsonrpc": "2.0", "id": 6, "method": "tools/call",
        "params": { "name": "agent_nope", "arguments": {} }
    });
    let resp = handle_request(&tool, &mut stub_dispatch(&mut calls)).unwrap();
    assert_eq!(resp["error"]["code"], -32602);
    assert!(resp["error"]["message"]
        .as_str()
        .unwrap()
        .contains("unknown tool"));
    assert!(calls.is_empty(), "dispatch must not run for unknown tools");

    let method = json!({ "jsonrpc": "2.0", "id": 7, "method": "tools/nope" });
    let resp = handle_request(&method, &mut stub_dispatch(&mut calls)).unwrap();
    assert_eq!(resp["error"]["code"], -32601);
}
