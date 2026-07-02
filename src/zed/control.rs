// WebSocket control — server loop and event dispatch for Zed headless communication.
//
// Manages the WebSocket connection between actus (server) and the Zed headless
// process (client). The connection loop handles:
// - Accepting incoming WS connections from Zed
// - Forwarding commands (chat_message, cancel) from actus to Zed
// - Receiving events (message_added, message_completed) from Zed
// - Automatic reconnection when the WS drops
//
// This replaces helixml/zed's external_websocket_sync::websocket_sync module
// with a simpler, single-runtime implementation.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use crate::server::WsCommandTx;
use crate::zed::ZedManager;

/// Run the WebSocket server that accepts connections from the Zed headless process.
///
/// Connection lifecycle:
///   1. Listen on ws_host
///   2. Accept connection from Zed
///   3. Set up send/receive channels
///   4. Process events until disconnect
///   5. On disconnect, accept the next connection (Zed auto-reconnects)
pub async fn run_ws_server(
    ws_host: &str,
    zed_manager: Arc<RwLock<ZedManager>>,
    ws_tx: WsCommandTx,
) -> anyhow::Result<()> {
    let port = ws_host.split(':').nth(1).unwrap_or("8080");
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;
    tracing::info!(
        "WebSocket server listening on ws://127.0.0.1:{}",
        listener.local_addr()?.port()
    );

    // Connection loop: accept → read → disconnect → accept again
    loop {
        let (stream, peer) = listener.accept().await?;
        tracing::info!("Zed connecting from {}", peer);

        let ws_stream = accept_async(stream).await?;
        let (mut write, mut read) = ws_stream.split();

        // Increment reconnect counter and mark connected
        {
            let mut mgr = zed_manager.write().await;
            mgr.reconnect_count += 1;
            mgr.zed_connected = true;
            mgr.agent_ready = false;
            tracing::info!(
                "Zed WebSocket connected (reconnect #{})",
                mgr.reconnect_count
            );
        }

        // Create channels: main command + resend (for reconnection retry)
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let tx_for_shared = tx.clone();
        let (resend_tx, mut resend_rx) = mpsc::unbounded_channel::<String>();

        // Register command sender
        {
            let mut mgr = zed_manager.write().await;
            mgr.set_ws_tx(tx);
        }
        {
            let mut tx_guard = ws_tx.lock().await;
            *tx_guard = Some(tx_for_shared);
        }
        tracing::info!("Zed WebSocket re-established");

        // Resend any pending messages from before the disconnect
        {
            let mgr = zed_manager.read().await;
            for (rid, tid, cmd) in &mgr.pending_chat_queue {
                tracing::info!(
                    "Resending pending message request_id={}, thread_id={}",
                    &rid[..rid.len().min(12)],
                    &tid[..tid.len().min(12)]
                );
                let _ = resend_tx.send(cmd.clone());
            }
        }

        // Clone for spawned tasks
        let zed_manager_for_read = zed_manager.clone();
        let _ws_tx_for_read = ws_tx.clone();

        // Write handle: forward commands from both normal and resend channels
        let write_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(cmd) = rx.recv() => {
                        if let Err(e) = write.send(Message::Text(cmd.into())).await {
                            tracing::error!("WebSocket write error: {}", e);
                            break;
                        }
                    },
                    Some(cmd) = resend_rx.recv() => {
                        if let Err(e) = write.send(Message::Text(cmd.into())).await {
                            tracing::error!("WebSocket resend error: {}", e);
                            break;
                        }
                    },
                }
            }
        });

        // Read handle: process incoming events with periodic health check
        let read_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = read.next() => {
                        if !handle_ws_message(&zed_manager_for_read, msg).await {
                            break;
                        }
                    },
                    // Health check: break if zed_connect was cleared by the
                    // health monitor (forces reconnection loop iteration).
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {
                        if !zed_manager_for_read.read().await.zed_connected {
                            break;
                        }
                    },
                }
            }
        });

        read_handle.await?;
        write_handle.abort();

        // Clear command sender on disconnect
        {
            let mut guard = ws_tx.lock().await;
            *guard = None;
        }

        tracing::info!("WS connection lost, waiting for next connection...");
    }
}

/// Process a single WebSocket message from Zed.
/// Returns false if the connection should be closed.
async fn handle_ws_message(
    zed_manager: &Arc<RwLock<ZedManager>>,
    msg: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
) -> bool {
    match msg {
        Some(Ok(Message::Text(text))) => {
            tracing::debug!("WS recv ({} bytes): {}", text.len(), &text[..text.len().min(200)]);
            handle_zed_event(zed_manager, &text).await;
            true
        }
        Some(Ok(Message::Binary(data))) => {
            if let Ok(text) = String::from_utf8(data.to_vec()) {
                handle_zed_event(zed_manager, &text).await;
            } else {
                tracing::warn!("Non-UTF-8 binary WS message ({} bytes)", data.len());
            }
            true
        }
        Some(Ok(Message::Ping(_))) => true,
        Some(Ok(Message::Close(_))) => {
            tracing::info!("Zed WebSocket closed");
            zed_manager.write().await.zed_connected = false;
            false
        }
        Some(Err(e)) => {
            tracing::error!("WebSocket read error: {}", e);
            zed_manager.write().await.zed_connected = false;
            false
        }
        None => {
            tracing::info!("WebSocket stream ended");
            zed_manager.write().await.zed_connected = false;
            false
        }
        _ => true,
    }
}

/// Dispatch a received JSON event to the appropriate handler.
async fn handle_zed_event(zed_manager: &Arc<RwLock<ZedManager>>, text: &str) {
    tracing::debug!("WS event: {}", &text[..text.len().min(200)]);

    let msg: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("Failed to parse WS event: {}", e);
            return;
        }
    };

    let event_type = msg
        .get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    tracing::debug!("WS event type: '{}'", event_type);

    // Update event timestamp for health monitor.
    // Ping only updates ping time; all others update SSE event time
    // so the monitor can detect stalled event flow.
    {
        let mut mgr = zed_manager.write().await;
        if event_type == "ping" {
            mgr.last_ping_time = Instant::now();
        } else {
            mgr.last_sse_event_time = Instant::now();
        }
    }

    let data = msg
        .get("data")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    match event_type {
        "ping" => {}
        "agent_ready" => {
            let mut mgr = zed_manager.write().await;
            mgr.agent_ready = true;
            tracing::info!(
                "Agent ready ({})",
                data.get("agent_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?")
            );
        }
        "thread_created" => {
            let acp_id = data
                .get("acp_thread_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let rid = data
                .get("request_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let mut mgr = zed_manager.write().await;
            tracing::info!("Thread created: {}", acp_id);

            // Map request_id → local thread_id, and acp_thread_id → local thread_id
            if let Some(local_id) = mgr.pending_requests.get(&rid).cloned() {
                mgr.thread_id_map
                    .insert(acp_id.clone(), local_id.clone());
                if let Some(local_thread) = mgr.threads.get_mut(&local_id) {
                    local_thread.acp_thread_id = Some(acp_id.clone());
                }
                if let Some(waiter) = mgr.thread_waiters.remove(&local_id) {
                    waiter.notify_one();
                }
            }
            mgr.get_or_create_thread(Some(&acp_id));
            mgr.pending_requests.insert(rid, acp_id);
            mgr.notify_thread_change();
            mgr.save_threads();
        }
        "message_added" => {
            let acp_id = data
                .get("acp_thread_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let content = data
                .get("content")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let role = data
                .get("role")
                .and_then(|v| v.as_str())
                .unwrap_or("assistant")
                .to_string();
            let msg_id = data.get("message_id").and_then(|v| v.as_str()).map(|s| s.to_string());
            let entry_type = data.get("entry_type").and_then(|v| v.as_str()).map(|s| s.to_string());
            let tool_name = data.get("tool_name").and_then(|v| v.as_str()).map(|s| s.to_string());
            let tool_status = data.get("tool_status").and_then(|v| v.as_str()).map(|s| s.to_string());

            tracing::debug!(
                "message_added: acp_id={}, role={}, content_len={}",
                &acp_id[..acp_id.len().min(12)],
                role,
                content.len()
            );

            let mut mgr = zed_manager.write().await;
            mgr.add_message_full(
                &acp_id, &role, &content, msg_id.clone(),
                entry_type.clone(), tool_name.clone(), tool_status.clone(),
            );
            // Mirror to the local thread
            if let Some(local_id) = mgr.thread_id_map.get(&acp_id).cloned() {
                mgr.add_message_full(
                    &local_id, &role, &content, msg_id,
                    entry_type, tool_name, tool_status,
                );
            }
            mgr.notify_thread_change();
            mgr.save_threads();
        }
        "message_completed" => {
            let acp_id = data
                .get("acp_thread_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let mut mgr = zed_manager.write().await;
            if let Some(thread) = mgr.threads.get_mut(&acp_id) {
                thread.completed = true;
                thread.turn_completed = thread.turn_completed.wrapping_add(1);
            }
            if let Some(local_id) = mgr.thread_id_map.get(&acp_id).cloned() {
                if let Some(thread) = mgr.threads.get_mut(&local_id) {
                    thread.completed = true;
                    thread.turn_completed = thread.turn_completed.wrapping_add(1);
                }
            }
            mgr.notify_thread_change();
            mgr.save_threads();
            tracing::info!(
                "Message complete for thread {}",
                &acp_id[..acp_id.len().min(12)]
            );
        }
        "chat_response_error" => {
            let error = data.get("error").and_then(|v| v.as_str()).unwrap_or("?");
            tracing::error!("Chat response error: {}", error);
        }
        _ => {
            tracing::debug!("Unhandled WS event type: {}", event_type);
        }
    }
}
