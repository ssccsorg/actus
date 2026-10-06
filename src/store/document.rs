//! The record as one document.
//!
//! This is the default store and the behavior actus had before the seam existed: every
//! thread with its messages in one JSON file, written whole. It is what a host that
//! configures nothing gets, so the seam costs that host nothing.
//!
//! The repairs below belong to this store rather than to the seam. They fix a document an
//! earlier version of this store wrote, and a store that keeps records cannot hold the
//! shapes they repair: a record cannot change, so it cannot carry a duplicate.

use std::collections::HashMap;
use std::path::PathBuf;

use super::RecordStore;
use crate::agent::{ThreadMessage, ThreadSession};
use crate::util::truncate_utf8;

pub struct DocumentStore {
    path: PathBuf,
}

impl DocumentStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl RecordStore for DocumentStore {
    fn describe(&self) -> String {
        format!("document at {}", self.path.display())
    }

    fn load(&self) -> Result<HashMap<String, ThreadSession>, String> {
        if !self.path.exists() {
            return Ok(HashMap::new());
        }
        let content = match std::fs::read_to_string(&self.path) {
            Ok(content) => content,
            Err(e) => {
                tracing::error!("Failed to read threads file {}: {}", self.path.display(), e);
                return Ok(HashMap::new());
            }
        };
        let mut threads = match serde_json::from_str::<HashMap<String, ThreadSession>>(&content) {
            Ok(threads) => threads,
            Err(e) => {
                tracing::error!(
                    "Failed to deserialize threads from {}: {}",
                    self.path.display(),
                    e
                );
                return Ok(HashMap::new());
            }
        };
        backfill_titles(&mut threads);
        drop_duplicate_message_ids(&mut threads);
        repair_turn_counter_drift(&mut threads);
        tracing::info!(
            "Loaded {} threads from {}",
            threads.len(),
            self.path.display()
        );
        Ok(threads)
    }

    fn persist(&self, threads: HashMap<String, ThreadSession>) -> Result<(), String> {
        let json =
            serde_json::to_string_pretty(&threads).map_err(|e| format!("serialize threads: {e}"))?;
        std::fs::write(&self.path, json).map_err(|e| format!("write {}: {e}", self.path.display()))
    }
}

/// Backfill titles for threads saved before the title field existed.
fn backfill_titles(threads: &mut HashMap<String, ThreadSession>) {
    for thread in threads.values_mut() {
        if thread.title.is_none() {
            if let Some(first_user) = thread.messages.iter().find(|m| m.role == "user") {
                let content = first_user.content.trim();
                let mut truncated = truncate_utf8(content, 80).to_string();
                if content.len() > 80 {
                    truncated.push_str("...");
                }
                thread.title = Some(truncated);
            }
        }
    }
}

/// Repair a historical bug where streaming updates to the same message id were appended
/// instead of replaced (Telos emits thinking, tool call, and answer messages with
/// interleaved ids), swelling a turn into dozens of duplicate entries. Keep the last
/// occurrence of each id and drop the earlier duplicates; user messages and id-less
/// entries are kept as-is.
fn drop_duplicate_message_ids(threads: &mut HashMap<String, ThreadSession>) {
    for thread in threads.values_mut() {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut kept: Vec<ThreadMessage> = Vec::with_capacity(thread.messages.len());
        for m in std::mem::take(&mut thread.messages) {
            match &m.message_id {
                Some(id) if !seen.insert(id.clone()) => {
                    tracing::warn!(
                        "Dropping duplicate message id {} in thread {}",
                        id,
                        thread.id
                    );
                }
                _ => kept.push(m),
            }
        }
        thread.messages = kept;
    }
}

/// Repair a historical index drift: an errored or cancelled turn used to bump
/// turn_completed without adding an assistant message, so persisted threads can have
/// turn_completed > assistant-message count. Poll and SSE address assistant messages by
/// turn index, so the counter must not point past them.
fn repair_turn_counter_drift(threads: &mut HashMap<String, ThreadSession>) {
    for thread in threads.values_mut() {
        let text_assistants = thread
            .messages
            .iter()
            .filter(|m| m.role == "assistant" && m.entry_type.as_deref() != Some("tool_call"))
            .count();
        if thread.turn_completed > text_assistants as u64 {
            tracing::warn!(
                "Repairing turn_completed {} -> {} ({} assistant msgs) for thread {}",
                thread.turn_completed,
                text_assistants,
                text_assistants,
                thread.id
            );
            thread.turn_completed = text_assistants as u64;
        }
    }
}
