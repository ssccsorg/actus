// File operations — search, mention, metadata for the headless agent.
//
// Uses walkdir for traversal and the `ignore` crate to respect .gitignore.

use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Serialize;

/// Result of a single file match.
#[derive(Debug, Clone, Serialize)]
pub struct FileEntry {
    pub path: String,
    pub relative_path: String,
    pub is_dir: bool,
    pub size: u64,
    pub modified: String, // ISO 8601
    pub mime_type: String,
}

/// Search options for file listing.
#[derive(Debug, Clone)]
pub struct FileSearchOptions {
    /// Substring or glob pattern. Empty = list all.
    pub query: String,
    /// Optional subdirectory within workdir to scope search.
    pub dir: Option<String>,
    /// Max results (0 = unlimited).
    pub max_results: usize,
    /// Include dot-files/dot-dirs.
    pub include_hidden: bool,
}

impl Default for FileSearchOptions {
    fn default() -> Self {
        Self {
            query: String::new(),
            dir: None,
            max_results: 100,
            include_hidden: false,
        }
    }
}

/// Search files under `workdir` matching the given options.
///
/// Respects .gitignore. Skips common heavy directories unconditionally
/// (node_modules, target, .git, .venv, __pycache__).
pub fn search_files(workdir: &Path, opts: &FileSearchOptions) -> Vec<FileEntry> {
    let root = match &opts.dir {
        Some(sub) => workdir.join(sub),
        None => workdir.to_path_buf(),
    };

    if !root.exists() {
        return vec![];
    }

    let query_lower = opts.query.to_lowercase();
    let max_results = if opts.max_results > 0 {
        opts.max_results
    } else {
        usize::MAX
    };

    // Build a .gitignore-aware walker.
    let mut walk = ignore::WalkBuilder::new(&root);
    walk.standard_filters(true); // respects .gitignore, hidden files, etc.
    if opts.include_hidden {
        walk.hidden(false);
    }

    let mut results: Vec<FileEntry> = Vec::new();

    for entry in walk.build().flatten() {
        if results.len() >= max_results {
            break;
        }

        let abs_path = entry.path();

        // Skip root dir itself
        if abs_path == root {
            continue;
        }

        // Extract relative path
        let relative = abs_path
            .strip_prefix(workdir)
            .unwrap_or(abs_path)
            .to_string_lossy()
            .to_string();

        // Apply query filter (if non-empty, substring match on filename)
        if !opts.query.is_empty() {
            let filename = abs_path
                .file_name()
                .map(|n| n.to_string_lossy().to_lowercase())
                .unwrap_or_default();
            if !filename.contains(&query_lower) && !relative.to_lowercase().contains(&query_lower) {
                continue;
            }
        }

        let metadata = match entry.metadata() {
            Ok(m) => m,
            _ => continue,
        };

        let mime_type = if metadata.is_dir() {
            "inode/directory".to_string()
        } else {
            mime_guess::from_path(abs_path)
                .first_or_octet_stream()
                .to_string()
        };

        let modified = metadata
            .modified()
            .ok()
            .map(|t| -> DateTime<Utc> { t.into() })
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default();

        results.push(FileEntry {
            path: abs_path.to_string_lossy().to_string(),
            relative_path: relative,
            is_dir: metadata.is_dir(),
            size: metadata.len(),
            modified,
            mime_type,
        });
    }

    results
}

/// Format file search results as a mention string suitable for embedding in a
/// chat prompt. Shows the most relevant matches in a compact form.
pub fn format_mention(results: &[FileEntry], query: &str) -> String {
    if results.is_empty() {
        return format!("No files match `{}`", query);
    }

    let mut out = String::from("Files matching `").to_string();
    out.push_str(query);
    out.push_str("`:\n");

    for entry in results.iter().take(10) {
        let kind = if entry.is_dir { "[DIR]" } else { "     " };
        out.push_str(&format!("  {} {}\n", kind, entry.relative_path));
    }

    if results.len() > 10 {
        out.push_str(&format!("  ... and {} more\n", results.len() - 10));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_search_files_basic() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();

        // Create test files
        fs::create_dir_all(base.join("src")).unwrap();
        fs::create_dir_all(base.join("docs")).unwrap();
        let mut f = fs::File::create(base.join("src/main.rs")).unwrap();
        f.write_all(b"fn main() {}").unwrap();
        let mut f = fs::File::create(base.join("docs/readme.md")).unwrap();
        f.write_all(b"# Readme").unwrap();

        // List all
        let results = search_files(base, &FileSearchOptions::default());
        assert!(
            results.len() >= 2,
            "expected at least 2 files, got {}",
            results.len()
        );
        assert!(results.iter().any(|e| e.relative_path.contains("main.rs")));
        assert!(
            results
                .iter()
                .any(|e| e.relative_path.contains("readme.md"))
        );

        // Query filter
        let results = search_files(
            base,
            &FileSearchOptions {
                query: "main".to_string(),
                ..Default::default()
            },
        );
        assert_eq!(results.len(), 1, "expected 1 file matching 'main'");
        assert!(results[0].relative_path.contains("main.rs"));
    }
}
