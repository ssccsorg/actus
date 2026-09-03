// Context mention sources for the `@` mention feature.
//
// Telos's mention picker offers files, symbols, threads, rules, and fetch as
// prompt-injection context. This module implements the server-side sources
// that do not need a language server: definition-pattern symbol search and
// project rule file discovery.

use std::path::Path;

use serde::Serialize;

/// A code symbol located by definition-pattern search.
#[derive(Debug, Clone, Serialize)]
pub struct SymbolEntry {
    pub file: String,
    pub line: usize,
    pub snippet: String,
}

/// A project rule file (AGENTS.md or *.mdc).
#[derive(Debug, Clone, Serialize)]
pub struct RuleFile {
    pub path: String,
    pub content: String,
}

/// File extensions scanned by the symbol search.
const SOURCE_EXTENSIONS: &[&str] = &[
    "rs", "py", "js", "ts", "jsx", "tsx", "go", "java", "c", "cpp", "h", "hpp", "rb", "swift",
    "kt", "php", "sh", "zig", "lua",
];

/// Definition keywords used to approximate "this line declares a symbol".
/// Substring match keeps the search dependency-free and fast enough for
/// interactive mention resolution.
const SYMBOL_KEYWORDS: &[&str] = &[
    "fn ", "pub fn", "struct ", "enum ", "trait ", "impl ", "class ", "def ", "func ",
    "function ", "const ", "let ", "interface ",
];

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

/// Search source files under `workdir` for definition-like lines matching
/// `query`. Respects .gitignore. Skips files larger than 1 MiB.
pub fn search_symbols(workdir: &Path, query: &str, max_results: usize) -> Vec<SymbolEntry> {
    let q = query.to_lowercase();
    if q.is_empty() {
        return Vec::new();
    }
    let max = if max_results > 0 { max_results } else { 20 };

    let mut walk = ignore::WalkBuilder::new(workdir);
    walk.standard_filters(true);
    // Respect .gitignore even when the workdir is not a git repository.
    walk.require_git(false);

    let mut results = Vec::new();
    for entry in walk.build().flatten() {
        if results.len() >= max {
            break;
        }
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        if !SOURCE_EXTENSIONS.contains(&ext.as_str()) {
            continue;
        }
        let content = match std::fs::read_to_string(path) {
            Ok(c) if c.len() <= 1_000_000 => c,
            _ => continue,
        };
        let relative = path
            .strip_prefix(workdir)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string();
        for (i, line) in content.lines().enumerate() {
            let lower = line.to_lowercase();
            if !lower.contains(&q) {
                continue;
            }
            let is_definition = SYMBOL_KEYWORDS.iter().any(|k| lower.contains(k));
            if !is_definition {
                continue;
            }
            results.push(SymbolEntry {
                file: relative.clone(),
                line: i + 1,
                snippet: truncate(line.trim(), 160),
            });
            if results.len() >= max {
                break;
            }
        }
    }
    results
}

/// Find project rule files (`AGENTS.md` or `*.mdc`) under `workdir`,
/// depth-limited. Returns their contents truncated to a context-safe size.
pub fn find_rules(workdir: &Path) -> Vec<RuleFile> {
    let mut walk = ignore::WalkBuilder::new(workdir);
    walk.standard_filters(true);
    // Rules conventionally live in hidden directories (.github, .cursor,
    // .telos), so hidden entries must be searched; .git is pruned.
    walk.hidden(false);
    walk.require_git(false);
    walk.filter_entry(|e| e.file_name() != std::ffi::OsStr::new(".git"));
    walk.max_depth(Some(6));

    let mut rules = Vec::new();
    for entry in walk.build().flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        if name != "AGENTS.md" && !name.ends_with(".mdc") {
            continue;
        }
        let content = std::fs::read_to_string(path).unwrap_or_default();
        if content.trim().is_empty() {
            continue;
        }
        let relative = path
            .strip_prefix(workdir)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string();
        rules.push(RuleFile {
            path: relative,
            content: truncate(&content, 20_000),
        });
    }
    rules.sort_by(|a, b| a.path.cmp(&b.path));
    rules
}
