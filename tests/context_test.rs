// Integration tests for context mention sources (symbols, rules).

use actus::context::{find_rules, search_symbols};
use std::fs;
use std::io::Write;

#[test]
fn symbol_search_finds_definitions() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();

    fs::create_dir_all(base.join("src")).unwrap();
    let mut f = fs::File::create(base.join("src/main.rs")).unwrap();
    f.write_all(
        b"pub fn main() {}\nfn helper() {}\nstruct Foo {}\n// some comment about helper\n",
    )
    .unwrap();
    let mut f = fs::File::create(base.join("src/lib.py")).unwrap();
    f.write_all(b"def compute(x):\n    return x\nclass Widget:\n    pass\n").unwrap();

    // Query matches only definition lines, not comments
    let syms = search_symbols(base, "helper", 20);
    assert_eq!(syms.len(), 1, "only the fn line should match, got {syms:?}");
    assert_eq!(syms[0].file, "src/main.rs");
    assert_eq!(syms[0].line, 2);
    assert!(syms[0].snippet.contains("fn helper"));

    let syms = search_symbols(base, "widget", 20);
    assert_eq!(syms.len(), 1);
    assert_eq!(syms[0].file, "src/lib.py");
    assert!(syms[0].snippet.contains("class Widget"));

    // Empty query returns nothing
    assert!(search_symbols(base, "", 20).is_empty());
}

#[test]
fn symbol_search_respects_gitignore() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    fs::write(base.join(".gitignore"), "ignored.rs\n").unwrap();
    fs::write(base.join("kept.rs"), "fn kept() {}\n").unwrap();
    fs::write(base.join("ignored.rs"), "fn ignored() {}\n").unwrap();

    let syms = search_symbols(base, "fn", 20);
    assert!(
        syms.iter().any(|s| s.file == "kept.rs"),
        "kept.rs should be found"
    );
    assert!(
        !syms.iter().any(|s| s.file == "ignored.rs"),
        "ignored.rs should be skipped by .gitignore"
    );
}

#[test]
fn rules_finds_agents_and_mdc() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();

    fs::create_dir_all(base.join(".github")).unwrap();
    fs::write(base.join("AGENTS.md"), "# Agent rules\n\nBe concise.\n").unwrap();
    fs::write(base.join(".github/rules.mdc"), "Always run tests.\n").unwrap();
    fs::write(base.join("README.md"), "Not a rule.\n").unwrap();
    fs::write(base.join("empty.mdc"), "   \n").unwrap();

    let rules = find_rules(base);
    let paths: Vec<String> = rules.iter().map(|r| r.path.clone()).collect();
    assert!(paths.iter().any(|p| p == "AGENTS.md"));
    assert!(paths.iter().any(|p| p == ".github/rules.mdc"));
    assert!(
        !paths.iter().any(|p| p == "README.md"),
        "README is not a rule"
    );
    assert_eq!(rules.len(), 2, "empty.mdc must be skipped");
}
