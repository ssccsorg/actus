// Git operations — status, diff, log for the headless agent.
//
// Spawns `git` CLI commands (must be in PATH) on the workdir.

use std::path::Path;

use serde::Serialize;

/// Git status of the working tree.
#[derive(Debug, Clone, Serialize)]
pub struct GitStatus {
    pub branch: String,
    pub ahead: usize,
    pub behind: usize,
    pub staged: Vec<String>,
    pub modified: Vec<String>,
    pub untracked: Vec<String>,
    pub conflicts: Vec<String>,
}

/// A single commit entry.
#[derive(Debug, Clone, Serialize)]
pub struct GitCommit {
    pub hash: String,
    pub author: String,
    pub date: String,
    pub message: String,
}

/// Run `git` command in the given directory, return stdout.
fn git(args: &[&str], workdir: &Path) -> Result<String, String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(workdir)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .map_err(|e| format!("Failed to run git: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git {}: {}", args.join(" "), stderr.trim()));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Get git status. Returns Ok(None) if not a git repository.
pub fn get_status(workdir: &Path) -> Result<Option<GitStatus>, String> {
    // Check if git repo
    if git(&["rev-parse", "--is-inside-work-tree"], workdir).is_err() {
        return Ok(None);
    }

    let branch = git(&["rev-parse", "--abbrev-ref", "HEAD"], workdir)
        .unwrap_or_default()
        .trim()
        .to_string();

    // Count ahead/behind
    let ahead_behind = git(&["rev-list", "--count", "--left-right", "@{u}...HEAD"], workdir).ok();
    let (ahead, behind) = match ahead_behind {
        Some(ref s) => {
            let parts: Vec<&str> = s.trim().split('\t').collect();
            let behind = parts.first().and_then(|p| p.trim().parse().ok()).unwrap_or(0);
            let ahead = parts.get(1).and_then(|p| p.trim().parse().ok()).unwrap_or(0);
            (ahead, behind)
        }
        None => (0, 0),
    };

    // Porcelain status
    let porcelain = git(&["status", "--porcelain"], workdir).unwrap_or_default();

    let mut staged = Vec::new();
    let mut modified = Vec::new();
    let mut untracked = Vec::new();
    let mut conflicts = Vec::new();

    for line in porcelain.lines() {
        if line.len() < 3 {
            continue;
        }
        let (xy, path) = line.split_at(2);
        let path = path.trim();
        match xy.trim() {
            "??" => untracked.push(path.to_string()),
            "DD" | "AU" | "UD" | "UA" | "DU" | "AA" | "UU" => conflicts.push(path.to_string()),
            _ => {
                if xy.contains('M') || xy.contains('A') || xy.contains('D') || xy.contains('R') || xy.contains('C') {
                    // First char = staged, second char = working tree
                    if xy.as_bytes()[0] != b' ' && xy.as_bytes()[0] != b'?' {
                        staged.push(path.to_string());
                    }
                    if xy.as_bytes().get(1).copied() != Some(b' ') {
                        modified.push(path.to_string());
                    }
                }
            }
        }
    }

    Ok(Some(GitStatus {
        branch,
        ahead,
        behind,
        staged,
        modified,
        untracked,
        conflicts,
    }))
}

/// Get git diff for the working tree (unstaged changes).
pub fn get_diff(workdir: &Path, staged: bool) -> Result<Option<String>, String> {
    if git(&["rev-parse", "--is-inside-work-tree"], workdir).is_err() {
        return Ok(None);
    }

    let mut args = vec!["diff", "--no-color"];
    if staged {
        args.push("--cached");
    }
    git(&args, workdir).map(Some)
}

/// Get recent git log.
pub fn get_log(workdir: &Path, max_count: usize) -> Result<Option<Vec<GitCommit>>, String> {
    if git(&["rev-parse", "--is-inside-work-tree"], workdir).is_err() {
        return Ok(None);
    }

    let count = max_count.min(100).max(1).to_string();
    let output = git(&[
        "log",
        &format!("--max-count={}", count),
        "--format=%H%n%an%n%aI%n%s%n---",
    ], workdir)?;

    let mut commits = Vec::new();
    for entry in output.split("\n---\n") {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let lines: Vec<&str> = entry.lines().collect();
        if lines.len() >= 4 {
            commits.push(GitCommit {
                hash: lines[0].to_string(),
                author: lines[1].to_string(),
                date: lines[2].to_string(),
                message: lines[3].to_string(),
            });
        }
    }

    Ok(Some(commits))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_git_not_a_repo() {
        let dir = tempfile::tempdir().unwrap();
        let status = get_status(dir.path()).unwrap();
        assert!(status.is_none(), "non-repo should return None");
    }

    #[test]
    fn test_git_repo_status() {
        let dir = tempfile::tempdir().unwrap();
        let repo_path = dir.path();

        // Init git repo
        git(&["init"], repo_path).unwrap();
        git(&["config", "user.email", "test@test.com"], repo_path).unwrap();
        git(&["config", "user.name", "Test"], repo_path).unwrap();

        // Create and commit a file
        let mut f = fs::File::create(repo_path.join("readme.md")).unwrap();
        f.write_all(b"# Test").unwrap();
        git(&["add", "."], repo_path).unwrap();
        git(&["commit", "-m", "Initial commit"], repo_path).unwrap();

        // Status should be clean
        let status = get_status(repo_path).unwrap().unwrap();
        assert_eq!(status.branch, "main");
        assert!(status.modified.is_empty());

        // Modify and check
        let mut f = fs::File::create(repo_path.join("readme.md")).unwrap();
        f.write_all(b"# Modified").unwrap();
        let status = get_status(repo_path).unwrap().unwrap();
        assert_eq!(status.modified.len(), 1);
    }
}
