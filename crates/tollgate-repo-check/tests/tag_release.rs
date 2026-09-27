//! `scripts/tag-release.sh` tags the commit its notes describe (GL-142).
//!
//! v0.29.0 was prepared on the release branch, then a fix merged to the
//! default branch, then the stale release merge request landed on top. The
//! tag went on the release *merge*, so it contained the fix while its
//! CHANGELOG section omitted it — and `prepare-release` listed the fix under
//! 0.29.1. This rebuilds that interleaving in a scratch repository and runs
//! the script in its dry-run mode.
use std::path::Path;
use std::process::Command;

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@example.com")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@example.com")
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("utf-8")
        .trim()
        .to_owned()
}

fn write(dir: &Path, path: &str, body: &str) {
    let full = dir.join(path);
    std::fs::create_dir_all(full.parent().expect("has a parent")).expect("dirs");
    std::fs::write(full, body).expect("write");
}

fn manifest(version: &str) -> String {
    format!("[workspace.package]\nversion = \"{version}\"\n")
}

#[test]
fn a_stale_release_merge_tags_the_release_commit_not_the_merge() {
    let scratch = tempfile::tempdir().expect("scratch repository");
    let dir = scratch.path();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/tag-release.sh");
    write(
        dir,
        "scripts/tag-release.sh",
        &std::fs::read_to_string(script).expect("the script exists"),
    );
    git(dir, &["init", "-q", "-b", "main"]);
    write(dir, "Cargo.toml", &manifest("0.1.0"));
    write(dir, "CHANGELOG.md", "# Changelog\n\n## [Unreleased]\n");
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", "feat: the released change"]);

    // Preparation: the release branch bumps the manifest and writes the notes.
    git(dir, &["switch", "-q", "-c", "chore/release"]);
    write(dir, "Cargo.toml", &manifest("0.2.0"));
    write(
        dir,
        "CHANGELOG.md",
        "# Changelog\n\n## [Unreleased]\n\n## [0.2.0] - 2026-09-25\n\n### Added\n\n- the released change\n",
    );
    git(dir, &["commit", "-q", "-am", "chore: release v0.2.0"]);
    let release_commit = git(dir, &["rev-parse", "HEAD"]);

    // A fix lands on the default branch before the release merge request does.
    git(dir, &["switch", "-q", "main"]);
    write(dir, "fix.txt", "fixed\n");
    git(dir, &["add", "-A"]);
    git(
        dir,
        &["commit", "-q", "-m", "fix: landed after preparation"],
    );

    // The stale release merge request lands on top of it.
    git(
        dir,
        &[
            "merge",
            "-q",
            "--no-ff",
            "-m",
            "Merge branch 'chore/release'",
            "chore/release",
        ],
    );
    let release_merge = git(dir, &["rev-parse", "HEAD"]);

    let output = Command::new("sh")
        .arg("scripts/tag-release.sh")
        .current_dir(dir)
        .env("TAG_RELEASE_DRY_RUN", "1")
        .output()
        .expect("the script runs");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains(&format!("would release v0.2.0 at {release_commit}")),
        "the tag goes on the release commit, whose tree the notes describe: {stdout}"
    );
    assert!(
        stdout.contains(&format!("landed by {release_merge}")),
        "and only once a merge on the first-parent history landed it: {stdout}"
    );
    // The release commit does not contain the fix; the merge does. The fix
    // belongs to the next release, which is where preparation lists it.
    let tagged_files = git(dir, &["ls-tree", "--name-only", &release_commit]);
    assert!(!tagged_files.contains("fix.txt"), "{tagged_files}");
    let merged_files = git(dir, &["ls-tree", "--name-only", &release_merge]);
    assert!(merged_files.contains("fix.txt"), "{merged_files}");
}

#[test]
fn an_unmerged_release_commit_is_not_tagged() {
    let scratch = tempfile::tempdir().expect("scratch repository");
    let dir = scratch.path();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/tag-release.sh");
    write(
        dir,
        "scripts/tag-release.sh",
        &std::fs::read_to_string(script).expect("the script exists"),
    );
    git(dir, &["init", "-q", "-b", "main"]);
    write(dir, "Cargo.toml", &manifest("0.2.0"));
    write(
        dir,
        "CHANGELOG.md",
        "# Changelog\n\n## [0.2.0] - 2026-09-25\n\n- notes\n",
    );
    git(dir, &["add", "-A"]);
    // A release commit committed straight to the branch, never merged.
    git(dir, &["commit", "-q", "-m", "chore: release v0.2.0"]);
    let output = Command::new("sh")
        .arg("scripts/tag-release.sh")
        .current_dir(dir)
        .env("TAG_RELEASE_DRY_RUN", "1")
        .output()
        .expect("the script runs");
    assert!(
        !output.status.success(),
        "a release no merge landed is refused"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("no first-parent merge introduces"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
