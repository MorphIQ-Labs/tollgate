//! `scripts/tag-release.sh` tags the commit its notes describe (GL-142).
//!
//! On GitLab, v0.29.0's tag went on the release *merge*, which also carried a
//! fix that reached the default branch after preparation: the tag contained
//! the fix while its CHANGELOG section omitted it. On GitHub, releases land by
//! squash on an up-to-date branch, so the release is one commit on the
//! first-parent history and later commits sit above it. These tests rebuild
//! those histories in scratch repositories and run the script in its dry-run
//! mode.
use std::path::Path;
use std::process::{Command, Output};

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@example.com")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@example.com")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
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

/// A repository holding the script, with 0.1.0 released and one change since.
fn repository() -> tempfile::TempDir {
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
    write(
        dir,
        "CHANGELOG.md",
        "# Changelog\n\n## [0.1.0]\n\n- first\n",
    );
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", "feat: the first change"]);
    git(dir, &["tag", "v0.1.0"]);
    write(dir, "src.txt", "released change\n");
    git(dir, &["add", "-A"]);
    git(
        dir,
        &["commit", "-q", "-m", "feat: the released change (#4)"],
    );
    scratch
}

/// The release as `prepare-release` writes it: the version bump and its
/// CHANGELOG section, in one commit with `subject`.
fn release(dir: &Path, subject: &str) -> String {
    write(dir, "Cargo.toml", &manifest("0.2.0"));
    write(
        dir,
        "CHANGELOG.md",
        "# Changelog\n\n## [0.2.0]\n\n- the released change (#4)\n\n## [0.1.0]\n\n- first\n",
    );
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", subject]);
    git(dir, &["rev-parse", "HEAD"])
}

fn tag_release(dir: &Path) -> Output {
    Command::new("sh")
        .arg("scripts/tag-release.sh")
        .current_dir(dir)
        .env("TAG_RELEASE_DRY_RUN", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .expect("the script runs")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn a_squashed_release_is_tagged_at_its_own_commit_not_at_head() {
    let scratch = repository();
    let dir = scratch.path();
    let release_commit = release(dir, "chore: release v0.2.0 (#5)");
    // A fix lands after the release: it belongs to the next release, not v0.2.0.
    write(dir, "src.txt", "a later fix\n");
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", "fix: a later fix (#6)"]);

    let output = tag_release(dir);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        stdout(&output).trim(),
        format!("tag-release: would release v0.2.0 at {release_commit}")
    );
}

#[test]
fn a_release_subject_without_a_pull_request_number_is_recognised() {
    let scratch = repository();
    let dir = scratch.path();
    let release_commit = release(dir, "chore: release v0.2.0");

    let output = tag_release(dir);
    assert!(output.status.success(), "{output:?}");
    assert!(
        stdout(&output).contains(&format!("would release v0.2.0 at {release_commit}")),
        "{output:?}"
    );
}

#[test]
fn a_version_with_no_release_commit_is_not_tagged() {
    let scratch = repository();
    let dir = scratch.path();
    // The manifest is bumped by something other than a release commit.
    release(dir, "chore: bump the version by hand (#5)");

    let output = tag_release(dir);
    assert!(!output.status.success(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("refusing to tag HEAD"),
        "{output:?}"
    );
}

#[test]
fn a_release_commit_off_the_first_parent_history_is_not_tagged() {
    let scratch = repository();
    let dir = scratch.path();
    // The release commit exists, but only on a branch that was merged in: the
    // default branch's own history never recorded it as a release.
    git(dir, &["switch", "-q", "-c", "side"]);
    release(dir, "chore: release v0.2.0 (#5)");
    git(dir, &["switch", "-q", "main"]);
    git(
        dir,
        &[
            "merge",
            "-q",
            "--no-ff",
            "-m",
            "merge the side branch",
            "side",
        ],
    );

    let output = tag_release(dir);
    assert!(!output.status.success(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("first-parent history"),
        "{output:?}"
    );
}

#[test]
fn an_already_tagged_version_is_left_alone() {
    let scratch = repository();
    let dir = scratch.path();
    release(dir, "chore: release v0.2.0 (#5)");
    git(dir, &["tag", "v0.2.0"]);

    let output = tag_release(dir);
    assert!(output.status.success(), "{output:?}");
    assert!(
        stdout(&output).contains("v0.2.0 already exists; nothing to do"),
        "{output:?}"
    );
}
