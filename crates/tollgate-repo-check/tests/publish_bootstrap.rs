//! New-name publishing must not replace existing crates' trusted credentials.
#![cfg(unix)]
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Output},
};

fn executable(path: &Path, source: &str) {
    fs::write(path, source).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}
fn run(new_name: bool, bootstrap: bool, dry_run: bool) -> (Output, String) {
    let scratch = tempfile::tempdir().unwrap();
    let root = scratch.path();
    fs::create_dir(root.join("scripts")).unwrap();
    fs::create_dir(root.join("bin")).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        "[workspace.package]\nversion = \"0.33.0\"\n",
    )
    .unwrap();
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/publish-crates.sh"),
        root.join("scripts/publish-crates.sh"),
    )
    .unwrap();
    executable(
        &root.join("bin/cargo"),
        r#"#!/bin/sh
if [ "$1" = metadata ]; then
  printf '{"packages":['
  separator=''
  for crate in tollgate-core tollgate-auth tollgate-store tollgate-admission tollgate-store-postgres tollgate-client tollgate-server tollgate-axum; do
    printf '%s{"name":"%s","publish":null}' "$separator" "$crate"
    separator=,
  done
  printf ']}\n'
else
  # The only credentials here are public fixture labels, never real tokens.
  printf '%s %s\n' "$4" "$CARGO_REGISTRY_TOKEN" >> "$FIXTURE_LOG"
fi
"#,
    );
    executable(
        &root.join("bin/curl"),
        r#"#!/bin/sh
for url in "$@"; do :; done
case "$url" in
  */tollgate-axum) if [ "$FIXTURE_NEW_NAME" = 1 ]; then printf 404; else printf 200; fi ;;
  *) printf 404 ;;
esac
"#,
    );
    let log = root.join("publish.log");
    let mut paths = vec![root.join("bin")];
    paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
    let mut command = Command::new("sh");
    command
        .arg("scripts/publish-crates.sh")
        .current_dir(root)
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("FIXTURE_LOG", &log)
        .env("FIXTURE_NEW_NAME", if new_name { "1" } else { "0" })
        .env("PUBLISH_DRY_RUN", if dry_run { "1" } else { "0" })
        .env("CARGO_REGISTRY_TOKEN", "fixture-trusted")
        .env_remove("TOLLGATE_AXUM_BOOTSTRAP_TOKEN");
    if bootstrap {
        command.env("TOLLGATE_AXUM_BOOTSTRAP_TOKEN", "fixture-bootstrap");
    }
    let output = command.output().unwrap();
    (output, fs::read_to_string(log).unwrap_or_default())
}

#[test]
fn missing_bootstrap_stops_before_any_partial_release() {
    let (output, log) = run(true, false, false);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("First tollgate-axum publication"));
    assert!(log.is_empty());
}
#[test]
fn bootstrap_is_used_only_for_the_new_crate_name() {
    let (output, log) = run(true, true, false);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(log.lines().count(), 8);
    for line in log.lines() {
        let (name, token) = line.split_once(' ').unwrap();
        assert_eq!(
            token,
            if name == "tollgate-axum" {
                "fixture-bootstrap"
            } else {
                "fixture-trusted"
            }
        );
    }
}
#[test]
fn an_existing_name_uses_trusted_publishing_even_if_bootstrap_remains() {
    let (output, log) = run(false, true, false);
    assert!(output.status.success());
    assert_eq!(log.lines().count(), 8);
    assert!(log.lines().all(|line| line.ends_with("fixture-trusted")));
}
#[test]
fn package_dry_run_does_not_require_or_use_publishing_credentials() {
    let (output, log) = run(true, false, true);
    assert!(output.status.success());
    assert!(log.is_empty());
    assert!(String::from_utf8_lossy(&output.stdout).contains("would publish tollgate-axum"));
}
