//! The HTTP API reference lists exactly the routes the server registers.
use std::path::Path;

#[test]
fn the_http_route_table_matches_the_router() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let problems = tollgate_repo_check::http_routes::check(&root).expect("sources are readable");
    if !problems.is_empty() {
        let expected = tollgate_repo_check::http_routes::expected(&root).expect("readable");
        let table: Vec<String> = expected.iter().map(|route| route.render()).collect();
        panic!(
            "{}\n\nThe table docs/HTTP_API.md should carry:\n\n\
             | Method | Path | Role |\n|---|---|---|\n{}",
            problems.join("\n"),
            table.join("\n")
        );
    }
}

#[test]
fn a_route_added_to_the_router_fails_until_the_table_lists_it() {
    let dir = tempfile::tempdir().expect("scratch repository");
    let root = dir.path();
    let write = |path: &str, body: &str| {
        let full = root.join(path);
        std::fs::create_dir_all(full.parent().expect("has a parent")).expect("dirs");
        std::fs::write(full, body).expect("write");
    };
    write(
        "crates/tollgate-server/src/lib.rs",
        r#"fn router() -> Router {
            Router::new().route("/livez", get(ok)).route("/new", post(h).get(h))
        }"#,
    );
    let page = |rows: &str| {
        format!(
            "<!-- http-routes:start -->\n| Method | Path | Role |\n|---|---|---|\n{rows}\
             <!-- http-routes:end -->\n"
        )
    };
    write("docs/HTTP_API.md", &page("| GET | /livez | none |\n"));
    let problems = tollgate_repo_check::http_routes::check(root).expect("readable");
    assert_eq!(
        problems,
        [
            "missing row: | GET | /new | none |",
            "missing row: | POST | /new | none |"
        ]
    );
    write(
        "docs/HTTP_API.md",
        &page("| GET | /livez | none |\n| GET | /new | none |\n| POST | /new | none |\n"),
    );
    assert!(
        tollgate_repo_check::http_routes::check(root)
            .expect("readable")
            .is_empty()
    );
}
