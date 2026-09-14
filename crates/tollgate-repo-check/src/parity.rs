//! Backend-suite parity: the mirrored scenarios must drive the same contract.
//!
//! `MemoryStore` carries inherent helpers that shadow its `AdminStore` methods
//! — `create_account`, `deposit`, `publish_snapshot`, `remove_snapshot` — while
//! `PostgresStore` has no such helpers and can only be driven through the
//! trait. A mirrored test that calls the inherent helper on one side and the
//! trait on the other is testing two different contracts under one name, and
//! nothing in the compiler or the test run says so.
//!
//! This is not a name diff. A name diff compares the *sets* of tests and would
//! have reported these suites as healthy: every divergence #85 fixed sat
//! **inside** a test both suites already had. It is also why the renamed pairs
//! (`snapshot_publish_fetch_and_push` against
//! `snapshot_publish_fetch_and_generation_monotonicity`) are not treated as
//! missing — a rename is how a name-based check loses its power, so this one
//! does not depend on names matching beyond the pairs it can see.
//!
//! What it cannot do: prove the two mirrored bodies assert the same things. It
//! reports one mechanical, historically-recurring divergence, and says so
//! rather than implying parity.

use std::{collections::BTreeSet, fs, path::Path};
use syn::visit::{self, Visit};

/// The `MemoryStore` inherent helpers that shadow an `AdminStore` method.
/// `try_create_account` is the fallible spelling of the same helper.
const SHADOWED: [(&str, &str); 5] = [
    ("create_account", "create_account"),
    ("try_create_account", "create_account"),
    ("deposit", "deposit"),
    ("publish_snapshot", "publish_snapshot"),
    ("remove_snapshot", "remove_snapshot"),
];

/// One mirrored test whose two sides drive different contracts.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Divergence {
    pub test: String,
    pub operation: String,
}

impl std::fmt::Display for Divergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: memory drives the inherent `{}` helper, PostgreSQL drives `AdminStore::{}`",
            self.test, self.operation, self.operation
        )
    }
}

#[derive(Default)]
struct Calls {
    /// Method-call syntax: `store.publish_snapshot(..)`.
    methods: BTreeSet<String>,
    /// Path-call syntax: `AdminStore::publish_snapshot(..)`.
    admin_store: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for Calls {
    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        self.methods.insert(call.method.to_string());
        visit::visit_expr_method_call(self, call);
    }

    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let syn::Expr::Path(path) = &*call.func {
            let segments: Vec<_> = path
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            if let [.., qualifier, method] = segments.as_slice()
                && qualifier == "AdminStore"
            {
                self.admin_store.insert(method.clone());
            }
        }
        visit::visit_expr_call(self, call);
    }
}

/// Every free function in `path`, with the calls its body makes.
fn functions(path: &Path) -> Result<Vec<(String, Calls)>, String> {
    let source = fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let file = syn::parse_file(&source).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(file
        .items
        .into_iter()
        .filter_map(|item| match item {
            syn::Item::Fn(function) => {
                let mut calls = Calls::default();
                calls.visit_block(&function.block);
                Some((function.sig.ident.to_string(), calls))
            }
            _ => None,
        })
        .collect())
}

/// Mirrored tests whose memory side drives an inherent helper where the
/// PostgreSQL side drives the trait.
pub fn diverging(memory: &Path, postgres: &Path) -> Result<Vec<Divergence>, String> {
    let memory = functions(memory)?;
    let postgres = functions(postgres)?;

    let mut found = Vec::new();
    for (name, mem) in &memory {
        let Some((_, pg)) = postgres.iter().find(|(other, _)| other == name) else {
            continue;
        };
        for (helper, operation) in SHADOWED {
            // On the PostgreSQL side there is no inherent helper to shadow, so
            // `store.publish_snapshot(..)` *is* the trait call. Accepting method
            // syntax there as well as `AdminStore::` closes a false negative:
            // keyed on `AdminStore::` alone, a memory test using the helper
            // whose mirror happens to use method syntax reports nothing. Six
            // PostgreSQL tests use that spelling today.
            let postgres_drives_the_trait =
                pg.admin_store.contains(operation) || pg.methods.contains(operation);
            if mem.methods.contains(helper)
                && !mem.admin_store.contains(operation)
                && postgres_drives_the_trait
            {
                found.push(Divergence {
                    test: name.clone(),
                    operation: operation.to_string(),
                });
            }
        }
    }
    found.sort();
    found.dedup();
    Ok(found)
}
