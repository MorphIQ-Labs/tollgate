# Invariant reference checks

Run `./scripts/check_invariant_witnesses.sh` from a checkout. The same command
runs in the blocking `repository-hygiene` job on every merge request. It builds
only the small `tollgate-repo-check` tool; it does not compile the workspace,
execute tests, run mutation testing, start PostgreSQL, or measure performance.
The binary accepts an optional repository path, `--help`, `--version`, and `--`.

The checker owns citation resolution for `INVARIANTS.md`. Every inline code
span that is a snake-case identifier or qualified identifier is checked,
including references outside a *Tests* paragraph. Wrapped brace groups such
as `reservation::tests::{drop_releases_pending, cancel_charges_zero_and_refunds}`
expand into separate references. Rust uses `::` qualification and Lean uses
`.`. A qualification must match a complete suffix of the declaration's scope;
an arbitrary wrong prefix cannot pass because the final name exists elsewhere.
An unqualified name may resolve in multiple backend suites. That is deliberate:
the prose can cite one shared scenario name for memory and PostgreSQL.

Rust declarations are parsed under `crates/` and `examples/` with `syn`, including
functions, methods, fields and modules. This distinguishes code and configuration
names from absent symbols without maintaining a list of every field. The
`proptest!` token stream is inspected separately because its strategy parameters
are not ordinary Rust function syntax. Rust comments, string literals and opaque
generator macros do not contribute declarations. Conditional declarations are
included regardless of the current host's enabled features. File and inline
module names supply lexical scopes; this is not compiler name resolution for
re-exports, renamed imports, `include!`, or arbitrary macro expansion. If those
become necessary, extend the resolver and its fixture tests in that change.

Lean's namespace/section, definition and theorem declarations are indexed after
removing comments (including nested comments) and strings. Named paths under
`formal/` ending in `.lean` must exist within the checkout. The scanner covers
the declaration notation used by the checked models; it does not elaborate Lean
or check proofs. The formal job does that separately.

Fenced examples and compound code expressions are not candidate names. Neither
are single-word or CamelCase type names. Use ordinary inline snake-case names
for test witnesses. The guard intentionally does not scan historical narrative
in `docs/DESIGN.md`, where removed test names remain useful explanations.

External APIs, compiler lints, SQL objects and static event names that are not
Rust/Lean declarations have exact entries with reasons in
`testing/invariant_external_symbols.json`. Do not add a test name there to
bypass a missing witness: correct the citation or supply its enforcing test.
Empty explanations and unused entries fail. If a formerly external name gains
a repository declaration, the entry must be removed. Changes to this small
reviewed manifest are the durable record of non-declaration classifications;
the normal checker reports obsolete entries and supplies the removal action.

Unreadable directories/files, source symlinks, malformed Rust, malformed reference groups,
unclosed spans/fences, absent source trees and empty candidate sets fail rather
than produce a successful partial audit. Diagnostics identify the document
line and unresolved name. The checker never replaces source or evidence files.

Passing establishes that references have declarations, not that every cited
function is a test or that a test enforces the stated invariant. In particular,
a helper with a matching name is not evidence of behavior. Review still checks
the enforcement ladder and the surrounding claim; unit/integration/property
tests, CI mutation testing and Lean verification establish their respective
evidence. The guard's integration tests use synthetic repositories as parser
input and assert the observable accept/reject contract. They do not assert
production source text or statement order.

`syn` 2 and `proc-macro2` 1 were already in the lockfile. They are maintained
MIT/Apache-2.0 Rust parser components; taking them directly avoids a second,
partial Rust lexer/parser. The additional full-AST/visitor feature work is
confined to this unpublished tool. It has no dependency path into Tollgate's
libraries or deployed services. Existing advisory, MSRV and workspace lint gates
cover it. No dependency version, production API, wire contract, database schema,
performance threshold or baseline changes with this check.
