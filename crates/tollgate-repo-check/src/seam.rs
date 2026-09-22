//! The staged admission seam must not publish what its contract does not declare.
//!
//! `docs/DESIGN.md` § "Staged admission interface (#96)" is the reviewed
//! destination for the types a service embeds against, and it states its own
//! amendment rule: an implementing issue that needs to diverge changes that
//! section first, rather than silently publishing a different seam. The rule
//! existed so a consumer would not have to adapt to a sequence of temporary
//! interfaces.
//!
//! It drifted anyway. `estimate_remaining` shipped on three stages and was
//! declared in none of them (#126), and nothing said so — a consumer reading
//! the contract would have found three published methods missing, with no way
//! to tell whether that meant *not part of the seam*, *not yet designed*, or
//! *nobody amended the section*.
//!
//! # What it checks, and what it cannot
//!
//! One direction only: a public method on a seam type whose name appears
//! nowhere in the section. The reverse — declared but unpublished — is not
//! checked, because the section's blocks carry abbreviated signatures and
//! commented struct bodies that describe rather than declare, and flagging
//! those would make the check unusable without making the seam safer.
//!
//! It is a name check, and only of methods. It does not compare signatures, so
//! a changed parameter or return type passes; that would need the section's
//! blocks to be compilable Rust, which they deliberately are not. It says
//! nothing about types, derives or attributes either — that `Released` became
//! `Copy`, or that `CancelHandle` became `Clone`, is real drift this cannot
//! see. What it catches is the failure that actually happened: a method
//! published into the seam with the contract never opened.
//!
//! # Why it reads code and not prose
//!
//! [`crate::check`] skips fenced code when scanning `INVARIANTS.md`, because
//! there a fenced example is prose rather than a declaration. Here it is the
//! other way round: the fenced blocks *are* the declaration, and the prose is
//! not.
//!
//! That distinction has to be enforced rather than assumed. `AdmissionEngine`
//! publishes `new` and `map`, the contract declares neither, and the English
//! words "new" and "map" occur eleven and seven times in the section — so a
//! scan that accepted prose would call them declared and report nothing.
//! Requiring code form means an inline span counts: writing `` `some_method` ``
//! in a sentence that explains why it sits outside the seam satisfies this
//! check, which is why there is no allowlist file and every exclusion is
//! written where a reader looking for the contract will find it.

use std::{collections::BTreeSet, fs, path::Path};

use syn::visit::{self, Visit};

/// The heading that opens the contract, matched by prefix so the section's
/// date can change without breaking the check.
const SECTION: &str = "## Staged admission interface (#96";

/// The types the section declares, and the file each is published from.
///
/// Scoped to types rather than whole files, and that is load-bearing.
/// `crates/tollgate-core/src/reservation.rs` publishes six methods the section
/// never declares — `reserve_at_locality`, `commit_at_execution_start` and
/// their siblings — on `Reservation`, `SharedCharge` and `CommitFunding`. Those
/// are the plumbing the seam is built from, not the seam, and a file-scoped
/// check would report every one of them and be wrong.
const SEAM: [(&str, &str); 6] = [
    ("crates/tollgate-admission/src/engine.rs", "AdmissionEngine"),
    ("crates/tollgate-admission/src/engine.rs", "RequestContext"),
    ("crates/tollgate-admission/src/engine.rs", "Pending"),
    ("crates/tollgate-admission/src/engine.rs", "ReadyToStart"),
    ("crates/tollgate-admission/src/engine.rs", "Committed"),
    ("crates/tollgate-core/src/reservation.rs", "CancelHandle"),
];

/// One published method the contract does not mention.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Undeclared {
    pub type_name: String,
    pub method: String,
}

impl std::fmt::Display for Undeclared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}::{} is published but named nowhere in the contract",
            self.type_name, self.method
        )
    }
}

/// Public method names on one type, collected from its `impl` blocks.
struct Methods<'a> {
    wanted: &'a str,
    found: BTreeSet<String>,
    /// Whether the file defines the type at all.
    ///
    /// Tracked separately from `found` because "no public methods" is a
    /// legitimate answer — a type can publish none — while "the type is not
    /// here" means the seam list points at something that moved, and the two
    /// must not produce the same silence.
    defined: bool,
}

impl<'ast> Visit<'ast> for Methods<'_> {
    fn visit_item_struct(&mut self, node: &'ast syn::ItemStruct) {
        self.defined |= node.ident == self.wanted;
    }

    fn visit_item_enum(&mut self, node: &'ast syn::ItemEnum) {
        self.defined |= node.ident == self.wanted;
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        // A trait `impl` publishes nothing of its own: the method is the
        // trait's, and the trait is declared wherever the trait is.
        if node.trait_.is_some() || !self_type_is(&node.self_ty, self.wanted) {
            return;
        }
        for item in &node.items {
            if let syn::ImplItem::Fn(function) = item
                && matches!(function.vis, syn::Visibility::Public(_))
            {
                self.found.insert(function.sig.ident.to_string());
            }
        }
        visit::visit_item_impl(self, node);
    }
}

/// Whether `ty` is the named type, ignoring generic arguments so
/// `Pending<S>` and `ReadyToStart<S, P>` match their bare names.
fn self_type_is(ty: &syn::Type, name: &str) -> bool {
    let syn::Type::Path(path) = ty else {
        return false;
    };
    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == name)
}

/// The text of the contract section, from its heading to the next `##`.
///
/// A missing heading is an error rather than an empty section: a renamed or
/// deleted heading would otherwise silently declare nothing, which reports
/// every published method as drift — or, if the seam list were also empty,
/// nothing at all.
fn section(document: &str) -> Result<String, String> {
    let mut lines = document
        .lines()
        .skip_while(|line| !line.starts_with(SECTION));
    let heading = lines
        .next()
        .ok_or_else(|| format!("docs/DESIGN.md has no section starting `{SECTION}`"))?;
    let body: Vec<&str> = std::iter::once(heading)
        .chain(lines.take_while(|line| !line.starts_with("## ")))
        .collect();
    Ok(body.join("\n"))
}

/// Report the public seam methods the contract section never names.
///
/// `root` is the repository root.
pub fn undeclared(root: &Path) -> Result<Vec<Undeclared>, String> {
    let design = root.join("docs/DESIGN.md");
    let document =
        fs::read_to_string(&design).map_err(|error| format!("{}: {error}", design.display()))?;
    let contract = code(&section(&document)?);

    let mut found = Vec::new();
    for (path, type_name) in SEAM {
        let source = root.join(path);
        let text = fs::read_to_string(&source)
            .map_err(|error| format!("{}: {error}", source.display()))?;
        let parsed =
            syn::parse_file(&text).map_err(|error| format!("{}: {error}", source.display()))?;
        let mut methods = Methods {
            wanted: type_name,
            found: BTreeSet::new(),
            defined: false,
        };
        methods.visit_file(&parsed);
        if !methods.defined {
            // The type moved or was renamed, so this file can say nothing
            // about it. Silence here would read as a clean seam, which is the
            // one answer that must never be a guess.
            return Err(format!(
                "{}: `{type_name}` is not defined here; the seam list in \
                 `crates/tollgate-repo-check/src/seam.rs` is stale",
                source.display()
            ));
        }
        for method in methods.found {
            if !names(&contract, &method) {
                found.push(Undeclared {
                    type_name: type_name.to_owned(),
                    method,
                });
            }
        }
    }
    found.sort();
    found.dedup();
    Ok(found)
}

/// The section's code: fenced blocks and inline spans, prose discarded.
///
/// Prose cannot declare, and letting it try is how this check would have gone
/// quiet. `AdmissionEngine::new` and `map` are published and undeclared, and
/// the words "new" and "map" occur eleven and seven times in the section's
/// English — so a scan of the whole section reports them as declared and finds
/// nothing. Requiring code form costs a consumer one pair of backticks and
/// keeps the check honest about short, common names.
fn code(section: &str) -> String {
    let mut code = String::new();
    let mut fenced = false;
    for line in section.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") {
            fenced = !fenced;
            continue;
        }
        if fenced {
            code.push_str(line);
            code.push('\n');
            continue;
        }
        // Inline spans. Unpaired backticks in prose are ignored rather than
        // treated as opening a span to end of line.
        let mut rest = line;
        while let Some(open) = rest.find('`') {
            let after = &rest[open + 1..];
            let Some(close) = after.find('`') else { break };
            code.push_str(&after[..close]);
            code.push('\n');
            rest = &after[close + 1..];
        }
    }
    code
}

/// Whether the contract's code names `method` as a whole word.
///
/// Whole-word, so `cancel` is not satisfied by `cancel_handle` appearing
/// beside it — the substring match is the other way a check like this quietly
/// stops catching anything.
fn names(contract: &str, method: &str) -> bool {
    let boundary = |c: char| !c.is_ascii_alphanumeric() && c != '_';
    contract.match_indices(method).any(|(at, _)| {
        let before = contract[..at].chars().next_back().is_none_or(boundary);
        let after = contract[at + method.len()..]
            .chars()
            .next()
            .is_none_or(boundary);
        before && after
    })
}
