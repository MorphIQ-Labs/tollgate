//! Mutation testing for the Lean models.
//!
//! A proof is only as strong as what it pins down. A model whose transition
//! can be broken — a guard flipped, a refund dropped, a bound loosened —
//! while every theorem still checks has a behaviour its proofs never stated,
//! which is the proof-side twin of a test that does not bite. So this does to
//! the models what `cargo-mutants` does to the Rust: it mutates each
//! transition definition, one change at a time, and requires some theorem to
//! fail.
//!
//! Only `def` and `abbrev` bodies are mutated; theorems are the tests, not the
//! subject. Each mutant is checked by the pinned Lean toolchain against the
//! already-built package, and classified by where Lean reports an error:
//!
//! - **killed**: an error inside a theorem — a proof no longer holds;
//! - **unviable**: errors only outside theorems — the mutant does not
//!   typecheck, so it says nothing about the proofs;
//! - **survived**: no error at all — the proofs hold for a broken model.
//!
//! A survivor fails the gate unless the allowlist names it with a reason: an
//! equivalent mutant (one that changes nothing observable) is the only honest
//! entry.

use std::{
    fmt,
    path::{Path, PathBuf},
};

/// One token-level change, applied to one occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Operator {
    pub from: &'static str,
    pub to: &'static str,
}

/// Replacements are matched as whole tokens: symbols with a space on both
/// sides, words at word boundaries.
pub const OPERATORS: &[Operator] = &[
    Operator {
        from: "≤", to: "<"
    },
    Operator {
        from: "<", to: "≤"
    },
    Operator {
        from: "≥", to: ">"
    },
    Operator {
        from: ">", to: "≥"
    },
    Operator { from: "+", to: "-" },
    Operator { from: "-", to: "+" },
    Operator {
        from: "=", to: "≠"
    },
    Operator {
        from: "≠", to: "="
    },
    Operator {
        from: "∧", to: "∨"
    },
    Operator {
        from: "∨", to: "∧"
    },
    Operator {
        from: "&&",
        to: "||",
    },
    Operator {
        from: "||",
        to: "&&",
    },
    Operator {
        from: "true",
        to: "false",
    },
    Operator {
        from: "false",
        to: "true",
    },
    Operator {
        from: "min",
        to: "max",
    },
    Operator {
        from: "max",
        to: "min",
    },
    Operator { from: "!", to: "" },
];

/// What kind of top-level declaration a line belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A transition or helper definition: mutated.
    Definition,
    /// A theorem or lemma: where a kill is reported.
    Theorem,
    /// Everything else: structures, inductives, namespaces, comments.
    Other,
}

/// A mutant of one module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mutant {
    pub module: String,
    /// 1-based line in the module.
    pub line: usize,
    pub operator: Operator,
    /// Which occurrence of the operator on that line, from 0.
    pub occurrence: usize,
    /// The original line, trimmed: the allowlist key, stable across line moves.
    pub original: String,
    pub mutated_source: String,
}

impl Mutant {
    /// The identity the allowlist and reports use.
    pub fn key(&self) -> String {
        format!(
            "{} :: {} :: {} -> {} #{}",
            self.module,
            self.original,
            self.operator.from,
            if self.operator.to.is_empty() {
                "∅"
            } else {
                self.operator.to
            },
            self.occurrence
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Killed,
    Unviable,
    Survived,
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Outcome::Killed => "killed",
            Outcome::Unviable => "unviable",
            Outcome::Survived => "SURVIVED",
        })
    }
}

/// The declaration kind of every line of `source` (index 0 is line 1).
///
/// A top-level declaration starts at column zero, possibly after attributes
/// and modifiers; every following line belongs to it until the next one.
/// Block comments and doc comments belong to `Other`.
pub fn line_kinds(source: &str) -> Vec<Kind> {
    let mut kinds = Vec::new();
    let mut current = Kind::Other;
    let mut in_block_comment = false;
    for line in source.lines() {
        if in_block_comment {
            kinds.push(Kind::Other);
            if line.contains("-/") {
                in_block_comment = false;
            }
            continue;
        }
        let trimmed = line.trim_start();
        if let Some(after_open) = trimmed.strip_prefix("/-") {
            // Stays open until a `-/`, which may be on this same line.
            in_block_comment = !after_open.contains("-/");
            kinds.push(Kind::Other);
            continue;
        }
        if !line.starts_with(' ') && !line.is_empty() {
            current = top_level_kind(line);
        }
        // An indented `--` comment belongs to the declaration around it, so a
        // comment inside a definition does not end its body; only its code
        // part (none) is ever mutated.
        kinds.push(current);
    }
    kinds
}

fn top_level_kind(line: &str) -> Kind {
    let mut rest = line.trim_start();
    while let Some(after) = rest.strip_prefix("@[") {
        rest = match after.split_once(']') {
            Some((_, tail)) => tail.trim_start(),
            None => return Kind::Other,
        };
    }
    for modifier in ["private ", "protected ", "noncomputable ", "partial "] {
        rest = rest.strip_prefix(modifier).unwrap_or(rest);
    }
    let word = rest.split_whitespace().next().unwrap_or_default();
    match word {
        "def" | "abbrev" => Kind::Definition,
        "theorem" | "lemma" => Kind::Theorem,
        _ => Kind::Other,
    }
}

/// The code part of a line, without a trailing `--` comment.
fn code_part(line: &str) -> &str {
    match line.find("--") {
        Some(at) => &line[..at],
        None => line,
    }
}

/// Byte offsets where `op.from` occurs as a whole token in `code`.
pub fn occurrences(code: &str, op: Operator) -> Vec<usize> {
    let word = op.from.chars().all(|c| c.is_alphabetic());
    let mut found = Vec::new();
    for (at, _) in code.match_indices(op.from) {
        let before = code[..at].chars().next_back();
        let after = code[at + op.from.len()..].chars().next();
        let ok = if word {
            !before.is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '.')
                && !after.is_some_and(|c| c.is_alphanumeric() || c == '_')
        } else if op.from == "!" {
            // Boolean negation: `!x` or `(!x`, never `!=` or a trailing `!`.
            !before.is_some_and(|c| !(c == ' ' || c == '('))
                && after.is_some_and(|c| c.is_alphanumeric() || c == '(' || c == '_')
        } else {
            before == Some(' ') && after == Some(' ')
        };
        if ok {
            found.push(at);
        }
    }
    found
}

/// For each line (index 0 is line 1), the byte offset from which its code is
/// a definition *body* and may be mutated, or `None`.
///
/// Mutation targets transitions, not their types and not specifications. A
/// definition's signature runs until its first `:=`, or until its first
/// `|` equation line; its parameters and preconditions are types. A
/// definition whose type is `Prop` is a specification: weakening one always
/// survives, because a theorem that proves a stronger statement still proves
/// the weaker, so such a survivor would say nothing about the proofs.
pub fn body_spans(source: &str) -> Vec<Option<usize>> {
    let kinds = line_kinds(source);
    let lines: Vec<&str> = source.lines().collect();
    let mut spans = vec![None; lines.len()];
    let mut index = 0;
    while index < lines.len() {
        let starts_definition = kinds[index] == Kind::Definition
            && !lines[index].starts_with(' ')
            && !lines[index].is_empty();
        if !starts_definition {
            index += 1;
            continue;
        }
        let mut end = index + 1;
        while end < lines.len()
            && !(kinds[end] != Kind::Definition
                || (!lines[end].starts_with(' ') && !lines[end].is_empty()))
        {
            end += 1;
        }
        // Find where the body begins, collecting the signature text before it.
        let mut signature = String::new();
        let mut body_start: Option<(usize, usize)> = None;
        for (line, text) in lines.iter().enumerate().take(end).skip(index) {
            let code = code_part(text);
            if code.trim_start().starts_with('|') {
                body_start = Some((line, 0));
                break;
            }
            if let Some(at) = code.find(":=") {
                signature.push_str(&code[..at]);
                body_start = Some((line, at + 2));
                break;
            }
            signature.push_str(code);
            signature.push(' ');
        }
        let is_spec = signature.contains(": Prop") || signature.contains("→ Prop");
        if let (Some((line, column)), false) = (body_start, is_spec) {
            spans[line] = Some(column);
            for span in spans.iter_mut().take(end).skip(line + 1) {
                *span = Some(0);
            }
        }
        index = end;
    }
    spans
}

/// Every mutant of one module's definition bodies.
pub fn mutants(module: &str, source: &str) -> Vec<Mutant> {
    let spans = body_spans(source);
    let lines: Vec<&str> = source.lines().collect();
    let mut found = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let Some(from) = spans[index] else {
            continue;
        };
        let code = code_part(line);
        let Some(body) = code.get(from..) else {
            continue;
        };
        for &op in OPERATORS {
            for (occurrence, at) in occurrences(body, op).into_iter().enumerate() {
                let at = at + from;
                let mut mutated_line = String::with_capacity(line.len());
                mutated_line.push_str(&line[..at]);
                mutated_line.push_str(op.to);
                mutated_line.push_str(&line[at + op.from.len()..]);
                let mut mutated = lines.clone();
                mutated[index] = &mutated_line;
                let mut mutated_source = mutated.join("\n");
                mutated_source.push('\n');
                found.push(Mutant {
                    module: module.to_owned(),
                    line: index + 1,
                    operator: op,
                    occurrence,
                    original: line.trim().to_owned(),
                    mutated_source,
                });
            }
        }
    }
    found
}

/// Classify a mutant from the 1-based lines Lean reported errors on.
pub fn classify(kinds: &[Kind], error_lines: &[usize]) -> Outcome {
    if error_lines.is_empty() {
        return Outcome::Survived;
    }
    let in_theorem = error_lines
        .iter()
        .any(|line| kinds.get(line.saturating_sub(1)) == Some(&Kind::Theorem));
    if in_theorem {
        Outcome::Killed
    } else {
        Outcome::Unviable
    }
}

/// The 1-based lines of `severity: error` messages in Lean's `--json` output.
pub fn error_lines(json_output: &str) -> Vec<usize> {
    json_output
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|message| message["severity"] == "error")
        .filter_map(|message| message["pos"]["line"].as_u64())
        .map(|line| line as usize)
        .collect()
}

/// Allowlisted survivors: `key :: reason` per line, `#` comments.
pub fn allowlist(text: &str) -> Vec<(String, String)> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let (key, reason) = line.rsplit_once(" :: reason: ")?;
            Some((key.trim().to_owned(), reason.trim().to_owned()))
        })
        .collect()
}

/// The model files under `formal/lean/Tollgate`, sorted.
pub fn modules(root: &Path) -> Result<Vec<PathBuf>, String> {
    let dir = root.join("formal/lean/Tollgate");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .map_err(|e| format!("cannot read {}: {e}", dir.display()))?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "lean"))
        .collect();
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MODEL: &str = "import Init.Omega

/-!
A doc block with def and ≤ inside.
-/
namespace M

/-- Doc. -/
def step (a b : Nat) : Nat :=
  if a ≤ b then a + 1 else b -- a comment: x = y

theorem step_le (a b : Nat) : step a b ≤ b + 1 := by
  unfold step; split <;> omega

end M
";

    #[test]
    fn lines_are_classified_by_their_declaration() {
        let kinds = line_kinds(MODEL);
        assert_eq!(kinds[3], Kind::Other, "inside the module doc");
        assert_eq!(kinds[8], Kind::Definition, "the def line");
        assert_eq!(kinds[9], Kind::Definition, "the def body");
        assert_eq!(kinds[11], Kind::Theorem);
        assert_eq!(kinds[12], Kind::Theorem, "the proof body");
    }

    #[test]
    fn only_definition_bodies_are_mutated_and_comments_are_not() {
        let found = mutants("M", MODEL);
        let keys: Vec<String> = found.iter().map(Mutant::key).collect();
        assert!(
            keys.iter()
                .all(|k| k.contains("if a ≤ b then a + 1 else b")),
            "{keys:?}"
        );
        let ops: Vec<(&str, &str)> = found
            .iter()
            .map(|m| (m.operator.from, m.operator.to))
            .collect();
        assert!(ops.contains(&("≤", "<")));
        assert!(ops.contains(&("+", "-")));
        assert!(!ops.contains(&("=", "≠")), "the comment's `=` is not code");
    }

    #[test]
    fn signatures_and_specifications_are_not_mutated() {
        let model = "namespace M\n\ndef f (a : Nat)\n    (h : a ≤ 3) : Nat :=\n  a + 1\n\ndef g : Nat → Nat\n  | 0 => 1 + 1\n  | n => n\n\ndef Valid (a : Nat) : Prop :=\n  a ≤ 3 ∧ a ≠ 1\n\ndef h (a : Nat) : Nat := a + 2\n\nend M\n";
        let found = mutants("M", model);
        let lines: Vec<usize> = found.iter().map(|m| m.line).collect();
        assert!(
            !lines.contains(&4),
            "a precondition on the signature is a type"
        );
        assert!(lines.contains(&5), "the body of f");
        assert!(lines.contains(&8), "an equation body of g");
        assert!(!lines.contains(&12), "a Prop definition is a specification");
        assert!(lines.contains(&14), "a one-line body after :=");
        assert!(
            found
                .iter()
                .filter(|m| m.line == 14)
                .all(|m| m.operator.from == "+")
        );
    }

    fn lines_of(found: &[Mutant]) -> Vec<usize> {
        found.iter().map(|m| m.line).collect()
    }

    fn mutated_line(mutant: &Mutant) -> &str {
        mutant
            .mutated_source
            .lines()
            .nth(mutant.line - 1)
            .expect("the mutated line exists")
    }

    #[test]
    fn a_comment_inside_a_definition_does_not_end_its_body() {
        let model = "def f (a : Nat) : Nat :=\n  -- a note: x = y\n  a + 1\n";
        let found = mutants("M", model);
        assert_eq!(
            lines_of(&found),
            [3],
            "the line after the comment is still body"
        );
        assert_eq!(found[0].operator.from, "+");
    }

    #[test]
    fn a_theorem_is_never_a_definition_body() {
        let model = "theorem t (a : Nat) : a + 1 = 1 + a := by\n  omega\n";
        assert!(mutants("M", model).is_empty());
    }

    #[test]
    fn a_definition_on_the_last_line_is_read_to_the_end() {
        let found = mutants("M", "def f (a : Nat) : Nat :=\n  a + 1");
        assert_eq!(lines_of(&found), [2]);
    }

    #[test]
    fn the_next_definition_ends_the_body_before_it() {
        let model =
            "def f (a : Nat) : Nat :=\n  a + 1\ndef g (a : Nat) (h : a ≤ 3) : Nat :=\n  a\n";
        assert_eq!(
            lines_of(&mutants("M", model)),
            [2],
            "g's signature is not f's body"
        );
    }

    #[test]
    fn a_mutation_lands_exactly_where_its_token_is() {
        let one_line = mutants("M", "def h (a : Nat) : Nat := a + 2\n");
        assert_eq!(one_line.len(), 1);
        assert_eq!(mutated_line(&one_line[0]), "def h (a : Nat) : Nat := a - 2");
        let wrapped = mutants("M", "def f (a : Nat) : Nat\n := a + 1\n");
        assert_eq!(wrapped.len(), 1);
        assert_eq!(mutated_line(&wrapped[0]), " := a - 1");
    }

    #[test]
    fn word_boundaries_exclude_identifier_characters_on_both_sides() {
        let op = Operator {
            from: "max",
            to: "min",
        };
        assert!(occurrences("x_max", op).is_empty());
        assert!(occurrences("a.max", op).is_empty());
        assert!(occurrences("max_x", op).is_empty());
        assert!(occurrences("max2", op).is_empty());
        let not = Operator { from: "!", to: "" };
        assert_eq!(occurrences("!_x", not), vec![0]);
        assert_eq!(occurrences("!(x)", not), vec![0]);
        assert!(occurrences("!", not).is_empty());
    }

    #[test]
    fn a_comment_is_not_an_allowlist_entry_even_when_it_looks_like_one() {
        let text = "# M :: a :: ≤ -> < #0 :: reason: commented out\n";
        assert!(allowlist(text).is_empty());
    }

    #[test]
    fn modules_are_the_lean_files_in_sorted_order() {
        let dir = tempfile::tempdir().expect("scratch repository");
        let models = dir.path().join("formal/lean/Tollgate");
        std::fs::create_dir_all(&models).expect("models dir");
        for name in ["B.lean", "A.lean", "notes.txt"] {
            std::fs::write(models.join(name), "").expect("file");
        }
        let names: Vec<String> = modules(dir.path())
            .expect("readable")
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, ["A.lean", "B.lean"]);
        assert!(modules(&dir.path().join("missing")).is_err());
    }

    #[test]
    fn symbol_operators_need_spaces_and_words_need_boundaries() {
        let op = |from, to| Operator { from, to };
        assert_eq!(occurrences("a => b", op(">", "≥")), Vec::<usize>::new());
        assert_eq!(occurrences("x := y = z", op("=", "≠")), vec![7]);
        assert_eq!(occurrences("maxTtl max min", op("max", "min")), vec![7]);
        assert_eq!(
            occurrences("if !s.revoked ∧ (!x)", op("!", "")),
            vec![3, 19]
        );
        assert_eq!(occurrences("a != b", op("!", "")), Vec::<usize>::new());
    }

    #[test]
    fn a_mutant_is_killed_only_by_a_theorem_error() {
        let kinds = line_kinds(MODEL);
        assert_eq!(classify(&kinds, &[]), Outcome::Survived);
        assert_eq!(classify(&kinds, &[10]), Outcome::Unviable);
        assert_eq!(classify(&kinds, &[10, 13]), Outcome::Killed);
    }

    #[test]
    fn errors_are_read_from_lean_json() {
        let out = r#"{"severity":"error","pos":{"line":13,"column":2},"data":"x"}
{"severity":"warning","pos":{"line":10,"column":0},"data":"y"}
not json"#;
        assert_eq!(error_lines(out), vec![13]);
    }

    #[test]
    fn the_allowlist_needs_a_reason() {
        let text = "# comment\nM :: a :: ≤ -> < #0 :: reason: equivalent at the boundary\nno reason here\n";
        assert_eq!(
            allowlist(text),
            vec![(
                "M :: a :: ≤ -> < #0".to_owned(),
                "equivalent at the boundary".to_owned()
            )]
        );
    }
}
