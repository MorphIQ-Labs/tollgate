//! Documents that show code excerpts, checked against the code they excerpt.
//!
//! A tutorial that quotes a program is only as good as the quote: an excerpt
//! edited in the document but not the program, or the reverse, teaches code
//! that no longer compiles. A document declares the file its Rust blocks come
//! from with a comment, `<!-- excerpts-of: PATH -->` (PATH from the repository
//! root). Every ```` ```rust ```` block in that document must then appear in
//! the file as a contiguous run of lines, differing only by one common
//! indentation, which lets an excerpt from inside a function sit flush left.
//!
//! The program itself is compiled and run by the ordinary test suite, so an
//! excerpt that matches is code that works.

use std::{fs, path::Path};

const DECLARATION: &str = "<!-- excerpts-of:";

/// Every problem with the excerpting documents under `docs/`.
pub fn check(root: &Path) -> Result<Vec<String>, String> {
    let docs = root.join("docs");
    let mut entries: Vec<_> = fs::read_dir(&docs)
        .map_err(|e| format!("cannot read {}: {e}", docs.display()))?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
        .collect();
    entries.sort();
    let mut problems = Vec::new();
    for doc in entries {
        let text =
            fs::read_to_string(&doc).map_err(|e| format!("cannot read {}: {e}", doc.display()))?;
        let Some(source) = declared_source(&text) else {
            continue;
        };
        let name = doc.strip_prefix(root).unwrap_or(&doc).display().to_string();
        let Ok(code) = fs::read_to_string(root.join(source)) else {
            problems.push(format!("{name}: excerpts-of `{source}` cannot be read"));
            continue;
        };
        let blocks = rust_blocks(&text);
        if blocks.is_empty() {
            problems.push(format!(
                "{name}: declares excerpts-of `{source}` but has no rust blocks"
            ));
        }
        for (line, block) in blocks {
            if !contains_excerpt(&code, &block) {
                problems.push(format!(
                    "{name}:{line}: this rust block is not an excerpt of `{source}`"
                ));
            }
        }
    }
    Ok(problems)
}

fn declared_source(text: &str) -> Option<&str> {
    text.lines().find_map(|line| {
        line.trim()
            .strip_prefix(DECLARATION)?
            .strip_suffix("-->")
            .map(str::trim)
    })
}

/// The ```` ```rust ```` blocks in `text`, each with its 1-based fence line.
fn rust_blocks(text: &str) -> Vec<(usize, Vec<String>)> {
    let mut blocks = Vec::new();
    let mut open: Option<(usize, Vec<String>)> = None;
    for (index, line) in text.lines().enumerate() {
        let fence = line.trim_start().starts_with("```");
        match open.as_mut() {
            Some(_) if fence => blocks.push(open.take().expect("open block")),
            Some((_, lines)) => lines.push(line.trim_end().to_owned()),
            None if fence => {
                let info = line.trim_start().trim_start_matches('`').trim();
                let lang = info.split([',', ' ']).next().unwrap_or_default();
                if lang == "rust" {
                    open = Some((index + 1, Vec::new()));
                } else {
                    // Skip a non-Rust block by treating it as open with no
                    // destination: its body must not be read as prose.
                    open = Some((0, Vec::new()));
                }
            }
            None => {}
        }
    }
    blocks.retain(|(line, lines)| *line != 0 && !lines.is_empty());
    blocks
}

/// Whether `block` occurs in `code` as consecutive lines under one common
/// indentation. Blank lines match blank lines.
pub fn contains_excerpt(code: &str, block: &[String]) -> bool {
    let code: Vec<&str> = code.lines().map(str::trim_end).collect();
    let Some(anchor) = block.iter().position(|line| !line.is_empty()) else {
        return false;
    };
    if block.len() > code.len() {
        return false;
    }
    (0..=code.len() - block.len()).any(|start| {
        let first = code[start + anchor];
        let Some(indent) = first.strip_suffix(block[anchor].as_str()) else {
            return false;
        };
        if !indent.chars().all(char::is_whitespace) {
            return false;
        }
        block.iter().zip(&code[start..]).all(|(want, have)| {
            if want.is_empty() {
                have.is_empty()
            } else {
                have.strip_prefix(indent) == Some(want.as_str())
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(text: &str) -> Vec<String> {
        text.lines().map(str::to_owned).collect()
    }

    #[test]
    fn an_indented_region_matches_flush_left() {
        let code = "fn main() {\n    let a = 1;\n\n    let b = a;\n}\n";
        assert!(contains_excerpt(code, &lines("let a = 1;\n\nlet b = a;")));
    }

    #[test]
    fn an_edited_line_or_reordered_lines_do_not_match() {
        let code = "fn main() {\n    let a = 1;\n    let b = a;\n}\n";
        assert!(!contains_excerpt(code, &lines("let a = 2;\nlet b = a;")));
        assert!(!contains_excerpt(code, &lines("let b = a;\nlet a = 1;")));
    }

    #[test]
    fn indentation_must_be_common_to_the_whole_excerpt() {
        let code = "    if x {\n        y();\n    }\n";
        assert!(contains_excerpt(code, &lines("if x {\n    y();\n}")));
        assert!(!contains_excerpt(code, &lines("if x {\ny();\n}")));
    }

    #[test]
    fn only_rust_blocks_are_excerpts() {
        let text = "<!-- excerpts-of: x.rs -->\n```toml\na = 1\n```\n\n```rust\nlet a = 1;\n```\n";
        let blocks = rust_blocks(text);
        assert_eq!(blocks, vec![(6, vec!["let a = 1;".to_owned()])]);
        assert_eq!(declared_source(text), Some("x.rs"));
    }
}
