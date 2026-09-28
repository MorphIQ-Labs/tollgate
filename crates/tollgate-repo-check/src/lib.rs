//! Declaration resolution for current invariant documentation. This is not a
//! coverage check, a Rust name resolver, or a substitute for executing proofs.

pub mod book;
pub mod cli;
pub mod parity;
pub mod seam;

use proc_macro2::{TokenStream, TokenTree};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};
use syn::visit::{self, Visit};

#[derive(Default)]
struct Symbols {
    names: BTreeSet<String>,
    scope: Vec<String>,
}

impl Symbols {
    fn add(&mut self, name: impl ToString) {
        let name = name.to_string();
        self.insert(
            self.scope
                .iter()
                .map(String::as_str)
                .chain([name.as_str()])
                .collect::<Vec<_>>()
                .join("::"),
        );
    }

    fn contains(&self, name: &str) -> bool {
        self.names.contains(name)
    }

    fn insert(&mut self, name: String) {
        // Index complete suffixes once; resolving each citation is a tree
        // lookup, rather than another scan through every source declaration.
        let delimiter = if name.contains("::") { "::" } else { "." };
        for (start, _) in name.match_indices(delimiter) {
            self.names
                .insert(name[start + delimiter.len()..].to_owned());
        }
        self.names.insert(name);
    }

    // proptest! accepts `fn name(arg in strategy)`, which is not Rust function
    // syntax. Its token tree retains declarations without parsing strings or
    // comments as code. Other macros are opaque: quote! and fixture generators
    // must not manufacture witnesses merely by mentioning a declaration.
    fn proptest(&mut self, tokens: TokenStream) {
        let mut tokens = tokens.into_iter().peekable();
        while let Some(token) = tokens.next() {
            match token {
                TokenTree::Ident(ident) if ident == "fn" => {
                    if let Some(TokenTree::Ident(name)) = tokens.next() {
                        self.add(name);
                    }
                }
                _ => {}
            }
        }
    }
}

impl<'ast> Visit<'ast> for Symbols {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        self.add(&item.ident);
        self.scope.push(item.ident.to_string());
        visit::visit_item_mod(self, item);
        self.scope.pop();
    }
    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.add(&item.sig.ident);
        visit::visit_item_fn(self, item);
    }
    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        let before = self.scope.len();
        if let syn::Type::Path(path) = item.self_ty.as_ref()
            && let Some(name) = path.path.segments.last()
        {
            self.scope.push(name.ident.to_string());
        }
        visit::visit_item_impl(self, item);
        self.scope.truncate(before);
    }
    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        self.add(&item.sig.ident);
        visit::visit_impl_item_fn(self, item);
    }
    fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
        self.scope.push(item.ident.to_string());
        visit::visit_item_trait(self, item);
        self.scope.pop();
    }
    fn visit_trait_item_fn(&mut self, item: &'ast syn::TraitItemFn) {
        self.add(&item.sig.ident);
        visit::visit_trait_item_fn(self, item);
    }
    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        self.scope.push(item.ident.to_string());
        visit::visit_item_struct(self, item);
        self.scope.pop();
    }
    fn visit_field(&mut self, item: &'ast syn::Field) {
        if let Some(name) = &item.ident {
            self.add(name);
        }
        visit::visit_field(self, item);
    }
    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        if item
            .path
            .segments
            .last()
            .is_some_and(|part| part.ident == "proptest")
        {
            self.proptest(item.tokens.clone());
        }
    }
}

fn read(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))
}

fn files(root: &Path, extension: &str) -> Result<Vec<PathBuf>, String> {
    let mut found = Vec::new();
    for entry in fs::read_dir(root).map_err(|error| format!("{}: {error}", root.display()))? {
        let entry = entry.map_err(|error| format!("{}: {error}", root.display()))?;
        let path = entry.path();
        let kind = entry
            .file_type()
            .map_err(|error| format!("{}: {error}", path.display()))?;
        if kind.is_symlink() {
            return Err(format!(
                "{}: source symlinks are not supported",
                path.display()
            ));
        }
        if kind.is_dir() {
            // Build output is not source, even when Cargo uses a local target.
            if entry.file_name() != "target" {
                found.extend(files(&path, extension)?);
            }
        } else if kind.is_file() && path.extension().is_some_and(|ext| ext == extension) {
            found.push(path);
        }
    }
    found.sort();
    Ok(found)
}

// Strip Lean comments (including nested block comments) and strings, preserving
// newlines. The declaration scanner below intentionally supports the namespace,
// section, def and theorem syntax used by this repository, not Lean elaboration.
fn lean_code(source: &str) -> Result<String, String> {
    let mut out = String::new();
    let mut chars = source.chars().peekable();
    let mut blocks = 0usize;
    let mut string = false;
    let mut line = false;
    while let Some(ch) = chars.next() {
        if line {
            if ch == '\n' {
                line = false;
                out.push('\n');
            }
        } else if blocks != 0 {
            if ch == '/' && chars.peek() == Some(&'-') {
                chars.next();
                blocks += 1;
            } else if ch == '-' && chars.peek() == Some(&'/') {
                chars.next();
                blocks -= 1;
            } else if ch == '\n' {
                out.push('\n');
            }
        } else if string {
            if ch == '\\' {
                chars.next();
            } else if ch == '"' {
                string = false;
            } else if ch == '\n' {
                out.push('\n');
            }
        } else if ch == '/' && chars.peek() == Some(&'-') {
            chars.next();
            blocks = 1;
            out.push(' ');
        } else if ch == '-' && chars.peek() == Some(&'-') {
            chars.next();
            line = true;
        } else if ch == '"' {
            string = true;
            out.push(' ');
        } else {
            out.push(ch);
        }
    }
    if blocks != 0 || string {
        return Err("unterminated Lean comment or string".into());
    }
    Ok(out)
}

fn lean_symbols(source: &str, symbols: &mut Symbols) -> Result<(), String> {
    let code = lean_code(source)?;
    let mut scopes: Vec<(&str, bool)> = Vec::new();
    for line in code.lines() {
        let mut words = line.split_whitespace();
        let mut kind = words.next().unwrap_or_default();
        if matches!(kind, "private" | "protected" | "noncomputable") {
            kind = words.next().unwrap_or_default();
        }
        match kind {
            "namespace" => scopes.push((words.next().ok_or("unnamed Lean namespace")?, true)),
            "section" => scopes.push((words.next().unwrap_or_default(), false)),
            "end" => {
                let (name, _) = scopes
                    .pop()
                    .ok_or("Lean end without namespace or section")?;
                if let Some(end) = words.next()
                    && end != name
                {
                    return Err(format!("Lean end {end} does not match {name}"));
                }
            }
            "theorem" | "lemma" | "def" => {
                let name = words.next().ok_or("unnamed Lean declaration")?;
                let qualified = scopes
                    .iter()
                    .filter(|(_, namespace)| *namespace)
                    .map(|(name, _)| *name)
                    .chain([name])
                    .collect::<Vec<_>>()
                    .join(".");
                symbols.insert(qualified);
            }
            _ => {}
        }
    }
    if !scopes.is_empty() {
        return Err("unclosed Lean namespace or section".into());
    }
    Ok(())
}

fn identifier(text: &str) -> bool {
    let mut chars = text.chars();
    chars
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn candidate(text: &str) -> bool {
    text.contains('_')
        && text
            .split("::")
            .flat_map(|part| part.split('.'))
            .all(identifier)
        && text.chars().any(|ch| ch.is_ascii_lowercase())
}

fn expand(text: &str) -> Result<Vec<String>, String> {
    if let Some((prefix, group)) = text.split_once("::{") {
        let group = group
            .strip_suffix('}')
            .ok_or_else(|| format!("malformed witness group: {text}"))?;
        let mut names = Vec::new();
        for name in group.split(',') {
            let name = format!("{prefix}::{}", name.trim());
            if !candidate(&name) {
                return Err(format!("malformed witness group: {text}"));
            }
            names.push(name);
        }
        Ok(names)
    } else if candidate(text) || (text.starts_with("formal/") && text.ends_with(".lean")) {
        Ok(vec![text.to_owned()])
    } else {
        Ok(Vec::new())
    }
}

/// Read inline code spans, including wrapped brace groups. Fenced examples are
/// prose examples rather than declarations and do not supply witness names.
fn references(document: &str) -> Result<Vec<(usize, String)>, String> {
    let mut visible = String::new();
    let mut fence: Option<(char, usize)> = None;
    for line in document.lines() {
        let trimmed = line.trim_start();
        let marker = trimmed.chars().next().unwrap_or(' ');
        let width = trimmed.chars().take_while(|ch| *ch == marker).count();
        if matches!(marker, '`' | '~') && width >= 3 {
            if fence.is_none() {
                fence = Some((marker, width));
            } else if fence.is_some_and(|(opened, count)| opened == marker && width >= count) {
                fence = None;
            }
            visible.push('\n');
        } else {
            if fence.is_none() {
                visible.push_str(line);
            }
            visible.push('\n');
        }
    }
    if fence.is_some() {
        return Err("unclosed Markdown fence".into());
    }
    let mut result = Vec::new();
    let mut remaining = visible.as_str();
    let mut line = 1usize;
    while let Some(start) = remaining.find('`') {
        line += remaining[..start].bytes().filter(|b| *b == b'\n').count();
        remaining = &remaining[start..];
        let width = remaining.bytes().take_while(|b| *b == b'`').count();
        let delimiter = "`".repeat(width);
        remaining = &remaining[width..];
        let end = remaining
            .find(&delimiter)
            .ok_or_else(|| format!("line {line}: unclosed code span"))?;
        let text = remaining[..end]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        for name in expand(&text)? {
            result.push((line, name));
        }
        line += remaining[..end].bytes().filter(|b| *b == b'\n').count();
        remaining = &remaining[end + width..];
    }
    Ok(result)
}

pub struct Report {
    pub resolved: usize,
    pub external: usize,
}

pub fn check(root: &Path) -> Result<Report, String> {
    let canonical_root = root
        .canonicalize()
        .map_err(|error| format!("{}: {error}", root.display()))?;
    let root = canonical_root.as_path();
    let mut symbols = Symbols::default();
    let mut rust_files = files(&root.join("crates"), "rs")?;
    rust_files.extend(files(&root.join("examples"), "rs")?);
    if rust_files.is_empty() {
        return Err("no Rust source files found".into());
    }
    for path in rust_files {
        let source = read(&path)?;
        let parsed =
            syn::parse_file(&source).map_err(|error| format!("{}: {error}", path.display()))?;
        let parts: Vec<_> = path
            .strip_prefix(root)
            .map_err(|error| error.to_string())?
            .iter()
            .map(|part| part.to_string_lossy().into_owned())
            .collect();
        symbols.scope.clear();
        if let Some(start) = parts
            .iter()
            .position(|part| part == "src" || part == "tests" || part == "benches")
        {
            symbols
                .scope
                .extend(parts[start + 1..parts.len() - 1].iter().cloned());
        }
        let stem = path
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or("non-UTF-8 Rust source name")?;
        if !matches!(stem, "lib" | "main" | "mod") {
            symbols.add(stem);
            symbols.scope.push(stem.to_owned());
        }
        symbols.visit_file(&parsed);
    }
    let lean_files = files(&root.join("formal/lean/Tollgate"), "lean")?;
    if lean_files.is_empty() {
        return Err("no Lean source files found".into());
    }
    for path in lean_files {
        lean_symbols(&read(&path)?, &mut symbols)
            .map_err(|error| format!("{}: {error}", path.display()))?;
    }
    let external_path = root.join("testing/invariant_external_symbols.json");
    let externals: BTreeMap<String, String> = serde_json::from_str(&read(&external_path)?)
        .map_err(|error| format!("{}: {error}", external_path.display()))?;
    let refs = references(&read(&root.join("INVARIANTS.md"))?)?;
    if refs.is_empty() {
        return Err("INVARIANTS.md contains no candidate references".into());
    }
    let mut used = BTreeSet::new();
    let mut errors = Vec::new();
    let mut report = Report {
        resolved: 0,
        external: 0,
    };
    for (line, name) in refs {
        if symbols.contains(&name) {
            report.resolved += 1;
        } else if name.starts_with("formal/") {
            if name.split('/').any(|part| part == "..")
                || !root.join(&name).is_file()
                || !root
                    .join(&name)
                    .canonicalize()
                    .is_ok_and(|path| path.starts_with(root))
            {
                errors.push(format!("INVARIANTS.md:{line}: missing proof file `{name}`"));
            } else {
                report.resolved += 1;
            }
        } else if externals
            .get(&name)
            .is_some_and(|reason| !reason.trim().is_empty())
        {
            used.insert(name);
            report.external += 1;
        } else {
            errors.push(format!(
                "INVARIANTS.md:{line}: unresolved reference `{name}`"
            ));
        }
    }
    for name in externals.keys() {
        if !used.contains(name) {
            errors.push(format!(
                "{}: unused or invalid external reference `{name}`; remove it",
                external_path.display()
            ));
        }
    }
    if errors.is_empty() {
        Ok(report)
    } else {
        Err(errors.join("\n"))
    }
}
