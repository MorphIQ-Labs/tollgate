//! The documentation site's link contract, as an mdBook preprocessor.
//!
//! The book is built from `docs/`, but the documents in it link to the whole
//! repository: `../INVARIANTS.md`, evidence under `testing/`, crate sources.
//! mdBook copies only its source directory, so without this every such link
//! would 404 on the published site while working on GitHub. Rewriting the
//! documents to suit the site would break them on GitHub instead. This keeps
//! one source that works in both places:
//!
//! - a link to a document that is a chapter becomes a link to that chapter;
//! - a link to any other repository path becomes a link to it on GitHub;
//! - a link to a path that does not exist, or to a heading a target does not
//!   have, fails the build.
//!
//! A chapter may also stand in for a document outside `docs/`: a page whose
//! first line is `<!-- repo-page: PATH -->` is replaced by the repository file
//! at `PATH`, and that file's links resolve from its own directory. The
//! invariants, contributing guide and changelog reach the site that way while
//! staying where the tooling and GitHub expect them.
//!
//! Every Markdown file under the source directory must be a chapter, so a new
//! document cannot be written and then left off the site.

use pulldown_cmark::{CowStr, Event, Options, Parser, Tag, TagEnd};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    ops::Range,
    path::{Component, Path, PathBuf},
};

const REPO_PAGE: &str = "<!-- repo-page:";

/// Where the site's out-of-book links point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repository {
    /// `https://github.com/<owner>/<name>`, without a trailing slash.
    pub url: String,
    /// The branch the site is built from.
    pub branch: String,
}

/// One chapter as the rewrite sees it.
struct Chapter {
    /// Path under the book source, as SUMMARY.md names it (`site/intro.md`).
    book_path: String,
    /// Repository path the content came from (`docs/EMBEDDING.md`).
    source: String,
    content: String,
    ids: BTreeSet<String>,
}

/// Rewrite the book mdBook hands a preprocessor: the `[context, book]` JSON
/// read from stdin. Returns the book JSON to write back, or every problem
/// found — all of them, so one build reports the whole list.
pub fn preprocess(input: &str) -> Result<String, Vec<String>> {
    let (root, src, repository, mut book) = parse_input(input).map_err(|e| vec![e])?;
    let mut chapters = Vec::new();
    collect(&mut book["sections"], &mut |chapter| {
        chapters.push(chapter);
    });
    let mut staged = Vec::with_capacity(chapters.len());
    let mut errors = Vec::new();
    for (book_path, content) in &chapters {
        match load(&root, &src, book_path, content) {
            Ok(chapter) => staged.push(chapter),
            Err(e) => errors.push(e),
        }
    }
    errors.extend(unlisted(&root, &src, &staged));
    let by_source: BTreeMap<&str, &Chapter> =
        staged.iter().map(|c| (c.source.as_str(), c)).collect();
    let mut rewritten = BTreeMap::new();
    for chapter in &staged {
        match rewrite(&root, &src, &repository, chapter, &by_source) {
            Ok(content) => {
                rewritten.insert(chapter.book_path.clone(), content);
            }
            Err(mut found) => errors.append(&mut found),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    replace(&mut book["sections"], &mut |path| rewritten.remove(path));
    serde_json::to_string(&book).map_err(|e| vec![format!("cannot encode the book: {e}")])
}

fn parse_input(input: &str) -> Result<(PathBuf, String, Repository, Value), String> {
    let value: Value =
        serde_json::from_str(input).map_err(|e| format!("mdBook input is not JSON: {e}"))?;
    let [context, book] = <[Value; 2]>::try_from(
        value
            .as_array()
            .cloned()
            .ok_or("mdBook input is not a [context, book] pair")?,
    )
    .map_err(|_| "mdBook input is not a [context, book] pair")?;
    let root = PathBuf::from(
        context["root"]
            .as_str()
            .ok_or("mdBook context has no root")?,
    );
    let src = context["config"]["book"]["src"]
        .as_str()
        .unwrap_or("src")
        .trim_end_matches('/')
        .to_owned();
    let config = &context["config"]["preprocessor"]["repo-links"];
    let url = config["repository"]
        .as_str()
        .ok_or("book.toml: [preprocessor.repo-links] must set `repository`")?
        .trim_end_matches('/')
        .to_owned();
    let branch = config["branch"].as_str().unwrap_or("main").to_owned();
    if book.get("sections").is_none() {
        return Err("mdBook book has no sections".into());
    }
    Ok((root, src, Repository { url, branch }, book))
}

/// Visit every chapter that has a file, depth first, in book order.
fn collect(sections: &mut Value, visit: &mut impl FnMut((String, String))) {
    for item in sections.as_array_mut().into_iter().flatten() {
        let Some(chapter) = item.get_mut("Chapter") else {
            continue;
        };
        if let (Some(path), Some(content)) = (chapter["path"].as_str(), chapter["content"].as_str())
        {
            visit((path.to_owned(), content.to_owned()));
        }
        collect(&mut chapter["sub_items"], visit);
    }
}

fn replace(sections: &mut Value, content_for: &mut impl FnMut(&str) -> Option<String>) {
    for item in sections.as_array_mut().into_iter().flatten() {
        let Some(chapter) = item.get_mut("Chapter") else {
            continue;
        };
        if let Some(content) = chapter["path"].as_str().and_then(&mut *content_for) {
            chapter["content"] = Value::String(content);
        }
        replace(&mut chapter["sub_items"], content_for);
    }
}

fn load(root: &Path, src: &str, book_path: &str, content: &str) -> Result<Chapter, String> {
    let own = format!("{src}/{book_path}");
    let (source, content) = match repo_page(content) {
        Some(target) => {
            let target = normalize(Path::new(""), target)
                .ok_or_else(|| format!("{own}: repo-page `{target}` leaves the repository"))?;
            let text = fs::read_to_string(root.join(&target))
                .map_err(|e| format!("{own}: repo-page `{target}` cannot be read: {e}"))?;
            (target, text)
        }
        None => (own, content.to_owned()),
    };
    let ids = heading_ids(&content, IdStyle::MdBook);
    Ok(Chapter {
        book_path: book_path.to_owned(),
        source,
        content,
        ids,
    })
}

/// The target of a `<!-- repo-page: PATH -->` first line, if the page is one.
fn repo_page(content: &str) -> Option<&str> {
    let first = content.lines().find(|line| !line.trim().is_empty())?.trim();
    first
        .strip_prefix(REPO_PAGE)?
        .strip_suffix("-->")
        .map(str::trim)
}

/// Markdown files under the book source that no chapter publishes.
fn unlisted(root: &Path, src: &str, chapters: &[Chapter]) -> Vec<String> {
    let listed: BTreeSet<&str> = chapters
        .iter()
        .map(|c| c.book_path.as_str())
        .chain(["SUMMARY.md"])
        .collect();
    let mut found = Vec::new();
    markdown_under(&root.join(src), Path::new(""), &mut found);
    found
        .into_iter()
        .filter(|path| !listed.contains(path.as_str()))
        .map(|path| format!("{src}/{path} is not in {src}/SUMMARY.md, so the site omits it"))
        .collect()
}

fn markdown_under(dir: &Path, prefix: &Path, found: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<_> = entries.flatten().collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let relative = prefix.join(entry.file_name());
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            markdown_under(&entry.path(), &relative, found);
        } else if relative.extension().is_some_and(|ext| ext == "md") {
            found.push(slash(&relative));
        }
    }
}

/// A link destination and where its text sits in the chapter.
struct Link {
    dest: String,
    span: Range<usize>,
}

fn rewrite(
    root: &Path,
    src: &str,
    repository: &Repository,
    chapter: &Chapter,
    by_source: &BTreeMap<&str, &Chapter>,
) -> Result<String, Vec<String>> {
    let (links, mut errors) = links(&chapter.content);
    let errors_at = |errors: &mut Vec<String>, dest: &str, why: String| {
        errors.push(format!("{}: `{dest}`: {why}", chapter.source));
    };
    let base = Path::new(&chapter.source)
        .parent()
        .unwrap_or(Path::new(""))
        .to_owned();
    let mut edits = Vec::new();
    for link in links {
        let dest = link.dest.as_str();
        if is_external(dest) {
            continue;
        }
        let (path, fragment) = match dest.split_once('#') {
            Some((path, fragment)) => (path, Some(fragment)),
            None => (dest, None),
        };
        if path.is_empty() {
            if let Some(fragment) = fragment
                && !chapter.ids.contains(fragment)
            {
                errors_at(&mut errors, dest, "no such heading on this page".into());
            }
            continue;
        }
        let Some(target) = normalize(&base, path) else {
            errors_at(&mut errors, dest, "leaves the repository".into());
            continue;
        };
        let on_disk = root.join(&target);
        if !on_disk.exists() {
            errors_at(&mut errors, dest, format!("`{target}` does not exist"));
            continue;
        }
        let replacement = if let Some(page) = by_source.get(target.as_str()) {
            if let Some(fragment) = fragment
                && !page.ids.contains(fragment)
            {
                errors_at(
                    &mut errors,
                    dest,
                    format!("`{target}` has no heading `#{fragment}`"),
                );
                continue;
            }
            with_fragment(relative(&chapter.book_path, &page.book_path), fragment)
        } else if let Some(inside) = target.strip_prefix(&format!("{src}/"))
            && !target.ends_with(".md")
        {
            // A non-document asset under the book source is copied by mdBook
            // itself; keep it relative to where this page is published.
            with_fragment(relative(&chapter.book_path, inside), fragment)
        } else if target.starts_with(&format!("{src}/")) {
            errors_at(
                &mut errors,
                dest,
                format!("`{target}` is not a chapter of the book"),
            );
            continue;
        } else {
            if let Some(fragment) = fragment
                && target.ends_with(".md")
            {
                let text = fs::read_to_string(&on_disk).unwrap_or_default();
                if !heading_ids(&text, IdStyle::GitHub).contains(fragment) {
                    errors_at(
                        &mut errors,
                        dest,
                        format!("`{target}` has no heading `#{fragment}`"),
                    );
                    continue;
                }
            }
            let kind = if on_disk.is_dir() { "tree" } else { "blob" };
            let url = format!("{}/{kind}/{}/{target}", repository.url, repository.branch);
            with_fragment(url, fragment)
        };
        if replacement != dest {
            edits.push((link.span, replacement));
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    // Applied last-first by position, so each replacement leaves every earlier
    // offset valid. `links` reports inline links and reference definitions
    // from separate passes, so its order is not source order.
    edits.sort_unstable_by_key(|(span, _): &(Range<usize>, String)| std::cmp::Reverse(span.start));
    let mut content = chapter.content.clone();
    for (span, replacement) in edits {
        content.replace_range(span, &replacement);
    }
    Ok(content)
}

fn with_fragment(path: String, fragment: Option<&str>) -> String {
    match fragment {
        Some(fragment) => format!("{path}#{fragment}"),
        None => path,
    }
}

fn is_external(dest: &str) -> bool {
    dest.contains("://")
        || dest.starts_with("mailto:")
        || dest.starts_with("tel:")
        || dest.starts_with("//")
        || dest.starts_with('/')
}

fn options() -> Options {
    // The extensions mdBook's renderer enables; a construct it parses must
    // be one this parses, or a link inside it would be missed.
    Options::ENABLE_TABLES
        | Options::ENABLE_FOOTNOTES
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_HEADING_ATTRIBUTES
}

/// Every link and image destination in `content`, with the byte range of the
/// destination text itself. A destination whose text cannot be located —
/// escaped characters, say — is reported rather than silently left alone.
fn links(content: &str) -> (Vec<Link>, Vec<String>) {
    let parser = Parser::new_ext(content, options());
    let mut found = Vec::new();
    let mut errors = Vec::new();
    for (_, def) in parser.reference_definitions().iter() {
        let span = def.span.clone();
        match locate(&content[span.clone()], &def.dest, "]:") {
            Some(inner) => found.push(Link {
                dest: def.dest.to_string(),
                span: span.start + inner.start..span.start + inner.end,
            }),
            None => errors.push(format!("cannot locate link target `{}`", def.dest)),
        }
    }
    for (event, span) in parser.into_offset_iter() {
        let dest = match event {
            Event::Start(Tag::Link {
                link_type,
                dest_url,
                ..
            })
            | Event::Start(Tag::Image {
                link_type,
                dest_url,
                ..
            }) => {
                use pulldown_cmark::LinkType::*;
                match link_type {
                    Inline => dest_url,
                    // Reference forms resolve through their definition,
                    // handled above; autolinks are absolute by construction.
                    _ => continue,
                }
            }
            _ => continue,
        };
        match locate(&content[span.clone()], &dest, "](") {
            Some(inner) => found.push(Link {
                dest: dest.to_string(),
                span: span.start + inner.start..span.start + inner.end,
            }),
            None => errors.push(format!("cannot locate link target `{dest}`")),
        }
    }
    (found, errors)
}

/// The range of `dest` within `text`, where it follows the last `opener`
/// (optionally inside angle brackets and after spaces).
fn locate(text: &str, dest: &CowStr<'_>, opener: &str) -> Option<Range<usize>> {
    text.rmatch_indices(opener).find_map(|(at, _)| {
        let after = at + opener.len();
        let rest = &text[after..];
        let skipped = rest.len() - rest.trim_start().len();
        let start = after + skipped;
        let rest = &text[start..];
        let start = if rest.starts_with('<') {
            start + 1
        } else {
            start
        };
        text[start..]
            .starts_with(dest.as_ref())
            .then(|| start..start + dest.len())
    })
}

/// Which renderer's heading anchors to compute.
#[derive(Clone, Copy)]
enum IdStyle {
    /// mdBook 0.4: ASCII-lowercased, so `Über` stays `Über`.
    MdBook,
    /// GitHub: full Unicode lowercasing.
    GitHub,
}

/// The anchor ids a renderer gives the headings in `content`, including the
/// `-1`, `-2` suffixes of repeated headings and explicit `{#id}` attributes.
fn heading_ids(content: &str, style: IdStyle) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut heading: Option<(Option<String>, String)> = None;
    for event in Parser::new_ext(content, options()) {
        match event {
            Event::Start(Tag::Heading { id, .. }) => {
                heading = Some((id.map(|id| id.to_string()), String::new()));
            }
            Event::Text(text) | Event::Code(text) => {
                if let Some((_, buffer)) = heading.as_mut() {
                    buffer.push_str(&text);
                }
            }
            Event::End(TagEnd::Heading(_)) => {
                let Some((explicit, text)) = heading.take() else {
                    continue;
                };
                let base = explicit.unwrap_or_else(|| slug(&text, style));
                let count = seen.entry(base.clone()).or_insert(0);
                let id = if *count == 0 {
                    base.clone()
                } else {
                    format!("{base}-{count}")
                };
                *count += 1;
                ids.insert(id);
            }
            _ => {}
        }
    }
    ids
}

fn slug(text: &str, style: IdStyle) -> String {
    text.trim()
        .chars()
        .flat_map(|ch| {
            let kept: Vec<char> = if ch.is_alphanumeric() || ch == '_' || ch == '-' {
                match style {
                    IdStyle::MdBook => vec![ch.to_ascii_lowercase()],
                    IdStyle::GitHub => ch.to_lowercase().collect(),
                }
            } else if ch.is_whitespace() {
                vec!['-']
            } else {
                Vec::new()
            };
            kept
        })
        .collect()
}

/// `base/path`, lexically normalized, as a `/`-separated repository path; or
/// `None` if it climbs out of the repository.
fn normalize(base: &Path, path: &str) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    for component in base.join(path).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir => {
                parts.pop()?;
            }
            Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(parts.join("/"))
}

/// The link from the page published at `from` to the one at `to`, both
/// paths under the book source.
fn relative(from: &str, to: &str) -> String {
    let from: Vec<&str> = from.split('/').collect();
    let from_dirs = &from[..from.len() - 1];
    let to: Vec<&str> = to.split('/').collect();
    let shared = from_dirs
        .iter()
        .zip(&to)
        .take_while(|(a, b)| a == b)
        .count();
    let mut parts = vec![".."; from_dirs.len() - shared];
    parts.extend(&to[shared..]);
    parts.join("/")
}

fn slash(path: &Path) -> String {
    path.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_match_mdbook_for_punctuation_duplicates_and_attributes() {
        let ids = heading_ids(
            "# A\n## Staged admission interface (GL-96)\n## A — B\n## `code_x` & more <em>x</em>\n\
             ## Foo\n## Foo\n## Tollgate's design: \"quoted\"\n## Über Straße 1.5 **bold** [link](x.md)\n\
             ## Custom {#my-id}\n",
            IdStyle::MdBook,
        );
        let expected = [
            "a",
            "staged-admission-interface-gl-96",
            "a--b",
            "code_x--more-x",
            "foo",
            "foo-1",
            "tollgates-design-quoted",
            "Über-straße-15-bold-link",
            "my-id",
        ];
        assert_eq!(ids, expected.into_iter().map(String::from).collect());
    }

    #[test]
    fn github_ids_lowercase_unicode() {
        assert!(heading_ids("# Über\n", IdStyle::GitHub).contains("über"));
    }

    #[test]
    fn relative_links_climb_only_the_directories_not_shared() {
        assert_eq!(relative("EMBEDDING.md", "DESIGN.md"), "DESIGN.md");
        assert_eq!(relative("site/intro.md", "EMBEDDING.md"), "../EMBEDDING.md");
        assert_eq!(
            relative("EMBEDDING.md", "site/invariants.md"),
            "site/invariants.md"
        );
        assert_eq!(relative("site/a.md", "site/b.md"), "b.md");
    }

    #[test]
    fn normalization_refuses_to_leave_the_repository() {
        assert_eq!(
            normalize(Path::new("docs"), "../INVARIANTS.md").as_deref(),
            Some("INVARIANTS.md")
        );
        assert_eq!(
            normalize(Path::new("docs"), "./a/../b.md").as_deref(),
            Some("docs/b.md")
        );
        assert_eq!(normalize(Path::new("docs"), "../../x"), None);
        assert_eq!(normalize(Path::new(""), "/etc/passwd"), None);
    }

    #[test]
    fn a_repo_page_is_recognised_only_on_its_first_line() {
        assert_eq!(
            repo_page("\n<!-- repo-page: INVARIANTS.md -->\nx"),
            Some("INVARIANTS.md")
        );
        assert_eq!(
            repo_page("# Title\n<!-- repo-page: INVARIANTS.md -->"),
            None
        );
    }

    #[test]
    fn destinations_are_located_exactly_including_angle_brackets() {
        let text = "See [a](b.md) and [c](<d e.md>) and ![i](img.png \"t\").";
        let (found, errors) = links(text);
        assert!(errors.is_empty());
        let spans: Vec<&str> = found.iter().map(|l| &text[l.span.clone()]).collect();
        assert_eq!(spans, ["b.md", "d e.md", "img.png"]);
    }

    #[test]
    fn links_in_code_are_not_links() {
        let (found, _) = links("`[a](b.md)`\n\n```\n[c](d.md)\n```\n");
        assert!(found.is_empty());
    }

    #[test]
    fn reference_definitions_are_rewritten_where_they_are_defined() {
        let text = "Use [the contract][c].\n\n[c]: ../INVARIANTS.md\n";
        let (found, errors) = links(text);
        assert!(errors.is_empty());
        let spans: Vec<&str> = found.iter().map(|l| &text[l.span.clone()]).collect();
        assert_eq!(spans, ["../INVARIANTS.md"]);
    }
}
