//! The HTTP API reference's route table, checked against the router.
//!
//! `docs/HTTP_API.md` lists every control-plane route between
//! `<!-- http-routes:start -->` and `<!-- http-routes:end -->`, one row per
//! method and path with the role that may call it. A table typed out once
//! drifts the first time a route is added, so it is derived instead: every
//! `.route("…", <method>(…))` registration under `crates/tollgate-server/src/`
//! is parsed with `syn`, including chained methods (`post(a).get(b)`), routers
//! bound to a local and nested under a prefix (`.nest(API_PREFIX, inner)`), and
//! merged routers. A prefix may be a string literal or a `&str` constant
//! declared under any `crates/*/src/`; a constant name declared with two
//! different values is refused rather than guessed.
//!
//! **Roles are derived from the authorization layer, not from handlers.** A
//! `route_layer(…)` or `layer(…)` whose arguments name `Role::Instance` or
//! `Role::Operator` assigns that role to every route registered on the router
//! before it, which is the scope axum gives such a layer. A route behind no
//! such layer is `none`. The check cannot see what a handler extracts or what
//! the named layer does at runtime; it trusts that a layer naming a `Role` is
//! the authorization layer. A route under two different roles is refused.
//!
//! Registrations the check cannot evaluate — `route_service`, `nest_service`,
//! a method router it does not recognise, a path that is not a literal or a
//! known constant — fail the check rather than being skipped. Test modules and
//! `#[cfg(test)]` items are ignored.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};
use syn::visit::{self, Visit};

const START: &str = "<!-- http-routes:start -->";
const END: &str = "<!-- http-routes:end -->";
const SERVER: &str = "crates/tollgate-server/src";
const PAGE: &str = "docs/HTTP_API.md";

/// Method routers the check understands, as axum's `routing` functions and
/// `MethodRouter` methods name them.
const METHODS: [&str; 10] = [
    "get", "post", "put", "delete", "patch", "head", "options", "trace", "connect", "any",
];

/// One route as the table shows it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Route {
    pub path: String,
    pub method: String,
    pub role: String,
}

impl Route {
    /// The table row the page must carry for this route.
    pub fn render(&self) -> String {
        format!("| {} | {} | {} |", self.method, self.path, self.role)
    }
}

/// The routes the page must show, derived from the server's source.
pub fn expected(root: &Path) -> Result<Vec<Route>, String> {
    let mut consts = BTreeMap::new();
    for crate_dir in fs::read_dir(root.join("crates"))
        .map_err(|e| format!("cannot read {}: {e}", root.join("crates").display()))?
        .flatten()
    {
        let src = crate_dir.path().join("src");
        if src.is_dir() {
            for path in crate::files(&src, "rs")? {
                string_consts(&read(&path)?, &mut consts)
                    .map_err(|e| format!("{}: {e}", path.display()))?;
            }
        }
    }
    let mut sources = Vec::new();
    for path in crate::files(&root.join(SERVER), "rs")? {
        sources.push((path.display().to_string(), read(&path)?));
    }
    let sources: Vec<(&str, &str)> = sources
        .iter()
        .map(|(name, text)| (name.as_str(), text.as_str()))
        .collect();
    routes(&sources, &consts)
}

/// Problems with the page's table; empty when it matches the router.
pub fn check(root: &Path) -> Result<Vec<String>, String> {
    let expected = expected(root)?;
    if expected.is_empty() {
        return Err(format!("no .route registrations found under {SERVER}"));
    }
    let page = read(&root.join(PAGE))?;
    Ok(compare(&expected, &table(&page)?))
}

/// Compare the router's routes with the table's rows.
pub fn compare(expected: &[Route], table: &[Route]) -> Vec<String> {
    let mut problems = Vec::new();
    let key = |route: &Route| (route.method.clone(), route.path.clone());
    let want: BTreeMap<_, _> = expected.iter().map(|r| (key(r), r)).collect();
    let mut have = BTreeMap::new();
    for row in table {
        if have.insert(key(row), row).is_some() {
            problems.push(format!("duplicate row: {}", row.render()));
        }
    }
    let methods = |routes: &BTreeMap<(String, String), &Route>, path: &str| -> Vec<String> {
        routes
            .keys()
            .filter(|(_, p)| p == path)
            .map(|(m, _)| m.clone())
            .collect()
    };
    for (k, route) in &want {
        match have.get(k) {
            Some(row) if row.role != route.role => problems.push(format!(
                "role differs for {} {}: the router requires `{}`, the table says `{}`",
                route.method, route.path, route.role, row.role
            )),
            Some(_) => {}
            None if !methods(&have, &route.path).is_empty() => problems.push(format!(
                "method differs for {}: the router serves {:?}, the table lists {:?}",
                route.path,
                methods(&want, &route.path),
                methods(&have, &route.path)
            )),
            None => problems.push(format!("missing row: {}", route.render())),
        }
    }
    // Every documented method the router does not serve is stale, whether or
    // not its path survives under another method.
    for (k, row) in &have {
        if !want.contains_key(k) {
            problems.push(format!("row with no route: {}", row.render()));
        }
    }
    problems
}

/// The rows between the page's markers.
pub fn table(page: &str) -> Result<Vec<Route>, String> {
    let Some(block) = page
        .split_once(START)
        .and_then(|(_, rest)| rest.split_once(END))
        .map(|(block, _)| block)
    else {
        return Err(format!("{PAGE} has no {START} … {END} table"));
    };
    let mut rows = Vec::new();
    for line in block.lines().map(str::trim).filter(|l| l.starts_with('|')) {
        let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
        if cells.first() == Some(&"Method") || cells.iter().all(|c| c.starts_with("---")) {
            continue;
        }
        let [method, path, role] = cells[..] else {
            return Err(format!("malformed route row: {line}"));
        };
        rows.push(Route {
            path: path.to_owned(),
            method: method.to_owned(),
            role: role.to_owned(),
        });
    }
    Ok(rows)
}

/// Every route registered in `sources`, sorted by path then method.
/// `consts` resolves a prefix or path named by a constant; a name mapped to
/// more than one value is ambiguous and refused where it is used.
pub fn routes(
    sources: &[(&str, &str)],
    consts: &BTreeMap<String, BTreeSet<String>>,
) -> Result<Vec<Route>, String> {
    let mut found = Vec::new();
    for (name, source) in sources {
        let file = syn::parse_file(source).map_err(|e| format!("{name}: {e}"))?;
        let mut functions = Functions::default();
        functions.visit_file(&file);
        for body in functions.bodies {
            let mut collect = Collect::default();
            collect.visit_block(body);
            let mut eval = Eval {
                consts,
                bindings: collect.bindings,
                used: BTreeSet::new(),
            };
            let mut entries = Vec::new();
            for root in &collect.roots {
                entries.extend(eval.router(root).map_err(|e| format!("{name}: {e}"))?);
            }
            let unused: Vec<String> = eval
                .bindings
                .keys()
                .filter(|binding| !eval.used.contains(*binding))
                .cloned()
                .collect();
            for binding in unused {
                let expr = eval.bindings[&binding];
                entries.extend(eval.router(expr).map_err(|e| format!("{name}: {e}"))?);
            }
            found.extend(entries);
        }
    }
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    for entry in found {
        if !seen.insert((entry.method.clone(), entry.path.clone())) {
            return Err(format!(
                "{} {} is registered twice",
                entry.method, entry.path
            ));
        }
        result.push(Route {
            path: entry.path,
            method: entry.method,
            role: entry.role.unwrap_or_else(|| "none".to_owned()),
        });
    }
    result.sort();
    Ok(result)
}

/// Record every `&str` constant initialised with a string literal.
pub fn string_consts(
    source: &str,
    consts: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), String> {
    struct Consts<'a>(&'a mut BTreeMap<String, BTreeSet<String>>);
    impl<'ast> Visit<'ast> for Consts<'_> {
        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if !test_only(&item.attrs) {
                visit::visit_item_mod(self, item);
            }
        }
        fn visit_item_const(&mut self, item: &'ast syn::ItemConst) {
            if let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(text),
                ..
            }) = item.expr.as_ref()
                && !test_only(&item.attrs)
            {
                self.0
                    .entry(item.ident.to_string())
                    .or_default()
                    .insert(text.value());
            }
        }
    }
    let file = syn::parse_file(source).map_err(|e| e.to_string())?;
    Consts(consts).visit_file(&file);
    Ok(())
}

fn read(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|e| format!("cannot read {}: {e}", path.display()))
}

fn test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("test")
            || (attr.path().is_ident("cfg")
                && matches!(&attr.meta, syn::Meta::List(list)
                    if list.tokens.to_string().split(|c: char| !c.is_alphanumeric() && c != '_')
                        .any(|word| word == "test")))
    })
}

/// Function bodies outside test code.
#[derive(Default)]
struct Functions<'ast> {
    bodies: Vec<&'ast syn::Block>,
}

impl<'ast> Visit<'ast> for Functions<'ast> {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if !test_only(&item.attrs) {
            visit::visit_item_mod(self, item);
        }
    }
    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if !test_only(&item.attrs) {
            self.bodies.push(&item.block);
        }
    }
    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        if !test_only(&item.attrs) {
            self.bodies.push(&item.block);
        }
    }
}

/// Whether a method-call chain registers routes anywhere along it.
fn registers(expr: &syn::Expr) -> bool {
    let mut current = expr;
    loop {
        match current {
            syn::Expr::MethodCall(call) => {
                if matches!(
                    call.method.to_string().as_str(),
                    "route" | "nest" | "merge" | "route_service" | "nest_service"
                ) {
                    return true;
                }
                current = &call.receiver;
            }
            syn::Expr::Paren(inner) => current = &inner.expr,
            syn::Expr::Group(inner) => current = &inner.expr,
            _ => return false,
        }
    }
}

/// The outermost router expressions of one function body, and the locals
/// bound to one.
#[derive(Default)]
struct Collect<'ast> {
    bindings: BTreeMap<String, &'ast syn::Expr>,
    roots: Vec<&'ast syn::Expr>,
}

impl<'ast> Visit<'ast> for Collect<'ast> {
    fn visit_local(&mut self, local: &'ast syn::Local) {
        if let (syn::Pat::Ident(name), Some(init)) = (&local.pat, &local.init)
            && registers(&init.expr)
        {
            self.bindings.insert(name.ident.to_string(), &init.expr);
            return;
        }
        visit::visit_local(self, local);
    }
    fn visit_expr(&mut self, expr: &'ast syn::Expr) {
        if registers(expr) {
            self.roots.push(expr);
        } else {
            visit::visit_expr(self, expr);
        }
    }
    // A nested function is its own scope, and is visited as one.
    fn visit_item_fn(&mut self, _: &'ast syn::ItemFn) {}
}

struct Entry {
    method: String,
    path: String,
    role: Option<String>,
}

struct Eval<'a, 'ast> {
    consts: &'a BTreeMap<String, BTreeSet<String>>,
    bindings: BTreeMap<String, &'ast syn::Expr>,
    used: BTreeSet<String>,
}

impl Eval<'_, '_> {
    fn router(&mut self, expr: &syn::Expr) -> Result<Vec<Entry>, String> {
        match expr {
            syn::Expr::Paren(inner) => self.router(&inner.expr),
            syn::Expr::Group(inner) => self.router(&inner.expr),
            syn::Expr::Call(call) if last_segment(&call.func).as_deref() == Some("new") => {
                Ok(Vec::new())
            }
            syn::Expr::Path(path) if path.path.get_ident().is_some() => {
                let name = path.path.segments[0].ident.to_string();
                let bound = *self
                    .bindings
                    .get(&name)
                    .ok_or_else(|| format!("router `{name}` is not a local bound to a router"))?;
                if !self.used.insert(name.clone()) {
                    return Err(format!("router `{name}` is used twice"));
                }
                self.router(bound)
            }
            syn::Expr::MethodCall(call) => {
                let mut entries = self.router(&call.receiver)?;
                let arg = |index: usize| {
                    call.args
                        .iter()
                        .nth(index)
                        .ok_or_else(|| format!(".{}(…) has too few arguments", call.method))
                };
                match call.method.to_string().as_str() {
                    "route" => {
                        let path = self.string(arg(0)?)?;
                        for method in methods(arg(1)?)? {
                            entries.push(Entry {
                                method,
                                path: path.clone(),
                                role: None,
                            });
                        }
                    }
                    "nest" => {
                        let prefix = self.string(arg(0)?)?;
                        for mut inner in self.router(arg(1)?)? {
                            inner.path = if inner.path == "/" {
                                prefix.clone()
                            } else {
                                format!("{prefix}{}", inner.path)
                            };
                            entries.push(inner);
                        }
                    }
                    "merge" => entries.extend(self.router(arg(0)?)?),
                    "route_layer" | "layer" => {
                        if let Some(role) = role(&call.args)? {
                            for entry in &mut entries {
                                match &entry.role {
                                    Some(existing) if *existing != role => {
                                        return Err(format!(
                                            "{} {} is behind both `{existing}` and `{role}`",
                                            entry.method, entry.path
                                        ));
                                    }
                                    _ => entry.role = Some(role.clone()),
                                }
                            }
                        }
                    }
                    "route_service" | "nest_service" => {
                        return Err(format!(
                            ".{}(…) serves every method; list it by hand and extend this check",
                            call.method
                        ));
                    }
                    _ => {}
                }
                Ok(entries)
            }
            _ => Err("unsupported router expression".to_owned()),
        }
    }

    fn string(&self, expr: &syn::Expr) -> Result<String, String> {
        match expr {
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(text),
                ..
            }) => Ok(text.value()),
            syn::Expr::Path(path) => {
                let name = last_segment(expr).unwrap_or_default();
                let values = self
                    .consts
                    .get(&name)
                    .ok_or_else(|| format!("no string constant `{name}`"))?;
                let mut values = values.iter();
                match (values.next(), values.next()) {
                    (Some(value), None) => Ok(value.clone()),
                    _ => Err(format!(
                        "string constant `{}` is declared with more than one value",
                        path.path
                            .segments
                            .iter()
                            .map(|s| s.ident.to_string())
                            .collect::<Vec<_>>()
                            .join("::")
                    )),
                }
            }
            _ => Err("a route path must be a string literal or a string constant".to_owned()),
        }
    }
}

/// The methods a method router serves, in the order they are chained.
fn methods(expr: &syn::Expr) -> Result<Vec<String>, String> {
    match expr {
        syn::Expr::Paren(inner) => methods(&inner.expr),
        syn::Expr::Group(inner) => methods(&inner.expr),
        syn::Expr::Call(call) => match last_segment(&call.func) {
            Some(name) if METHODS.contains(&name.as_str()) => Ok(vec![name.to_uppercase()]),
            Some(name) => Err(format!("unrecognised method router `{name}(…)`")),
            None => Err("unrecognised method router".to_owned()),
        },
        syn::Expr::MethodCall(call) => {
            let mut found = methods(&call.receiver)?;
            let name = call.method.to_string();
            if METHODS.contains(&name.as_str()) {
                found.push(name.to_uppercase());
            } else if name == "on" {
                return Err("`.on(…)` method filters are not supported".to_owned());
            }
            Ok(found)
        }
        _ => Err("unrecognised method router".to_owned()),
    }
}

/// The role a layer's arguments name, if any.
fn role(
    args: &syn::punctuated::Punctuated<syn::Expr, syn::Token![,]>,
) -> Result<Option<String>, String> {
    #[derive(Default)]
    struct Roles(BTreeSet<String>);
    impl<'ast> Visit<'ast> for Roles {
        fn visit_path(&mut self, path: &'ast syn::Path) {
            let names: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
            if let [.., kind, role] = names.as_slice()
                && kind == "Role"
            {
                self.0.insert(role.to_lowercase());
            }
            visit::visit_path(self, path);
        }
    }
    let mut roles = Roles::default();
    for arg in args {
        roles.visit_expr(arg);
    }
    let mut roles = roles.0.into_iter();
    match (roles.next(), roles.next()) {
        (role, None) => Ok(role),
        (Some(a), Some(b)) => Err(format!("one layer names two roles, `{a}` and `{b}`")),
        (None, Some(_)) => unreachable!("an iterator yields a second item only after a first"),
    }
}

fn last_segment(expr: &syn::Expr) -> Option<String> {
    match expr {
        syn::Expr::Path(path) => path.path.segments.last().map(|s| s.ident.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = r#"
        const PREFIX: &str = "/v2";
        fn router(state: State) -> Router {
            let inner = Router::new()
                .route("/things", get(list).post(create))
                .route("/things/{id}", axum::routing::delete(remove))
                .route_layer(from_fn_with_state(auth(Role::Instance), authorize));
            let admin = Router::new()
                .route("/users", put(set).layer(DefaultBodyLimit::max(10)))
                .route_layer(from_fn_with_state(auth(Role::Operator), authorize));
            Router::new()
                .route("/livez", get(async || StatusCode::OK))
                .nest(PREFIX, inner.nest("/admin", admin))
                .with_state(state)
                .layer(from_fn(report))
        }
        #[cfg(test)]
        mod tests {
            fn fixture() -> Router { Router::new().route("/test-only", get(x)) }
        }
    "#;

    fn fixture_routes() -> Vec<String> {
        let mut consts = BTreeMap::new();
        string_consts(FIXTURE, &mut consts).expect("parses");
        routes(&[("fixture.rs", FIXTURE)], &consts)
            .expect("evaluates")
            .iter()
            .map(Route::render)
            .collect()
    }

    #[test]
    fn chained_nested_and_layered_routes_are_all_found() {
        assert_eq!(
            fixture_routes(),
            [
                "| GET | /livez | none |",
                "| PUT | /v2/admin/users | operator |",
                "| GET | /v2/things | instance |",
                "| POST | /v2/things | instance |",
                "| DELETE | /v2/things/{id} | instance |",
            ]
        );
    }

    #[test]
    fn an_unknown_prefix_or_method_router_is_refused_not_skipped() {
        let consts = BTreeMap::new();
        let unknown =
            r#"fn f() { Router::new().nest(MISSING, Router::new().route("/a", get(h))) }"#;
        assert!(routes(&[("a.rs", unknown)], &consts).is_err());
        let service = r#"fn f() { Router::new().route_service("/a", svc) }"#;
        assert!(routes(&[("b.rs", service)], &consts).is_err());
        let filter = r#"fn f() { Router::new().route("/a", on(MethodFilter::GET, h)) }"#;
        assert!(routes(&[("c.rs", filter)], &consts).is_err());
    }

    #[test]
    fn a_constant_with_two_values_is_ambiguous() {
        let mut consts = BTreeMap::new();
        string_consts(r#"const P: &str = "/a";"#, &mut consts).unwrap();
        string_consts(r#"const P: &str = "/b";"#, &mut consts).unwrap();
        let source = r#"fn f() { Router::new().nest(P, Router::new().route("/x", get(h))) }"#;
        assert!(routes(&[("a.rs", source)], &consts).is_err());
    }

    #[test]
    fn the_table_is_read_between_its_markers() {
        let page = "| GET | /outside | none |\n<!-- http-routes:start -->\n\
                    | Method | Path | Role |\n|---|---|---|\n| GET | /livez | none |\n\
                    <!-- http-routes:end -->\n";
        assert_eq!(
            table(page).unwrap(),
            [Route {
                path: "/livez".into(),
                method: "GET".into(),
                role: "none".into()
            }]
        );
        assert!(table("no markers").is_err());
        assert!(table(&page.replace("| GET | /livez | none |", "| GET | /livez |")).is_err());
    }

    #[test]
    fn drift_in_either_direction_is_reported() {
        let route = |method: &str, path: &str, role: &str| Route {
            path: path.into(),
            method: method.into(),
            role: role.into(),
        };
        let router = [route("GET", "/a", "none"), route("POST", "/b", "instance")];
        assert!(compare(&router, &router).is_empty());
        let problems = compare(
            &router,
            &[
                route("GET", "/b", "instance"),
                route("GET", "/c", "none"),
                route("GET", "/a", "operator"),
            ],
        );
        assert_eq!(problems.len(), 4, "{problems:?}");
        assert!(problems[0].starts_with("role differs for GET /a"));
        assert!(problems[1].starts_with("method differs for /b"));
        assert!(problems[2].starts_with("row with no route: | GET | /b"));
        assert!(problems[3].starts_with("row with no route: | GET | /c"));
        let missing = compare(&router, &[route("GET", "/a", "none")]);
        assert_eq!(missing, ["missing row: | POST | /b | instance |"]);
    }

    /// A path that keeps one method but loses another still has a stale row,
    /// and the check must name it rather than accept it because the path
    /// survives (#36 review).
    #[test]
    fn a_documented_method_the_router_no_longer_serves_is_reported() {
        let route = |method: &str, path: &str| Route {
            path: path.into(),
            method: method.into(),
            role: "none".into(),
        };
        let problems = compare(
            &[route("GET", "/livez")],
            &[route("GET", "/livez"), route("POST", "/livez")],
        );
        assert_eq!(problems, ["row with no route: | POST | /livez | none |"]);
    }
}
