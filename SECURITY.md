# Security policy

Tollgate decides who may spend what, and records what was spent. A flaw that
lets work run without being charged, charges twice, bypasses authentication,
leaks a credential or its digest, or corrupts the ledger is a security issue.

## Reporting a vulnerability

Report privately through GitHub: on this repository's **Security** tab, choose
**Report a vulnerability**. Please do not open a public issue, pull request, or
discussion for a suspected vulnerability.

A useful report says which crate and version or commit is affected, what an
attacker controls, and what they gain, with a reproduction if you have one. A
failing test against the invariants in [`INVARIANTS.md`](INVARIANTS.md) is the
most direct form.

The report is acknowledged and tracked in the private advisory. A fix is
developed there, released, and then disclosed through a GitHub security
advisory, crediting the reporter unless they ask otherwise.

## Supported versions

Tollgate is pre-1.0. Fixes land on `main` and ship in the next release of the
current `0.x` series; earlier releases are not patched.

## Scope

In scope: the crates in this repository, including the `tollgate-server`
control plane's authentication, TLS, and authorization. Deployment
configuration is the operator's, but documentation that leads to an insecure
deployment is in scope; see
[`docs/CONTROL_PLANE_SECURITY.md`](docs/CONTROL_PLANE_SECURITY.md).

Out of scope: `examples/`, which demonstrate embedding with demo credentials,
and denial of service that requires control of the store or the network path
between instances and the server.
