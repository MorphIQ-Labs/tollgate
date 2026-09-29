# tollgate-axum

Axum integration for Tollgate's quota admission and usage accounting.

Share an existing `InstanceRuntime` handle, local authenticator, application
clock, request-ID source and startup-selected capacity gate through `Tollgate`.
The application keeps ownership of the runtime and its graceful shutdown.
No store backend is selected by the adapter, and no worker starts per route.

`prepare_json` verifies identity, checks route permission and reserves usage
queue capacity before decoding bounded JSON. Its owned `Prepared` value keeps
the original policy context and queue permit. Dropping it charges nothing;
custom integrations can consume `into_parts` to validate input and continue
through the existing admission/capacity/commit API.

Install `TollgateConnection` with Axum's
`into_make_service_with_connect_info::<TollgateConnection>()` when using
`BearerAuth`. Missing connection state is an error. The cache belongs to one
accepted connection; do not install a single global connection cache.

Route input limits are explicit. Axum's existing body limit still applies;
the adapter never widens it. Readiness and diagnostic reports come from the
existing runtime. Stop HTTP admission and start runtime shutdown together,
allowing owned request permits to participate in the bounded drain.

Declare a metered POST with `post_json(operation, permissions, limits,
validate, execute)`. Validation returns `Validated<T>` with the checked item
quantity; the pinned account cost table supplies the price. The wrapper
reserves funding, acquires its typed capacity gate, and commits immediately
before constructing the execution future. The callback receives authoritative
`ChargeMetadata` from that committed guard. `post` supports bodyless fixed
workloads. A request cannot select its own price or capacity class.

Return `BufferedResponse::bytes` or `BufferedResponse::json` after work and
serialization finish. The wrapper retains the charge guard across the future,
including handler errors, cancellation and panic unwinding. Streaming bodies,
upgrades, detached tasks, blocking work surviving cancellation, and automatic
retry layers require an explicit low-level integration. Network transmission
of already completed output is outside the billable execution lifetime.

`post_json_with_error_handler` customizes local HTTP errors. Its renderer gets
`None` before execution and committed metadata after execution; it must never
retry work. The default renderer distinguishes capacity saturation from rate
limiting and reports the committed units for serialization failures.

Request IDs must be unique across instances and restarts. Inject a local,
nonblocking `RequestIdSource` (a closure is supported); a source failure refuses
before admission. A process-local counter is only suitable for tests.
