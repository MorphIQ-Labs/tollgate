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

This crate is being assembled on the Axum integration branch. The metered
route wrapper and end-to-end guide follow the staging boundary; the current
API does not automatically commit or execute application work.
