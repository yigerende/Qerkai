# Basis Points protocol provenance

The protocol implementation and its tests are ported from
https://github.com/JaxsonWang/cpa-plugin-oai-basispoints
at v0.2.2, commit 25ec8d06225edbb6f4293ffc29fba15960e43a30.
The original MIT license is included in LICENSE.

Qerkai adaptations:

- native.go replaces the CPA HTTP/stream host ABI with cancellable in-process HTTP.
- BPS upstream WS uses the upstream v0.2.2 wire protocol, one handshake per generation,
  safe pre-connection HTTP fallback, and no transport replay after connection establishment.
  Native cancellation is attached directly to the Qerkai request context; CPA lifecycle
  headers are only used by the retained ABI compatibility tests.
- Qerkai defaults to HTTP, including saved settings predating this upgrade. Its BPS
  `upstream_transport: auto` is the sole WS permission, independent of account-level
  Codex WS/force-HTTP settings. The account's proxy is still inherited; no configured
  proxy means direct transport, matching Qerkai HTTP rather than environment proxies.
- Native requests use immutable configuration snapshots and Qerkai-managed OAuth credentials.
- Tool history is scoped by trusted tenant/account/model and session identity.
- The bounded attachment cache is shared between native requests, retaining the upstream credential isolation.
- Native attachment-cache waiters honor request cancellation without interrupting another request's upload.
- Native host options observe real upstream events for Qerkai's selected first-token metric. Network mode releases response notifications immediately, while tool validation and the no-retry-after-delivery boundary remain enforced. The default CPA executor retains its original delivery behavior.
- Upstream protocol tests are retained, including WS, history replay, and strict tool
  validation. CPA distribution and source-auth-file management tests/pages are excluded:
  Qerkai manages OAuth accounts and BPS settings through its existing UI and database.

Plugin auth/register entry points are retained for source test compatibility but are not used by Qerkai's runtime.
