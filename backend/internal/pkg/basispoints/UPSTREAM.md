# Basis Points protocol provenance

The protocol implementation and its tests are ported from
https://github.com/JaxsonWang/cpa-plugin-oai-basispoints
at v0.1.18, commit 11df6f8855847ec1957b0d2f4271a9cd9b13cfe1.
The original MIT license is included in LICENSE.

Qerkai adaptations:

- native.go replaces the CPA HTTP/stream host ABI with cancellable in-process HTTP.
- Native requests use immutable configuration snapshots and Qerkai-managed OAuth credentials.
- Tool history is scoped by trusted tenant/account/model and session identity.
- The bounded attachment cache is shared between native requests, retaining the upstream credential isolation.
- Native attachment-cache waiters honor request cancellation without interrupting another request's upload.
- Native host options observe real upstream events for Qerkai's selected first-token metric. Network mode releases response notifications immediately, while tool validation and the no-retry-after-delivery boundary remain enforced. The default CPA executor retains its original delivery behavior.
- All protocol tests are retained. Only distribution_test.go, which validates the CPA plugin-store registry, is excluded because this is not a CPA distribution.

Plugin auth/register entry points are retained for source test compatibility but are not used by Qerkai's runtime.
