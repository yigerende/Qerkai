# OpenAI WS Business Retry Validation

Validated on Windows, 2026-09-16, using Go 1.27.0.

## Scope

The opt-in retry applies to streaming HTTP `/responses` requests using forced
upstream WS on OpenAI OAuth-like accounts. It recognizes the designated
processing-failure (502) and server-overload (503) messages inside `error` or
`response.failed` events. A numeric 502/503 by itself does not opt in.

Connection establishment, legacy retries, direct WS ingress and other endpoints
retain their existing retry paths. Disabling the switch does not create business
retry state or adapt the downstream stream.

Recognized metadata notifications remain immediate. Answer, reasoning, tool,
output-item and unknown output events prevent another whole-request replay.
After a hidden business failure, duplicate empty creation notifications are
suppressed and the client-visible response identity and sequence remain stable.
Turn-state values are checked against their issuing account, including values
from handshake headers and streamed Codex metadata.

## Counting And Timing

- Same-account retries and account switches share the configured total budget.
- A zero same-account limit selects another account immediately.
- Only an actually started, explicitly owned retry increments the usage column
  and the retry panel. Connection retries and canceled pending retries count zero.
- Recovery latency starts with the first intercepted business error and stops at
  the first committed output or the decision to stop recovery. It includes retry
  delays, account admission and connection acquisition, but excludes subsequent
  answer generation. It is separate from the CPA first-frame metric.
- An HTTP 200 SSE stream is classified using its terminal event. Recovered
  failures remain upstream observations; only a final visible failure enters
  request errors.

## Deterministic Network Simulation

The handler tests use real loopback HTTP/SSE clients, the real Gin Responses
handler, real pooled WS connections and a local fake OpenAI server. Five fake
OAuth accounts are configured for each load run. Every request fails four times:
three same-account retries, then a switch to a second account that succeeds.
The original user input, instructions, model, tools and reasoning settings are
compared across attempts. Tests also assert response IDs, event sequences,
terminal status, usage repository records and retry-panel totals.

| Concurrency | Requests | Peak In Flight | Upstream Requests | Business Retries | Recovered | Elapsed |
| --- | --- | --- | --- | --- | --- | --- |
| 50 | 500 | 50 | 2,500 | 2,000 | 500 | 9.90 s |
| 200 | 500 | 200 | 2,500 | 2,000 | 500 | 5.73 s |

These are correctness simulations, not production throughput measurements.
The fake upstream uses a one-second local dial timeout, no WAN latency and no
real OAuth credentials. No paid OpenAI requests are used in these simulations.

Additional passing cases cover:

- Both designated messages and both WS error envelopes.
- First CPA notification received before the fake upstream is allowed to proceed.
- Retry-count exhaustion, capacity time exhaustion and no available account.
- Cancellation during retry delay, without incrementing the count.
- Zero same-account budget and successful account switching.
- No replay after text, tool, reasoning or unknown output.
- Failure after an earlier successful recovery attempt.
- Switch disabled and internal 502 handshake reconnects excluded from counting.
- Cross-account turn-state isolation, including the next client turn.
- A delayed answer tail excluded from recovery latency.
- The production ops capture/parser classifying successful recovery outside
  request errors, while still detecting final SSE failures.

## Reproduction

From `backend`:

```powershell
go test -tags=unit ./internal/handler ./internal/service -run 'TestOpenAIWSBusinessRetry|Test.*OpenAIUpstream5xx' -count=1 -v
go test -tags=unit ./internal/handler ./internal/service ./internal/repository ./internal/handler/dto -count=1 -timeout=15m
go build -tags=embed -o bin/server-ws-business-retry-ui.exe ./cmd/server
```

From `frontend`, before the embedded backend build:

```powershell
pnpm typecheck
pnpm test:run src/components/admin/usage/__tests__/UsageTable.spec.ts
pnpm build
```

The focused service/handler tests passed. The usage table's 24 tests, frontend
type checking, frontend production build and backend build passed. Full handler,
repository and DTO suites passed.

The complete service suite was not green. Failing tests were rerun on both the
working tree and an isolated checkout of baseline commit `1f51e67`: 25 failures
persisted in the working tree, all also present in the baseline. They concern
existing WS pool settings validation fixtures and Windows plugin archive rename
failures. Timing-sensitive content-moderation and pool queue assertions also
failed during the full run; the current-tree rerun passed both, while the
content-moderation timeout reproduced in the baseline. No new failure appeared
in this comparison.

The Go race detector was not run: this environment has CGO disabled and no C
compiler on PATH. A live Codex CLI session and OpenAI production fault injection
are not part of the above validation.

Migration `235_add_usage_log_openai_upstream_5xx_retry_count.sql` adds the usage
counter. Historical rows remain NULL; newly measured requests with no owned
business retry record zero. Retry-panel records remain an in-memory ring buffer.
