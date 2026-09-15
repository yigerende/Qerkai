export default {
  openaiUpstream5xxRetry: {
    title: 'OpenAI 502/503 Retry Log',
    description: 'Read-only in-memory records, cleared on process restart, never persisted',
    refresh: 'Refresh',
    clear: 'Clear log',
    clearConfirm: 'Clear all retry records? This only affects observability data, not retry behavior.',
    cleared: 'Retry log cleared',
    clearFailed: 'Failed to clear',
    loadFailed: 'Failed to load retry log',
    updatedAt: 'Updated {time}',
    autoRefresh: 'Auto refresh',
    disabledHint: 'OpenAI upstream 502/503 auto retry is currently off, so no new records will appear. Existing records remain viewable.',
    configLabel: 'Active configuration',
    config: {
      sameAccount: '{n} same-account retries',
      total: '{n} retries per request max',
      delay: '{n} ms retry delay'
    },
    stats: {
      total: 'Records',
      intercepted: 'Intercepted',
      succeeded: 'Retry succeeded',
      exhausted: 'Budget exhausted',
      skipped: 'Not retried',
      avgExtraLatency: 'Avg retry extra latency',
      maxExtraLatency: 'Max retry extra latency',
      capacity: 'Buffer capacity'
    },
    filters: {
      event: 'Event',
      statusCode: 'Upstream status',
      accountId: 'Account ID',
      all: 'All',
      accountIdPlaceholder: 'Enter account ID',
      reset: 'Reset'
    },
    events: {
      intercepted: 'Intercepted',
      succeeded: 'Retry succeeded',
      exhausted: 'Budget exhausted',
      skipped: 'Not retried'
    },
    eventHints: {
      intercepted: 'Upstream returned 502/503; intercepted and about to retry on the same account',
      succeeded: 'Upstream succeeded after retry; the client never saw a failure',
      exhausted: 'Retry budget ran out and the error was returned to the client',
      skipped: 'Matched 502/503 but was not retried (see notes column)'
    },
    columns: {
      time: 'Time',
      event: 'Event',
      upstreamStatus: 'Upstream',
      clientStatus: 'To client',
      account: 'Account',
      model: 'Model',
      transport: 'Transport',
      attempt: 'Progress',
      delay: 'Delay',
      extraLatency: 'Retry extra latency',
      requestId: 'Request ID',
      message: 'Notes'
    },
    transports: {
      http_sse: 'HTTP/SSE',
      responses_websockets_v2: 'WebSocket',
      unknown: 'Unknown'
    },
    attemptValue: 'attempt {attempt} / same-account {same}/{max}',
    attemptTerminal: '{n} retries total',
    empty: 'No retry records yet',
    emptyFiltered: 'No records match the current filters',
    accountFallback: 'Account #{id}',
    idLabel: 'ID {id}',
    pagination: '{from}-{to} of {total}'
  }
}
