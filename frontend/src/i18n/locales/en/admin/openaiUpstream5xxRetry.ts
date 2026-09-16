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
      intercepted: 'Retries started',
      succeeded: 'Retry succeeded',
      exhausted: 'Retry failed',
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
      intercepted: 'Retry started',
      succeeded: 'Retry succeeded',
      exhausted: 'Retry failed',
      skipped: 'Not retried'
    },
    eventHints: {
      intercepted: 'A designated upstream error triggered this switch and the next request started; counted in usage logs',
      succeeded: 'Upstream succeeded after retry; the client never saw a failure',
      exhausted: 'The request failed or was canceled after retries; see notes for the stop reason',
      skipped: 'Matched 502/503 but was not retried (see notes column)'
    },
    columns: {
      wsRetry: 'WS internal retries',
      rule: 'Matched rule',
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
    attemptSwitch: 'attempt {attempt} / switch account',
    stopReasons: {
      ws_retry_count_exhausted: 'WS reconnect limit reached',
      ws_retry_time_exhausted: 'WS reconnect time budget exhausted',
      ws_not_retryable: 'WS error is not reconnectable',
      ws_error: 'WS recovery stopped',
      upstream_error: 'Other upstream error',
      retry_count_exhausted: 'Total retry limit reached',
      retry_time_exhausted: 'Retry time budget exhausted',
      no_available_account: 'No available account',
      client_disconnected: 'Client disconnected',
      output_started: 'Output started; retries stopped'
    },
    wsRetry: { none: 'None', retrying: '{n} retries, reconnecting', recovered: '{n} retries, recovered', failed: '{n} retries, not recovered' },
    rules: {
      title: 'Business retry matching rules', restore: 'Restore default rules', add: 'Add rule', remove: 'Delete rule', up: 'Move up', down: 'Move down',
      empty: 'No matching rules enabled', name: 'Rule name', status: 'Classified status', mode: 'Content matching', all: 'All keywords', any: 'Any keyword',
      keywords: 'Content keywords (one per line)', payload: 'Upstream error event JSON', preview: 'Test match', newName: 'Custom error rule', failed: 'Match preview failed',
      reasons: { invalid_json: 'Invalid JSON', unsupported_event: 'Not an error / response.failed event', excluded_error: 'Handled by existing error policy', excluded_status: 'Explicit status is not 502/503', no_match: 'No enabled rule matched' }
    },
    attemptTerminal: '{n} retries total',
    empty: 'No retry records yet',
    emptyFiltered: 'No records match the current filters',
    accountFallback: 'Account #{id}',
    idLabel: 'ID {id}',
    pagination: '{from}-{to} of {total}'
  }
}
