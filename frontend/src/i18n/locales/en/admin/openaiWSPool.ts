export default {
  openaiWSPool: {
    title: 'OpenAI WS Pool Operations',
    description: 'Read-only in-memory snapshot with no upstream probes or state changes',
    refresh: 'Refresh',
    updatedAt: 'Updated at {time}',
    loadFailed: 'Failed to load the connection pool snapshot',
    forceDisabled: 'Force OpenAI upstream WebSocket is disabled, so pool optimization is inactive.',
    optimizationDisabled: 'The original forced WS scheduler is active. Enable WS pool optimization to use prewarm and standby tiers.',
    modes: { forceDisabled: 'Forced WS disabled', optimized: 'Optimized scheduler active', legacy: 'Original forced WS scheduler' },
    summary: {
      totalConnections: 'Connections', inUse: 'In use', sessionOwned: 'Session owned', prewarm: 'Public prewarm', standby: 'Public standby', reuseRate: 'Reuse rate', averageQueue: 'Average queue'
    },
    handshake: { title: 'Handshake Metrics', total: 'Attempts', success: 'Succeeded', failure: 'Failed', forbidden: '403', rateLimited: '429', average: 'Average handshake' },
    accounts: { title: 'Account Pools', activeCount: '{count} active account pools', empty: 'No OpenAI WS connection pools have been created' },
    columns: { account: 'Account', status: 'Status', connections: 'Connections', inUse: 'In use', session: 'Primary / backup', inventory: 'Prewarm / standby', creating: 'Connecting', queue: 'Queued', connectionId: 'Connection ID', role: 'Role', sessionId: 'Session', age: 'Age', idle: 'Idle', unbindIn: 'Unbind in', waiters: 'Waiters' },
    connectionState: { inUse: 'In use', idle: 'Idle' },
    status: { healthy: 'Healthy', replenishing: 'Replenishing', cooldown: 'Cooldown' },
    roles: { prewarm: 'Public prewarm', standby: 'Public standby', sessionPrimary: 'Session primary', sessionStandby: 'Session backup', legacy: 'Original scheduler' },
    accountFallback: 'Account #{id}',
    idLabel: 'ID {id}',
    noConnections: 'No connections'
  }
}
