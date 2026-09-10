export default {
  openaiWSPool: {
    title: 'OpenAI WS 池运维',
    description: '只读内存快照，不探测上游，不写入运行状态',
    refresh: '刷新',
    updatedAt: '更新于 {time}',
    loadFailed: '连接池快照加载失败',
    forceDisabled: '“强制 OpenAI 上游 WebSocket”当前关闭，连接池优化不会运行。',
    optimizationDisabled: '当前使用原强制 WS 调度；启用“WS 连接池优化调度”后才使用预热/备用分层。',
    modes: { forceDisabled: '强制 WS 已关闭', optimized: '优化调度运行中', legacy: '原强制 WS 调度' },
    summary: {
      totalConnections: '总连接', inUse: '使用中', sessionOwned: '会话专属', prewarm: '公共预热', standby: '公共备用', reuseRate: '连接复用率', averageQueue: '平均排队'
    },
    handshake: { title: '握手运行指标', total: '建连尝试', success: '成功', failure: '失败', forbidden: '403', rateLimited: '429', average: '平均握手' },
    accounts: { title: '账号连接池', activeCount: '{count} 个活跃账号池', empty: '当前没有已创建的 OpenAI WS 连接池' },
    columns: { account: '账号', status: '状态', connections: '连接', inUse: '使用中', session: '会话主/备用', inventory: '预热/备用', creating: '建连中', queue: '排队', connectionId: '连接 ID', role: '角色', sessionId: '会话', age: '存活', idle: '空闲', unbindIn: '解绑倒计时', waiters: '等待' },
    connectionState: { inUse: '使用中', idle: '空闲' },
    status: { healthy: '正常', replenishing: '补池中', cooldown: '冷却中' },
    roles: { prewarm: '公共预热', standby: '公共备用', sessionPrimary: '会话主连接', sessionStandby: '会话备用', legacy: '原调度连接' },
    accountFallback: '账号 #{id}',
    idLabel: 'ID {id}',
    noConnections: '暂无连接'
  }
}
