export default {
  openaiUpstream5xxRetry: {
    title: 'OpenAI 502/503 重试观测',
    description: '只读内存记录，进程重启即清空，不落库',
    refresh: '刷新',
    clear: '清空记录',
    clearConfirm: '确认清空全部重试观测记录？只影响观测数据，不改变重试行为。',
    cleared: '已清空重试观测记录',
    clearFailed: '清空失败',
    loadFailed: '重试观测记录加载失败',
    updatedAt: '更新于 {time}',
    autoRefresh: '自动刷新',
    disabledHint: '「OpenAI 上游 502/503 自动重试」当前关闭，不会产生新记录。已有记录仍可查看。',
    configLabel: '当前生效配置',
    config: {
      sameAccount: '同账号重试 {n} 次',
      total: '整请求累计上限 {n} 次',
      delay: '重试间隔 {n} ms'
    },
    stats: {
      total: '记录总数',
      intercepted: '已拦截',
      succeeded: '重试成功',
      exhausted: '预算耗尽',
      skipped: '未重试',
      avgExtraLatency: '平均重试额外耗时',
      maxExtraLatency: '最大重试额外耗时',
      capacity: '缓冲容量'
    },
    filters: {
      event: '事件类型',
      statusCode: '上游状态码',
      accountId: '账号 ID',
      all: '全部',
      accountIdPlaceholder: '输入账号 ID',
      reset: '重置'
    },
    events: {
      intercepted: '已拦截待重试',
      succeeded: '重试成功',
      exhausted: '预算耗尽',
      skipped: '未重试'
    },
    eventHints: {
      intercepted: '上游返回 502/503，已拦下并准备原账号重试',
      succeeded: '重试后上游返回成功，客户端未感知失败',
      exhausted: '重试预算用尽，最终把错误返回给了客户端',
      skipped: '命中 502/503 但本次未重试（原因见备注列）'
    },
    columns: {
      time: '时间',
      event: '事件',
      upstreamStatus: '上游',
      clientStatus: '返回客户端',
      account: '账号',
      model: '模型',
      transport: '协议',
      attempt: '重试进度',
      delay: '间隔',
      extraLatency: '重试额外耗时',
      requestId: '请求 ID',
      message: '备注'
    },
    transports: {
      http_sse: 'HTTP/SSE',
      responses_websockets_v2: 'WebSocket',
      unknown: '未知'
    },
    attemptValue: '第 {attempt} 次 / 同账号 {same}/{max}',
    attemptTerminal: '共重试 {n} 次',
    empty: '暂无重试观测记录',
    emptyFiltered: '没有符合筛选条件的记录',
    accountFallback: '账号 #{id}',
    idLabel: 'ID {id}',
    pagination: '第 {from}-{to} 条，共 {total} 条'
  }
}
