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
      intercepted: '已执行重试',
      succeeded: '重试成功',
      exhausted: '重试未恢复',
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
      intercepted: '已开始重试',
      succeeded: '重试成功',
      exhausted: '重试未恢复',
      skipped: '未重试'
    },
    eventHints: {
      intercepted: '指定上游错误触发本开关，下一次请求已开始；与使用日志计数一致',
      succeeded: '重试后上游返回成功，客户端未感知失败',
      exhausted: '重试后请求最终失败或取消，停止原因见备注',
      skipped: '命中 502/503 但本次未重试（原因见备注列）'
    },
    columns: {
      wsRetry: 'WS 内部重试',
      rule: '命中规则',
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
    attemptSwitch: '第 {attempt} 次 / 换账号',
    stopReasons: {
      ws_retry_count_exhausted: 'WS 内部重连次数用尽',
      ws_retry_time_exhausted: 'WS 内部重连时间预算用尽',
      ws_not_retryable: 'WS 错误不可内部重试',
      ws_error: 'WS 连接恢复停止',
      upstream_error: '其他上游错误',
      retry_count_exhausted: '累计重试次数用尽',
      retry_time_exhausted: '重试时间预算用尽',
      no_available_account: '无可用账号',
      client_disconnected: '客户端已断开',
      output_started: '已开始输出，停止重试'
    },
    wsRetry: { none: '未发生', retrying: '{n} 次，重连中', recovered: '{n} 次，已恢复', failed: '{n} 次，未恢复' },
    rules: {
      title: '业务重试匹配规则', restore: '恢复默认规则', add: '新增规则', remove: '删除规则', up: '上移', down: '下移',
      empty: '未启用任何匹配规则', name: '规则名称', status: '归类状态码', mode: '内容匹配', all: '全部关键词', any: '任意关键词',
      keywords: '内容关键词（每行一项）', payload: '上游错误事件 JSON', preview: '测试匹配', newName: '自定义错误规则', failed: '匹配测试失败',
      reasons: { invalid_json: 'JSON 格式无效', unsupported_event: '不属于 error / response.failed 事件', excluded_error: '该错误由原有逻辑处理', excluded_status: '明确状态码不属于 502/503', no_match: '未命中已启用规则' }
    },
    attemptTerminal: '共重试 {n} 次',
    empty: '暂无重试观测记录',
    emptyFiltered: '没有符合筛选条件的记录',
    accountFallback: '账号 #{id}',
    idLabel: 'ID {id}',
    pagination: '第 {from}-{to} 条，共 {total} 条'
  }
}
