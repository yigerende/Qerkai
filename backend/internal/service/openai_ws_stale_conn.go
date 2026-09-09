package service

import (
	"errors"
	"strings"
)

// 池内陈旧连接的免费重试。
//
// 二次开发新增文件，上游不存在，不产生合并冲突。
//
// ## 现象
//
// 生产日志：
//
//	read_fail account_id=54752 conn_id=oa_ws_54752_44265 wrote_downstream=false
//	  close_status=1011(StatusInternalError)
//	  close_reason=keepalive ping timeout
//	  events=0 token_events=0 ...
//	reconnect_retry account_id=54752 retry=1 max_retries=5 reason=read_event backoff_ms=125
//
// ## 根因
//
// close_reason 是【上游发来的】：OpenAI ping 了这条连接，我们没有回 pong。
// coder/websocket 只在有 Read() 调用时处理控制帧（其
// SupportsIdlePingWithoutReader 显式返回 false，openai_ws_client.go 注释写明
// "control frames are only consumed by Read"），而连接池对空闲连接不设 reader，
// 于是 ping 无人应答，上游按自己的 keepalive 窗口关闭连接。
//
// 上游对此已有缓解：openAIWSConnIdleRecycleAfter=90s 会回收「不支持 idle ping
// 且空闲超过 90s」的连接，注释说明意图是「在上游 keepalive 窗口到期前回收」。
// 生产证据表明 90s 并不总是早于该窗口——连接仍会在池中静默死亡。
//
// ## 为什么不缩短回收阈值
//
// 这是最直观的修法，但与本项目实测的 403 结论直接冲突：Cloudflare 边缘按
// 【账号维度的建连速率】限流，实测受控对照中把 max_idle_per_account 从 12
// 提到 30（即让池多留热连接）把失败率从 16% 降到 5.3%。缩短空闲回收会减少
// 热连接、抬高建连率，正是那条「建连→驱逐→重建→超速 403」自我强化循环。
//
// 且两种路径都要付一次 dial：现在是「租用陈旧连接→失败→dial 新的」，
// 提前回收是「dial 新的」。差别只在那一次浪费的往返，而代价是全局建连率上升。
//
// ## 修法
//
// 不改回收阈值，而是让这次失败【不计入重试预算】——它根本不是上游故障，
// 是我们自己池里的连接过期了，那次「重试」才是真正的第一次尝试。
//
// 判据三者同时成立，缺一不可：
//   - 连接是复用的（lease.Reused()）——新 dial 后立刻失败是真实上游故障
//   - 一个事件都没收到（events==0）——收到过事件说明连接本来是活的
//   - 未向下游写出任何字节——写过就不能重放
//
// 每个请求只放行一次：一次新 dial 之后，要么成功，要么是真实故障，
// 继续免费重试会掩盖真问题并可能空转。

// openAIWSStaleConnReason 是陈旧池连接失败的专用 fallback reason。
//
// 取值刻意带 read_event 前缀：classifyOpenAIWSReconnectReason 用
// strings.TrimPrefix(reason,"prewarm_") 之后做精确匹配，未知 reason 会落到
// 兜底分支。用独立取值并在下方显式声明可重试，避免依赖上游 switch 的兜底行为。
const openAIWSStaleConnReason = "read_event_stale_pooled_conn"

// wrapOpenAIWSStaleConnFallback 构造陈旧连接的 fallback 错误。
func wrapOpenAIWSStaleConnFallback(err error) error {
	return wrapOpenAIWSFallback(openAIWSStaleConnReason, err)
}

// isOpenAIWSStaleConnError 报告该错误是否为池内陈旧连接失败。
func isOpenAIWSStaleConnError(err error) bool {
	if err == nil {
		return false
	}
	var fallbackErr *openAIWSFallbackError
	if !errors.As(err, &fallbackErr) || fallbackErr == nil {
		return false
	}
	return strings.TrimSpace(fallbackErr.Reason) == openAIWSStaleConnReason
}

// openAIWSReadFailureIsStalePooledConn 判断一次读失败是否应归因于陈旧池连接。
//
// reused 来自 lease.Reused()，eventCount 是本次转发已收到的上游事件数。
func openAIWSReadFailureIsStalePooledConn(reused bool, eventCount int, wroteDownstream bool) bool {
	return reused && eventCount == 0 && !wroteDownstream
}

// classifyOpenAIWSReconnectReasonWithStale 是 classifyOpenAIWSReconnectReason
// 的陈旧连接感知包装。
//
// 必要性：openAIWSStaleConnReason 是本项目新增取值，上游那个 switch 认不出来，
// 会落到 default 返回 retryable=false。免费重试用掉之后若直接交给上游判定，
// 第二次陈旧失败就会让请求立即失败——比没有本功能时（归为 read_event、
// 正常可重试）更差。
//
// 因此这里把用尽免费额度后的陈旧失败还原成上游的 read_event 语义：
// 走常规重试，计入 attempt 与预算。免费重试只是把「第一次」的记账免掉，
// 不改变之后的任何行为。
func classifyOpenAIWSReconnectReasonWithStale(err error) (string, bool) {
	if isOpenAIWSStaleConnError(err) {
		return "read_event", true
	}
	return classifyOpenAIWSReconnectReason(err)
}
