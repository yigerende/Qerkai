package service

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// 生产实测形态：复用的池连接，events=0，未写下游，
// close_reason=keepalive ping timeout。
func TestStaleConnDetectionMatchesProductionShape(t *testing.T) {
	require.True(t, openAIWSReadFailureIsStalePooledConn(true, 0, false))
}

// 三个判据缺一不可。
func TestStaleConnDetectionRequiresAllConditions(t *testing.T) {
	// 新 dial 后立刻失败是真实上游故障，不能算陈旧连接。
	require.False(t, openAIWSReadFailureIsStalePooledConn(false, 0, false),
		"非复用连接不应判为陈旧")
	// 收到过事件说明连接本来是活的，中途断开是真实故障。
	require.False(t, openAIWSReadFailureIsStalePooledConn(true, 1, false),
		"收到过事件不应判为陈旧")
	// 写过下游就不能重放。
	require.False(t, openAIWSReadFailureIsStalePooledConn(true, 0, true),
		"已写下游不应判为陈旧")
}

func TestStaleConnErrorRoundTrip(t *testing.T) {
	base := errors.New(`failed to get reader: received close frame: status = StatusInternalError and reason = "keepalive ping timeout"`)
	wrapped := wrapOpenAIWSStaleConnFallback(base)

	require.True(t, isOpenAIWSStaleConnError(wrapped))
	require.ErrorIs(t, wrapped, base, "必须保留原始错误链")
}

// 其它 fallback 原因不得被误判——否则会获得不该有的免费重试。
func TestStaleConnErrorRejectsOtherReasons(t *testing.T) {
	for _, reason := range []string{
		"read_event", "write_request", "auth_failed",
		"message_too_big", "policy_violation", "dial_failed",
	} {
		err := wrapOpenAIWSFallback(reason, errors.New("boom"))
		require.False(t, isOpenAIWSStaleConnError(err), "reason=%s 不应判为陈旧连接", reason)
	}
}

func TestStaleConnErrorRejectsNonFallbackErrors(t *testing.T) {
	require.False(t, isOpenAIWSStaleConnError(nil))
	require.False(t, isOpenAIWSStaleConnError(errors.New("plain")))
	require.False(t, isOpenAIWSStaleConnError(fmt.Errorf("wrapped: %w", errors.New("plain"))))
}

// 陈旧连接的 reason 必须与 read_event 区分开，否则会走进上游那条
// 「计入预算」的常规重试分支。
func TestStaleConnReasonDistinctFromReadEvent(t *testing.T) {
	require.NotEqual(t, "read_event", openAIWSStaleConnReason)
}

// 关键回归：免费重试用尽后，陈旧失败必须还原成 read_event 的可重试语义。
//
// openAIWSStaleConnReason 是本项目新增取值，上游 switch 认不出来会落到
// default 返回 retryable=false。若直接交给上游判定，第二次陈旧失败会让请求
// 立即失败——比没有本功能时（归为 read_event、正常可重试）更差。
func TestStaleConnClassificationFallsBackToRetryableReadEvent(t *testing.T) {
	staleErr := wrapOpenAIWSStaleConnFallback(errors.New("keepalive ping timeout"))

	// 上游原判定：认不出，不可重试。
	_, upstreamRetryable := classifyOpenAIWSReconnectReason(staleErr)
	require.False(t, upstreamRetryable, "前提：上游确实认不出该 reason")

	// 包装后判定：还原成 read_event 且可重试。
	reason, retryable := classifyOpenAIWSReconnectReasonWithStale(staleErr)
	require.Equal(t, "read_event", reason)
	require.True(t, retryable, "免费额度用尽后必须仍可正常重试")
}

// 包装函数对非陈旧错误必须与上游判定完全一致。
func TestStaleConnClassificationDelegatesOtherReasons(t *testing.T) {
	for _, reason := range []string{
		"read_event", "write_request", "auth_failed", "message_too_big",
		"policy_violation", "dial_failed", "previous_response_not_found",
		"acquire_timeout", "upstream_5xx",
	} {
		err := wrapOpenAIWSFallback(reason, errors.New("boom"))
		wantReason, wantRetryable := classifyOpenAIWSReconnectReason(err)
		gotReason, gotRetryable := classifyOpenAIWSReconnectReasonWithStale(err)
		require.Equal(t, wantReason, gotReason, "reason=%s", reason)
		require.Equal(t, wantRetryable, gotRetryable, "reason=%s", reason)
	}
	// nil 与非 fallback 错误同样保持一致。
	for _, err := range []error{nil, errors.New("plain")} {
		wantReason, wantRetryable := classifyOpenAIWSReconnectReason(err)
		gotReason, gotRetryable := classifyOpenAIWSReconnectReasonWithStale(err)
		require.Equal(t, wantReason, gotReason)
		require.Equal(t, wantRetryable, gotRetryable)
	}
}

// wsLastFailureReason 会被传回 forwardOpenAIWSV2 并进入
// shouldForceNewConnOnStoreDisabled。新增取值必须与它原本会取到的
// read_event 落在同一分支，否则会静默改变连接复用策略。
func TestStaleConnReasonKeepsSameForceNewConnDecision(t *testing.T) {
	for _, mode := range []string{
		openAIWSStoreDisabledConnModeOff,
		openAIWSStoreDisabledConnModeAdaptive,
		"strict",
	} {
		want := shouldForceNewConnOnStoreDisabled(mode, "read_event")
		got := shouldForceNewConnOnStoreDisabled(mode, openAIWSStaleConnReason)
		require.Equal(t, want, got, "mode=%s 下新增 reason 不得改变连接复用决策", mode)
	}
}
