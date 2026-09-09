package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// OpenAI WS 建连限速与 403 退避重试。
//
// 二次开发新增文件，上游不存在，不产生合并冲突。
//
// 背景（实测结论）：上游对 WS 握手有账号维度的速率限制，命中后 Cloudflare
// 边缘直接返回 403（dial_resp_server=cloudflare、无 x-request-id，请求未达
// OpenAI）。实测同一账号：
//   - 瞬间并发建 30 条：第 2 轮起整批 403
//   - 同时最多 3 条、间隔 150ms：撑到第 3 轮
//   - 同时最多 1 条、间隔 400ms：120 次握手仅 1 次失败
//   - 27 条连接同时保持：完全正常
// 即限制的是「建连速率」而非「连接数」，且作用域是账号——同一 IP 下账号 A
// 被限时，账号 B 握手照常成功。
//
// 上游连接池在判断需要新连接后会释放 ap.mu 再 dial（避免持锁做网络 IO），
// 于是并发请求各自发起握手。预热路径 prewarmConns 本身是串行的，不受影响；
// 缺口只在 acquire 的新建分支。
//
// 本文件提供两件事：
//  1. 每账号建连信号量：同时在途握手数受限，其余请求等待后重新尝试复用
//  2. 403 退避重试：命中速率限制时短暂等待再试，而不是直接失败

const (
	// openAIWSDialConcurrencyPerAccount 单账号同时在途握手数。
	// 取 2 是实测折中：1 最稳但爬坡慢（30 条约 16s），3 在高频轮次下仍会被限。
	openAIWSDialConcurrencyPerAccount = 2

	// openAIWSDialMinInterval 同账号两次握手之间的最小间隔。
	// 实测 400ms 下 120 次握手仅 1 次失败。
	openAIWSDialMinInterval = 400 * time.Millisecond

	// openAIWSDialRetryOn403Max 403 后的最大重试次数。
	openAIWSDialRetryOn403Max = 2

	// openAIWSDialRetryBaseDelay 403 重试的基础退避时长（按次数线性放大）。
	openAIWSDialRetryBaseDelay = 900 * time.Millisecond

	// openAIWSDialLimiterIdleTTL 限速器空闲多久后可被回收，避免账号维度无限增长。
	openAIWSDialLimiterIdleTTL = 10 * time.Minute

	// openAIWSDialWaitReserve 是留给握手本身的时间余量。
	// acquire 的总预算为 dial_timeout+2s（默认 12s），限速等待必须给 dial
	// 留出足够窗口，否则「等到了但来不及握手」比不限速更糟。
	openAIWSDialWaitReserve = 6 * time.Second
)

// openAIWSAccountDialLimiter 单账号的建连闸门。
type openAIWSAccountDialLimiter struct {
	sem chan struct{}

	mu       sync.Mutex
	lastDial time.Time

	lastUsedUnixNano atomic.Int64
}

func newOpenAIWSAccountDialLimiter() *openAIWSAccountDialLimiter {
	l := &openAIWSAccountDialLimiter{sem: make(chan struct{}, openAIWSDialConcurrencyPerAccount)}
	l.lastUsedUnixNano.Store(time.Now().UnixNano())
	return l
}

// acquire 取得握手许可；返回的 release 必须被调用。
//
// 除并发闸门外还强制最小间隔：即便并发数没打满，同账号的连续握手也会被
// 拉开到 openAIWSDialMinInterval，避免「串行但极快」同样触发速率限制。
func (l *openAIWSAccountDialLimiter) acquire(ctx context.Context) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	l.lastUsedUnixNano.Store(time.Now().UnixNano())
	// 先显式检查一次：信号量有空位时 select 的两个 case 同时就绪，
	// Go 会随机挑一个，已取消的 ctx 可能被忽略。
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case l.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	wait := l.reserveInterval()
	if wait > 0 {
		// 限速不能把请求拖到超时：acquire 的总预算只有 dial_timeout+2s。
		// 剩余时间不足以既等待又完成握手时，放弃本次间隔约束直接放行——
		// 宁可偶尔超速被 403（有退避重试兜底），也不能因为等待而必然失败。
		if deadline, ok := ctx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining <= openAIWSDialWaitReserve {
				wait = 0
			} else if budget := remaining - openAIWSDialWaitReserve; wait > budget {
				wait = budget
			}
		}
	}
	if wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			<-l.sem
			return nil, ctx.Err()
		}
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			<-l.sem
			l.lastUsedUnixNano.Store(time.Now().UnixNano())
		})
	}, nil
}

// reserveInterval 记录本次握手时刻并返回需要等待的时长。
func (l *openAIWSAccountDialLimiter) reserveInterval() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	earliest := l.lastDial.Add(openAIWSDialMinInterval)
	if l.lastDial.IsZero() || !earliest.After(now) {
		l.lastDial = now
		return 0
	}
	wait := earliest.Sub(now)
	// 预占该时隙，避免多个等待者被同时放行。
	l.lastDial = earliest
	return wait
}

func (l *openAIWSAccountDialLimiter) idleFor(now time.Time) time.Duration {
	if l == nil {
		return 0
	}
	return now.Sub(time.Unix(0, l.lastUsedUnixNano.Load()))
}

// openAIWSDialLimiterRegistry 按账号维护限速器。
type openAIWSDialLimiterRegistry struct {
	limiters sync.Map // accountID -> *openAIWSAccountDialLimiter
	lastGC   atomic.Int64
}

var globalOpenAIWSDialLimiters = &openAIWSDialLimiterRegistry{}

func (r *openAIWSDialLimiterRegistry) get(accountID int64) *openAIWSAccountDialLimiter {
	if r == nil || accountID <= 0 {
		return nil
	}
	if raw, ok := r.limiters.Load(accountID); ok {
		if limiter, ok := raw.(*openAIWSAccountDialLimiter); ok {
			return limiter
		}
	}
	created := newOpenAIWSAccountDialLimiter()
	raw, loaded := r.limiters.LoadOrStore(accountID, created)
	limiter, _ := raw.(*openAIWSAccountDialLimiter)
	if !loaded {
		r.maybeGC()
	}
	return limiter
}

// maybeGC 回收长期空闲的账号限速器；每分钟至多一次，成本可忽略。
func (r *openAIWSDialLimiterRegistry) maybeGC() {
	now := time.Now()
	last := r.lastGC.Load()
	if now.UnixNano()-last < int64(time.Minute) {
		return
	}
	if !r.lastGC.CompareAndSwap(last, now.UnixNano()) {
		return
	}
	r.limiters.Range(func(key, value any) bool {
		limiter, ok := value.(*openAIWSAccountDialLimiter)
		if !ok || limiter == nil {
			r.limiters.Delete(key)
			return true
		}
		if limiter.idleFor(now) > openAIWSDialLimiterIdleTTL && len(limiter.sem) == 0 {
			r.limiters.Delete(key)
		}
		return true
	})
}

// acquireOpenAIWSDialSlot 是连接池调用的入口。
// accountID 无效时返回 no-op，行为与未启用限速一致。
func acquireOpenAIWSDialSlot(ctx context.Context, accountID int64) (func(), error) {
	limiter := globalOpenAIWSDialLimiters.get(accountID)
	if limiter == nil {
		return func() {}, nil
	}
	return limiter.acquire(ctx)
}

// isOpenAIWSDialRateLimited 判断握手错误是否为上游速率限制（403）。
//
// 只认 403：401 是真正的鉴权失败，重试无意义且会放大问题。
func isOpenAIWSDialRateLimited(err error) bool {
	if err == nil {
		return false
	}
	var dialErr *openAIWSDialError
	if !errors.As(err, &dialErr) || dialErr == nil {
		return false
	}
	return dialErr.StatusCode == 403
}

// openAIWSDialRetryDelay 返回第 attempt 次重试前应等待的时长（attempt 从 1 起）。
func openAIWSDialRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	return time.Duration(attempt) * openAIWSDialRetryBaseDelay
}

// dialOpenAIWSWithRateLimit 在限速闸门内执行 dial，并对 403 做有限退避重试。
//
// dialFn 由调用方提供（连接池的 p.dialConn），本函数不感知连接对象类型，
// 避免与上游池实现耦合。
func dialOpenAIWSWithRateLimit[T any](
	ctx context.Context,
	accountID int64,
	dialFn func(context.Context) (T, error),
) (T, error) {
	var zero T
	var lastErr error
	for attempt := 0; attempt <= openAIWSDialRetryOn403Max; attempt++ {
		if attempt > 0 {
			delay := openAIWSDialRetryDelay(attempt)
			// 剩余时间不足以「退避 + 再握手」时直接返回上次错误，
			// 避免把必然失败的重试计入预算、拖垮整个 acquire。
			if deadline, ok := ctx.Deadline(); ok {
				if time.Until(deadline) <= delay+openAIWSDialWaitReserve {
					return zero, lastErr
				}
			}
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return zero, lastErr
			}
		}
		release, err := acquireOpenAIWSDialSlot(ctx, accountID)
		if err != nil {
			if lastErr != nil {
				return zero, lastErr
			}
			return zero, err
		}
		result, dialErr := dialFn(ctx)
		release()
		if dialErr == nil {
			return result, nil
		}
		lastErr = dialErr
		if !isOpenAIWSDialRateLimited(dialErr) {
			return zero, dialErr
		}
		logOpenAIWSModeInfo(
			"dial_rate_limited account_id=%d attempt=%d max=%d next_delay_ms=%d",
			accountID,
			attempt+1,
			openAIWSDialRetryOn403Max+1,
			openAIWSDialRetryDelay(attempt+1).Milliseconds(),
		)
	}
	return zero, lastErr
}
