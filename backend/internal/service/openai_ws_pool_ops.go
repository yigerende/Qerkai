package service

import (
	"sort"
	"sync/atomic"
	"time"
)

type OpenAIWSConnectionOpsSnapshot struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Session     string `json:"session"`
	State       string `json:"state"`
	CreatedAt   int64  `json:"created_at"`
	LastUsedAt  int64  `json:"last_used_at"`
	AgeSeconds  int64  `json:"age_seconds"`
	IdleSeconds int64  `json:"idle_seconds"`
	UnbindAt    int64  `json:"unbind_at"`
	UnbindIn    int64  `json:"unbind_in_seconds"`
	Waiters     int32  `json:"waiters"`
	Prewarmed   bool   `json:"prewarmed"`
}

type OpenAIWSAccountPoolOpsSnapshot struct {
	AccountID      int64                           `json:"account_id"`
	AccountName    string                          `json:"account_name"`
	Total          int                             `json:"total"`
	InUse          int                             `json:"in_use"`
	Waiters        int                             `json:"waiters"`
	Creating       int                             `json:"creating"`
	SessionPrimary int                             `json:"session_primary"`
	SessionStandby int                             `json:"session_standby"`
	Prewarm        int                             `json:"prewarm"`
	Standby        int                             `json:"standby"`
	Legacy         int                             `json:"legacy"`
	Status         string                          `json:"status"`
	Connections    []OpenAIWSConnectionOpsSnapshot `json:"connections"`
}

type OpenAIWSPoolOpsSnapshot struct {
	GeneratedAt         int64                            `json:"generated_at"`
	ForceWSEnabled      bool                             `json:"force_ws_enabled"`
	OptimizationEnabled bool                             `json:"optimization_enabled"`
	Settings            OpenAIWSPoolOptimizationSettings `json:"settings"`
	Metrics             OpenAIWSPoolMetricsSnapshot      `json:"metrics"`
	TotalConnections    int                              `json:"total_connections"`
	TotalInUse          int                              `json:"total_in_use"`
	TotalWaiters        int                              `json:"total_waiters"`
	TotalCreating       int                              `json:"total_creating"`
	TotalPrewarm        int                              `json:"total_prewarm"`
	TotalStandby        int                              `json:"total_standby"`
	TotalSession        int                              `json:"total_session"`
	Accounts            []OpenAIWSAccountPoolOpsSnapshot `json:"accounts"`
}

var activeOpenAIWSPool atomic.Pointer[openAIWSConnPool]

func registerActiveOpenAIWSPool(pool *openAIWSConnPool) {
	if pool != nil {
		activeOpenAIWSPool.Store(pool)
	}
}

// RemoveOpenAIWSAccountPool removes a deleted account from the process-local
// pool and the read-only operations snapshot.
func RemoveOpenAIWSAccountPool(accountID int64) {
	if pool := activeOpenAIWSPool.Load(); pool != nil {
		pool.RemoveAccount(accountID)
	}
}

func GetOpenAIWSPoolOpsSnapshot() OpenAIWSPoolOpsSnapshot {
	settings := currentOpenAIWSPoolOptimizationSettings()
	out := OpenAIWSPoolOpsSnapshot{GeneratedAt: time.Now().Unix(), ForceWSEnabled: ForceUpstreamWSEnabled(), OptimizationEnabled: ForceUpstreamWSEnabled() && settings.Enabled, Settings: settings, Accounts: []OpenAIWSAccountPoolOpsSnapshot{}}
	pool := activeOpenAIWSPool.Load()
	if pool == nil {
		return out
	}
	out.Metrics = pool.SnapshotMetrics()
	now := time.Now()
	pool.accounts.Range(func(key, value any) bool {
		accountID, _ := key.(int64)
		if pool.isAccountRemoved(accountID) {
			pool.accounts.Delete(accountID)
			return true
		}
		ap, _ := value.(*openAIWSAccountPool)
		if ap == nil {
			return true
		}
		ap.mu.Lock()
		if out.OptimizationEnabled {
			pool.releaseExpiredOwnersLocked(ap, now)
		}
		row := OpenAIWSAccountPoolOpsSnapshot{AccountID: accountID, AccountName: ap.accountName, Creating: ap.creating, Status: "healthy", Connections: make([]OpenAIWSConnectionOpsSnapshot, 0, len(ap.conns))}
		if ap.prewarmActive {
			row.Status = "replenishing"
		}
		if !ap.prewarmUntil.IsZero() && now.Before(ap.prewarmUntil) {
			row.Status = "cooldown"
		}
		for _, conn := range ap.conns {
			if conn == nil {
				continue
			}
			state := "idle"
			if conn.isLeased() {
				state = "in_use"
				row.InUse++
			}
			waiters := conn.waiters.Load()
			row.Waiters += int(waiters)
			switch conn.poolRole {
			case openAIWSConnRolePrewarm:
				row.Prewarm++
			case openAIWSConnRoleStandby:
				row.Standby++
			case openAIWSConnRoleSessionPrimary:
				row.SessionPrimary++
			case openAIWSConnRoleSessionStandby:
				row.SessionStandby++
			default:
				row.Legacy++
			}
			session := conn.ownerSession
			if len(session) > 12 {
				session = session[:12]
			}
			unbindAt, unbindIn := int64(0), int64(0)
			if deadline := pool.sessionOwnerUnbindDeadline(conn, now); !deadline.IsZero() {
				unbindAt = deadline.Unix()
				unbindIn = int64(deadline.Sub(now).Seconds())
				if unbindIn < 0 {
					unbindIn = 0
				}
			}
			row.Connections = append(row.Connections, OpenAIWSConnectionOpsSnapshot{ID: conn.id, Role: conn.poolRole, Session: session, State: state, CreatedAt: conn.createdAt().Unix(), LastUsedAt: conn.lastUsedAt().Unix(), AgeSeconds: int64(conn.age(now).Seconds()), IdleSeconds: int64(conn.idleDuration(now).Seconds()), UnbindAt: unbindAt, UnbindIn: unbindIn, Waiters: waiters, Prewarmed: conn.isPrewarmed()})
		}
		row.Total = len(row.Connections)
		ap.mu.Unlock()
		sort.Slice(row.Connections, func(i, j int) bool { return row.Connections[i].CreatedAt > row.Connections[j].CreatedAt })
		out.TotalConnections += row.Total
		out.TotalInUse += row.InUse
		out.TotalWaiters += row.Waiters
		out.TotalCreating += row.Creating
		out.TotalPrewarm += row.Prewarm
		out.TotalStandby += row.Standby
		out.TotalSession += row.SessionPrimary + row.SessionStandby
		out.Accounts = append(out.Accounts, row)
		return true
	})
	sort.Slice(out.Accounts, func(i, j int) bool { return out.Accounts[i].AccountID < out.Accounts[j].AccountID })
	return out
}
