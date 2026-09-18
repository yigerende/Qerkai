package service

// The experimental collector intentionally does NOT reuse x-codex-turn-state.
// That token is turn-scoped. Only an actual 292 + current_turn_state response
// qualifies for the opt-in mechanism described by the operator's article.
import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

const openAIStateKeeperSettingKey = "openai_state_keeper_v1"
const openAICollectedStateHeader = "current_turn_state"
const openAIStateProbeContextKey = "openai_state_keeper_probe"
const openAIStateKeeperWorkers = 50

type OpenAIStateKeeperSettings struct {
	Enabled                    bool    `json:"enabled"`
	InjectionEnabled           bool    `json:"injection_enabled"`
	AutoRefresh                bool    `json:"auto_refresh"`
	AutoCollectIntervalSeconds int     `json:"auto_collect_interval_seconds"`
	AccountIDs                 []int64 `json:"account_ids"`
	GroupIDs                   []int64 `json:"group_ids"`
	AllGroups                  bool    `json:"all_groups"`
	ProxyID                    int64   `json:"proxy_id"`
	Model                      string  `json:"model"`
	TTLSeconds                 int     `json:"ttl_seconds"`
	RefreshBeforeSeconds       int     `json:"refresh_before_seconds"`
	RetrySeconds               int     `json:"retry_seconds"`
	Revision                   string  `json:"revision"`
}

func DefaultOpenAIStateKeeperSettings() OpenAIStateKeeperSettings {
	return OpenAIStateKeeperSettings{AutoRefresh: true, AccountIDs: []int64{}, GroupIDs: []int64{}, Model: "gpt-6-astra", TTLSeconds: 3600, RefreshBeforeSeconds: 300, RetrySeconds: 60}
}

func (q OpenAIStateKeeperSettings) Validate() error {
	if len(q.AccountIDs) > 500 || len(q.GroupIDs) > 500 {
		return errors.New("最多选择 500 个账号或分组")
	}
	for _, ids := range [][]int64{q.AccountIDs, q.GroupIDs} {
		seen := map[int64]bool{}
		for _, id := range ids {
			if id <= 0 || seen[id] {
				return errors.New("账号和分组 ID 必须为不重复的正整数")
			}
			seen[id] = true
		}
	}
	if len(q.Model) > 200 || strings.TrimSpace(q.Model) == "" {
		return errors.New("请填写采集模型")
	}
	if q.AutoCollectIntervalSeconds < 0 || q.AutoCollectIntervalSeconds > 86400 {
		return errors.New("自动采集间隔须为 0–86400 秒，0 表示沿用原有自动续期")
	}
	if q.TTLSeconds < 300 || q.TTLSeconds > 86400 || q.RefreshBeforeSeconds < 30 || q.RefreshBeforeSeconds >= q.TTLSeconds || q.RetrySeconds < 30 || q.RetrySeconds > 3600 {
		return errors.New("缓存时间须为 300–86400 秒，提前刷新须为 30 秒以上且小于缓存时间，失败重试须为 30–3600 秒")
	}
	if q.Enabled && (q.ProxyID <= 0 || len(q.AccountIDs) == 0) {
		return errors.New("启用前请选择采集代理和账号")
	}
	if q.InjectionEnabled && !q.AllGroups && len(q.GroupIDs) == 0 {
		return errors.New("启用注入前请选择适用分组")
	}
	return nil
}

type openAIStateKeeperConfig struct {
	OpenAIStateKeeperSettings
	accounts map[int64]bool
	groups   map[int64]bool
}

type OpenAIStateKeeperRow struct {
	AccountID         int64      `json:"account_id"`
	Model             string     `json:"model"`
	Status            string     `json:"status"`
	Queued            bool       `json:"queued"`
	Collecting        bool       `json:"collecting"`
	HTTPStatus        int        `json:"http_status"`
	Message           string     `json:"message"`
	LastAttemptAt     *time.Time `json:"last_attempt_at,omitempty"`
	CollectedAt       *time.Time `json:"collected_at,omitempty"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	NextAttemptAt     *time.Time `json:"next_attempt_at,omitempty"`
	Fingerprint       string     `json:"fingerprint,omitempty"`
	HasCodexTurnState bool       `json:"has_codex_turn_state"`
	StateFileSaved    bool       `json:"state_file_saved"`
	Attempts          int64      `json:"attempts"`
	Successes         int64      `json:"successes"`
	Injections        int64      `json:"injections"`
}

type openAIKeptState struct {
	row             OpenAIStateKeeperRow
	value           string
	credentialStamp string
	failures        int
	lastFinishedAt  time.Time
}

type OpenAIStateKeeperEvent struct {
	At         time.Time `json:"at"`
	AccountID  int64     `json:"account_id"`
	Model      string    `json:"model"`
	HTTPStatus int       `json:"http_status"`
	Result     string    `json:"result"`
	Message    string    `json:"message"`
}

type OpenAIStateKeeperSnapshot struct {
	CollectionPath     string                    `json:"collection_path"`
	CollectionEndpoint string                    `json:"collection_endpoint"`
	Settings           OpenAIStateKeeperSettings `json:"settings"`
	Rows               []OpenAIStateKeeperRow    `json:"rows"`
	Events             []OpenAIStateKeeperEvent  `json:"events"`
	ServerTime         time.Time                 `json:"server_time"`
	ConfigError        string                    `json:"config_error,omitempty"`
}

type openAIStateProbeResult struct {
	status            int
	value             string
	message           string
	result            string
	hasCodexTurnState bool
	credentialStamp   string
}

type openAIStateKeeperJob struct {
	accountID int64
	revision  string
}

type OpenAIStateKeeperService struct {
	settings      SettingRepository
	accounts      AccountRepository
	proxies       ProxyRepository
	gateway       *OpenAIGatewayService
	config        atomic.Pointer[openAIStateKeeperConfig]
	saveMu        sync.Mutex
	mu            sync.RWMutex
	rows          map[int64]*openAIKeptState
	events        []OpenAIStateKeeperEvent
	configError   string
	queue         chan openAIStateKeeperJob
	ctx           context.Context
	cancel        context.CancelFunc
	activeCancels map[int64]context.CancelFunc
	wg            sync.WaitGroup
	probe         func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult
	files         *openAIStateFileStore
}

func ProvideOpenAIStateKeeperService(settings *SettingService, accounts AccountRepository, proxies ProxyRepository, gateway *OpenAIGatewayService) *OpenAIStateKeeperService {
	s := newOpenAIStateKeeper(settings.settingRepo, accounts, proxies, gateway)
	s.files = newOpenAIStateFileStore(gateway.cfg)
	gateway.stateKeeper.Store(s)
	s.startWorkers()
	s.wg.Add(1)
	go s.scheduler()
	return s
}

func newOpenAIStateKeeper(settings SettingRepository, accounts AccountRepository, proxies ProxyRepository, gateway *OpenAIGatewayService) *OpenAIStateKeeperService {
	ctx, cancel := context.WithCancel(context.Background())
	s := &OpenAIStateKeeperService{settings: settings, accounts: accounts, proxies: proxies, gateway: gateway, rows: map[int64]*openAIKeptState{}, events: []OpenAIStateKeeperEvent{}, queue: make(chan openAIStateKeeperJob, 500), ctx: ctx, cancel: cancel, activeCancels: make(map[int64]context.CancelFunc)}
	s.probe = s.collect
	s.install(DefaultOpenAIStateKeeperSettings())
	return s
}

func (s *OpenAIStateKeeperService) Stop() {
	if s != nil {
		s.cancel()
		s.wg.Wait()
	}
}

func (s *OpenAIStateKeeperService) install(q OpenAIStateKeeperSettings) {
	q.AccountIDs = append([]int64{}, q.AccountIDs...)
	q.GroupIDs = append([]int64{}, q.GroupIDs...)
	cfg := &openAIStateKeeperConfig{OpenAIStateKeeperSettings: q, accounts: map[int64]bool{}, groups: map[int64]bool{}}
	for _, id := range q.AccountIDs {
		cfg.accounts[id] = true
	}
	for _, id := range q.GroupIDs {
		cfg.groups[id] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.config.Load()
	if previous != nil && previous.Revision == q.Revision {
		return
	}
	for _, cancel := range s.activeCancels {
		cancel()
	}
	// Discard queued jobs from the previous configuration before accepting new
	// ones; running accounts stay reserved until their cancelled probe exits.
	for len(s.queue) > 0 {
		select {
		case <-s.queue:
		default:
		}
	}
	// Never publish state from a superseded collection, or keep state across a
	// changed account/model/proxy policy.
	oldRows := s.rows
	s.rows = map[int64]*openAIKeptState{}
	keep := previous != nil && previous.Enabled && q.Enabled && previous.ProxyID == q.ProxyID && previous.Model == q.Model && previous.TTLSeconds == q.TTLSeconds
	for _, id := range q.AccountIDs {
		entry := &openAIKeptState{row: OpenAIStateKeeperRow{AccountID: id, Model: q.Model, Status: "empty"}}
		if old := oldRows[id]; keep && old != nil {
			copy := *old
			entry = &copy
			entry.row.Queued = false
			entry.row.Collecting = false
		}
		entry.row.Collecting = s.activeCancels[id] != nil
		entry.row.NextAttemptAt = stateKeeperNextAttempt(q, entry)
		s.rows[id] = entry
	}
	s.config.Store(cfg)
}

func (s *OpenAIStateKeeperService) reload(ctx context.Context) {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	raw, err := s.settings.GetValue(ctx, openAIStateKeeperSettingKey)
	q := DefaultOpenAIStateKeeperSettings()
	if errors.Is(err, ErrSettingNotFound) {
		err = nil
	} else if err == nil {
		err = json.Unmarshal([]byte(raw), &q)
	}
	if err == nil {
		err = q.Validate()
	}
	if err != nil {
		s.mu.Lock()
		s.configError = "无法加载配置，已暂停采集和注入"
		s.mu.Unlock()
		q = DefaultOpenAIStateKeeperSettings()
		q.Revision = "unavailable"
	} else {
		s.mu.Lock()
		s.configError = ""
		s.mu.Unlock()
	}
	s.install(q)
}

func (s *OpenAIStateKeeperService) Save(ctx context.Context, q OpenAIStateKeeperSettings) error {
	q.Model = strings.TrimSpace(q.Model)
	if err := q.Validate(); err != nil {
		return err
	}
	if q.Enabled {
		proxy, err := s.proxies.GetByID(ctx, q.ProxyID)
		if err != nil || proxy == nil || !proxy.IsActive() || proxy.IsExpired(time.Now()) {
			return errors.New("采集代理不存在、已禁用或已过期")
		}
		accounts, err := s.accounts.GetByIDs(ctx, q.AccountIDs)
		if err != nil {
			return errors.New("无法读取采集账号")
		}
		if len(accounts) != len(q.AccountIDs) {
			return errors.New("部分采集账号不存在")
		}
		for _, a := range accounts {
			if !stateKeeperAccountEligible(a) {
				return errors.New("采集仅支持启用的 OpenAI OAuth 账号")
			}
		}
	}
	q.Revision = uuid.NewString()
	encoded, err := json.Marshal(q)
	if err != nil {
		return err
	}
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	if err = s.settings.Set(ctx, openAIStateKeeperSettingKey, string(encoded)); err != nil {
		return err
	}
	s.install(q)
	s.mu.Lock()
	s.configError = ""
	s.mu.Unlock()
	return nil
}

func stateKeeperAccountEligible(a *Account) bool {
	return a != nil && a.Platform == PlatformOpenAI && a.Type == AccountTypeOAuth && a.Status == StatusActive
}

func (s *OpenAIStateKeeperService) Snapshot() OpenAIStateKeeperSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs = append([]int64{}, q.AccountIDs...)
	q.GroupIDs = append([]int64{}, q.GroupIDs...)
	out := OpenAIStateKeeperSnapshot{CollectionPath: openAIStateKeeperCollectionPath, CollectionEndpoint: openAIStateKeeperCollectionURL, Settings: q, Rows: []OpenAIStateKeeperRow{}, Events: append([]OpenAIStateKeeperEvent{}, s.events...), ServerTime: time.Now().UTC(), ConfigError: s.configError}
	for _, entry := range s.rows {
		r := entry.row
		if entry.value != "" && r.ExpiresAt != nil && !r.ExpiresAt.After(out.ServerTime) {
			r.Status = "expired"
		}
		out.Rows = append(out.Rows, r)
	}
	sort.Slice(out.Rows, func(i, j int) bool { return out.Rows[i].AccountID < out.Rows[j].AccountID })
	return out
}

func (s *OpenAIStateKeeperService) Schedule(ids []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.config.Load()
	if !cfg.Enabled {
		return errors.New("请先保存并启用采集配置")
	}
	if len(ids) == 0 {
		ids = cfg.AccountIDs
	}
	if len(ids) > 500 {
		return errors.New("一次最多采集 500 个账号")
	}
	for _, id := range ids {
		if !cfg.accounts[id] {
			return errors.New("账号未加入采集范围")
		}
	}
	return s.enqueueLocked(cfg, ids)
}

// The caller holds s.mu so a configuration change cannot enqueue an old plan.
func (s *OpenAIStateKeeperService) enqueueLocked(cfg *openAIStateKeeperConfig, ids []int64) error {
	for _, id := range ids {
		entry := s.rows[id]
		if entry.row.Queued || entry.row.Collecting || s.activeCancels[id] != nil {
			continue
		}
		select {
		case s.queue <- openAIStateKeeperJob{id, cfg.Revision}:
			entry.row.Queued = true
		default:
			return errors.New("采集队列已满，请稍后重试")
		}
	}
	return nil
}

func (s *OpenAIStateKeeperService) scheduleDue(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.config.Load()
	if s.ctx.Err() != nil || !cfg.Enabled || !cfg.AutoRefresh {
		return
	}
	ids := make([]int64, 0, len(cfg.AccountIDs))
	for _, id := range cfg.AccountIDs {
		r := s.rows[id].row
		if !r.Queued && !r.Collecting && (r.NextAttemptAt == nil || !r.NextAttemptAt.After(now)) {
			ids = append(ids, id)
		}
	}
	_ = s.enqueueLocked(cfg, ids)
}

func stateKeeperNextAttempt(q OpenAIStateKeeperSettings, entry *openAIKeptState) *time.Time {
	if entry.lastFinishedAt.IsZero() {
		return nil
	}
	var next time.Time
	switch {
	case q.AutoCollectIntervalSeconds > 0:
		// An explicit interval controls successful and failed probes alike.
		next = entry.lastFinishedAt.Add(time.Duration(q.AutoCollectIntervalSeconds) * time.Second)
	case entry.failures > 0:
		shift := min(entry.failures-1, 5)
		delay := min(time.Duration(q.RetrySeconds)*time.Second*time.Duration(1<<shift), 30*time.Minute)
		next = entry.lastFinishedAt.Add(delay)
	case entry.row.ExpiresAt != nil:
		next = entry.row.ExpiresAt.Add(-time.Duration(q.RefreshBeforeSeconds) * time.Second)
	default:
		return nil
	}
	return &next
}

func (s *OpenAIStateKeeperService) scheduler() {
	defer s.wg.Done()
	reload := func() { ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second); defer cancel(); s.reload(ctx) }
	reload()
	// Restoration is background work. Requests never wait on file or DB reads.
	restoreCtx, restoreCancel := context.WithTimeout(s.ctx, 15*time.Second)
	s.restoreStateFiles(restoreCtx)
	restoreCancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastReload := time.Now()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			if now.Sub(lastReload) >= 30*time.Second {
				reload()
				lastReload = now
			}
			s.scheduleDue(now)
		}
	}
}

func (s *OpenAIStateKeeperService) startWorkers() {
	s.wg.Add(openAIStateKeeperWorkers)
	for range openAIStateKeeperWorkers {
		go s.worker()
	}
}

func (s *OpenAIStateKeeperService) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case job := <-s.queue:
			if s.config.Load().Revision != job.revision {
				continue
			}
			s.run(job)
		}
	}
}

func (s *OpenAIStateKeeperService) run(job openAIStateKeeperJob) {
	s.mu.Lock()
	cfg := s.config.Load()
	entry := s.rows[job.accountID]
	if s.ctx.Err() != nil || !cfg.Enabled || cfg.Revision != job.revision || entry == nil || s.activeCancels[job.accountID] != nil {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, 45*time.Second)
	s.activeCancels[job.accountID] = cancel
	now := time.Now().UTC()
	entry.row.Queued = false
	entry.row.Collecting = true
	entry.row.Attempts++
	entry.row.LastAttemptAt = &now
	s.mu.Unlock()
	result := s.probe(ctx, cfg.OpenAIStateKeeperSettings, job.accountID)
	cancel()
	// Serialize durable publication with configuration changes, without holding
	// the mutex used by request injection during disk I/O.
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	now = time.Now().UTC()
	saved := false
	if s.files != nil && s.config.Load().Revision == job.revision && result.result == "collected" && result.status == 292 && validCollectedState(result.value) {
		err := s.files.save(openAIStateFileRecord{AccountID: job.accountID, Model: cfg.Model, ProxyID: cfg.ProxyID, TTLSeconds: cfg.TTLSeconds, Endpoint: openAIStateKeeperCollectionURL, CredentialStamp: result.credentialStamp, Value: result.value, CollectedAt: now, ExpiresAt: now.Add(time.Duration(cfg.TTLSeconds) * time.Second)})
		if err != nil {
			result.result = "failed"
			result.message = "已取得 292，但账号 State 文件保存失败，未启用新状态"
		} else {
			saved = true
			result.message = "已取得 HTTP 292 与 current_turn_state 响应头，已写入账号独立 State 文件"
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.activeCancels, job.accountID)
	if current := s.rows[job.accountID]; current != nil {
		current.row.Collecting = false
	}
	if s.config.Load().Revision != job.revision {
		return
	}
	entry = s.rows[job.accountID]
	entry.lastFinishedAt = now
	entry.row.Collecting = false
	entry.row.HTTPStatus = result.status
	entry.row.Message = result.message
	entry.row.HasCodexTurnState = result.hasCodexTurnState
	if result.result == "collected" && result.status == 292 && validCollectedState(result.value) {
		entry.value = result.value
		entry.credentialStamp = result.credentialStamp
		entry.failures = 0
		entry.row.Status = "ready"
		entry.row.StateFileSaved = saved
		entry.row.CollectedAt = &now
		expires := now.Add(time.Duration(cfg.TTLSeconds) * time.Second)
		entry.row.ExpiresAt = &expires
		sum := sha256.Sum256([]byte(result.value))
		entry.row.Fingerprint = fmt.Sprintf("%x", sum[:6])
		entry.row.Successes++
	} else {
		entry.failures++
		entry.row.Status = result.result
		if entry.value != "" && entry.row.ExpiresAt != nil && entry.row.ExpiresAt.After(now) {
			entry.row.Status = "refresh_failed"
		}
	}
	entry.row.NextAttemptAt = stateKeeperNextAttempt(cfg.OpenAIStateKeeperSettings, entry)
	s.events = append([]OpenAIStateKeeperEvent{{At: now, AccountID: job.accountID, Model: cfg.Model, HTTPStatus: result.status, Result: result.result, Message: result.message}}, s.events...)
	if len(s.events) > 200 {
		s.events = s.events[:200]
	}
}

func validCollectedState(value string) bool {
	if len(value) == 0 || len(value) > 16384 {
		return false
	}
	for _, r := range value {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}

// Checking credentials also prevents an old account ID's state being used
// after reauthorization. Rotating an access token merely requires recollection.
func stateKeeperCredentialStamp(account *Account) string {
	sum := sha256.Sum256([]byte(account.GetOpenAIAccessToken()))
	return fmt.Sprintf("%x", sum[:])
}

func (s *OpenAIStateKeeperService) valueFor(c *gin.Context, a *Account, model string) string {
	if s == nil {
		return ""
	}
	cfg := s.config.Load()
	if cfg == nil || !cfg.Enabled || !cfg.InjectionEnabled {
		return ""
	}
	if !stateKeeperAccountEligible(a) || !cfg.accounts[a.ID] || model != cfg.Model || c == nil || c.Request == nil || c.GetBool(openAIStateProbeContextKey) {
		return ""
	}
	if !strings.HasSuffix(strings.TrimRight(c.Request.URL.Path, "/"), "/responses") {
		return ""
	}
	if !cfg.AllGroups && !cfg.groups[getOpenAIGroupIDFromContext(c)] {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Recheck under the lock so a concurrent disable cannot publish a new injection.
	if s.config.Load() != cfg {
		return ""
	}
	entry := s.rows[a.ID]
	if entry == nil || entry.value == "" || entry.row.ExpiresAt == nil || !entry.row.ExpiresAt.After(time.Now()) || entry.credentialStamp != stateKeeperCredentialStamp(a) {
		return ""
	}
	entry.row.Injections++
	return entry.value
}

func (s *OpenAIGatewayService) injectCollectedStateHTTP(c *gin.Context, a *Account, body []byte, headers http.Header) {
	if s == nil || headers == nil {
		return
	}
	keeper := s.stateKeeper.Load()
	if keeper == nil {
		return
	}
	cfg := keeper.config.Load()
	if cfg == nil || !cfg.Enabled || !cfg.InjectionEnabled {
		return
	}
	if value := keeper.valueFor(c, a, gjson.GetBytes(body, "model").String()); value != "" {
		headers.Set(openAICollectedStateHeader, value)
	}
}

func (s *OpenAIGatewayService) injectCollectedStateWS(c *gin.Context, a *Account, payload map[string]any) {
	if s == nil || payload == nil {
		return
	}
	keeper := s.stateKeeper.Load()
	if keeper == nil {
		return
	}
	model, _ := payload["model"].(string)
	value := keeper.valueFor(c, a, model)
	if value == "" {
		return
	}
	// Copy, never mutate the caller's metadata or bind experimental state to a
	// pooled handshake. Reused connections receive the current value per request.
	metadata := map[string]any{}
	switch old := payload["client_metadata"].(type) {
	case map[string]any:
		for k, v := range old {
			metadata[k] = v
		}
	case map[string]string:
		for k, v := range old {
			metadata[k] = v
		}
	}
	metadata[openAICollectedStateHeader] = value
	payload["client_metadata"] = metadata
}
