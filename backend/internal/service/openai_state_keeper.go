package service

// Capture account-specific x-codex-turn-state headers for encrypted storage
// and the separately configured request injection path.
import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const openAIStateKeeperSettingKey = "openai_state_keeper_v1"
const openAICollectedStateHeader = openAICodexTurnStateHeader
const openAIStateProbeContextKey = "openai_state_keeper_probe"
const openAIStateKeeperDefaultConcurrency = 50

type OpenAIStateKeeperSettings struct {
	Enabled                        bool     `json:"enabled"`
	InjectionEnabled               bool     `json:"injection_enabled"`
	ResponseRefreshEnabled         bool     `json:"response_refresh_enabled"`
	AutoRefresh                    bool     `json:"auto_refresh"`
	AutoCollectIntervalSeconds     int      `json:"auto_collect_interval_seconds"`
	DegradationScanEnabled         bool     `json:"degradation_scan_enabled"`
	DegradationScanIntervalSeconds int      `json:"degradation_scan_interval_seconds"`
	Concurrency                    int      `json:"concurrency"`
	AccountConcurrency             int      `json:"account_concurrency"`
	MaxAttempts                    int      `json:"max_attempts"`
	RetryCount                     int      `json:"retry_count"`
	RetryIntervalSeconds           int      `json:"retry_interval_seconds"`
	RequestIntervalSeconds         int      `json:"request_interval_seconds"`
	ProxyFailureThreshold          int      `json:"proxy_failure_threshold"`
	CooldownSeconds                int      `json:"cooldown_seconds"`
	MaxCooldownSeconds             int      `json:"max_cooldown_seconds"`
	AccountHourlyLimit             int      `json:"account_hourly_limit"`
	AccountFiveMinuteLimit         int      `json:"account_five_minute_limit"`
	AccountTenMinuteLimit          int      `json:"account_ten_minute_limit"`
	AllowedStateLengths            []int    `json:"allowed_state_lengths"`
	DegradedStateLengths           []int    `json:"degraded_state_lengths"`
	AccountIDs                     []int64  `json:"account_ids"`
	CollectionGroupIDs             []int64  `json:"collection_group_ids"`
	GroupIDs                       []int64  `json:"group_ids"`
	AllGroups                      bool     `json:"all_groups"`
	ProxyID                        int64    `json:"proxy_id"`
	ProxyIDs                       []int64  `json:"proxy_ids,omitempty"`
	Model                          string   `json:"model"`
	Models                         []string `json:"models,omitempty"`
	Revision                       string   `json:"revision"`
}

func DefaultOpenAIStateKeeperSettings() OpenAIStateKeeperSettings {
	return OpenAIStateKeeperSettings{AutoRefresh: true, DegradationScanIntervalSeconds: 60, Concurrency: openAIStateKeeperDefaultConcurrency, AccountConcurrency: 1, MaxAttempts: 3, RetryIntervalSeconds: 5, RequestIntervalSeconds: 1, ProxyFailureThreshold: 2, CooldownSeconds: 30, MaxCooldownSeconds: 900, AccountHourlyLimit: 120, AllowedStateLengths: []int{}, DegradedStateLengths: []int{}, AccountIDs: []int64{}, GroupIDs: []int64{}, Model: "gpt-6-astra"}
}

func (q OpenAIStateKeeperSettings) modelNames() []string {
	if q.Models != nil {
		return q.Models
	}
	return []string{q.Model}
}

func (q OpenAIStateKeeperSettings) forModel(model string) OpenAIStateKeeperSettings {
	q.Model = model
	return q
}

func (q OpenAIStateKeeperSettings) proxyIDs() []int64 {
	if q.ProxyIDs != nil {
		return q.ProxyIDs
	}
	if q.ProxyID > 0 {
		return []int64{q.ProxyID}
	}
	return nil
}

func (q OpenAIStateKeeperSettings) allowsProxy(id int64) bool {
	for _, selected := range q.proxyIDs() {
		if selected == id {
			return true
		}
	}
	return false
}

func defaultStateKeeperModels() []string {
	return []string{"gpt-5.5", "gpt-5.6-sol", "gpt-5.6-terra", "gpt-6-astra"}
}

func (q OpenAIStateKeeperSettings) Validate() error {
	if q.AccountFiveMinuteLimit < 0 || q.AccountFiveMinuteLimit > 10000 || q.AccountTenMinuteLimit < 0 || q.AccountTenMinuteLimit > 10000 {
		return errors.New("每账号 5 分钟和 10 分钟采集上限须为 0–10000，0 表示不限")
	}
	if q.RequestIntervalSeconds < 0 || q.RequestIntervalSeconds > 300 || q.ProxyFailureThreshold < 1 || q.ProxyFailureThreshold > 100 {
		return errors.New("单账号请求间隔须为 0–300 秒，切换代理失败次数须为 1–100")
	}
	if q.CooldownSeconds < 1 || q.MaxCooldownSeconds < q.CooldownSeconds || q.MaxCooldownSeconds > 86400 || q.AccountHourlyLimit < 1 || q.AccountHourlyLimit > 10000 {
		return errors.New("自动冷却须为 1–86400 秒且上限不小于起始值，每账号每小时上限须为 1–10000")
	}
	if len(q.AccountIDs) > 500 || len(q.GroupIDs) > 500 || len(q.CollectionGroupIDs) > 500 {
		return errors.New("最多选择 500 个账号或分组")
	}
	for _, ids := range [][]int64{q.AccountIDs, q.GroupIDs, q.CollectionGroupIDs} {
		seen := map[int64]bool{}
		for _, id := range ids {
			if id <= 0 || seen[id] {
				return errors.New("账号和分组 ID 必须为不重复的正整数")
			}
			seen[id] = true
		}
	}
	models := q.modelNames()
	if len(models) == 0 || len(models) > 20 {
		return errors.New("请填写 1–20 个采集模型，每行一个")
	}
	seenModels := map[string]bool{}
	for _, model := range models {
		if model == "" || len(model) > 200 || strings.ContainsAny(model, " \t\r\n") || seenModels[model] {
			return errors.New("采集模型不能为空、包含空白或重复，单个最多 200 字符")
		}
		seenModels[model] = true
	}
	proxies := q.proxyIDs()
	if len(proxies) > 20 {
		return errors.New("最多选择 20 个采集代理")
	}
	seenProxies := map[int64]bool{}
	for _, id := range proxies {
		if id <= 0 || seenProxies[id] {
			return errors.New("采集代理必须为不重复的有效 ID")
		}
		seenProxies[id] = true
	}
	if q.RetryCount < 0 || q.RetryCount > 100 || q.RetryIntervalSeconds < 0 || q.RetryIntervalSeconds > 86400 || ((q.RetryCount > 0 || len(proxies) > 1) && q.RetryIntervalSeconds == 0) {
		return errors.New("重试次数须为 0–100，启用重试时间隔须为 1–86400 秒")
	}
	if q.AutoCollectIntervalSeconds < 0 || q.AutoCollectIntervalSeconds > 86400 {
		return errors.New("自动采集间隔须为 0–86400 秒，0 表示不定时采集")
	}
	if q.DegradationScanIntervalSeconds < 0 || q.DegradationScanIntervalSeconds > 86400 || (q.DegradationScanEnabled && q.DegradationScanIntervalSeconds == 0) {
		return errors.New("降智扫描间隔须为 1–86400 秒")
	}
	if q.Concurrency < 1 || q.Concurrency > 500 {
		return errors.New("采集并发数须为 1–500")
	}
	if q.AccountConcurrency < 1 || q.AccountConcurrency > 100 || q.MaxAttempts < 1 || q.MaxAttempts > 100 {
		return errors.New("单账号并发和每轮最多次数须为 1–100")
	}
	if len(q.DegradedStateLengths) > 100 {
		return errors.New("降智响应头长度最多填写 100 个")
	}
	degraded := map[int]bool{}
	for _, length := range q.DegradedStateLengths {
		if length < 1 || length > 16384 || degraded[length] {
			return errors.New("降智响应头长度须为不重复的 1–16384 整数")
		}
		degraded[length] = true
	}
	if len(q.AllowedStateLengths) > 100 {
		return errors.New("允许保存的响应头长度最多填写 100 个")
	}
	seenLengths := make(map[int]bool, len(q.AllowedStateLengths))
	for _, length := range q.AllowedStateLengths {
		if length < 1 || length > 16384 || seenLengths[length] {
			return errors.New("允许保存的响应头长度须为不重复的 1–16384 整数")
		}
		seenLengths[length] = true
		if degraded[length] {
			return errors.New("允许保存与降智响应头长度不能重叠")
		}
	}
	if q.Enabled && (len(proxies) == 0 || (len(q.AccountIDs) == 0 && len(q.CollectionGroupIDs) == 0)) {
		return errors.New("启用前请选择采集代理，以及采集账号或分组")
	}
	if q.InjectionEnabled && !q.AllGroups && len(q.GroupIDs) == 0 {
		return errors.New("启用注入前请选择适用分组")
	}
	return nil
}

func (q OpenAIStateKeeperSettings) allowsStateLength(length int) bool {
	if q.isDegradedLength(length) {
		return false
	}
	if len(q.AllowedStateLengths) == 0 {
		return true
	}
	for _, allowed := range q.AllowedStateLengths {
		if length == allowed {
			return true
		}
	}
	return false
}

func (q OpenAIStateKeeperSettings) isDegradedLength(length int) bool {
	for _, n := range q.DegradedStateLengths {
		if n == length {
			return true
		}
	}
	return false
}

type openAIStateKeeperConfig struct {
	OpenAIStateKeeperSettings
	accounts         map[int64]bool
	collectionGroups map[int64]bool
	groups           map[int64]bool
	models           map[string]bool
}

type openAIStateKey struct {
	accountID int64
	model     string
}

func (s *OpenAIStateKeeperService) key(id int64, models ...string) openAIStateKey {
	model := s.config.Load().modelNames()[0]
	if len(models) > 0 && models[0] != "" {
		model = models[0]
	}
	return openAIStateKey{id, model}
}

func (s *OpenAIStateKeeperService) entryLocked(id int64, models ...string) *openAIKeptState {
	return s.rows[s.key(id, models...)]
}

type OpenAIStateKeeperRow struct {
	AccountID                int64                  `json:"account_id"`
	AccountName              string                 `json:"account_name"`
	AccountGroupIDs          []int64                `json:"account_group_ids"`
	AccountStatus            string                 `json:"account_status"`
	AccountUnavailable       bool                   `json:"account_unavailable"`
	AccountUnavailableReason string                 `json:"account_unavailable_reason"`
	Model                    string                 `json:"model"`
	Status                   string                 `json:"status"`
	Queued                   bool                   `json:"queued"`
	Collecting               bool                   `json:"collecting"`
	HTTPStatus               int                    `json:"http_status"`
	TurnStateLength          int                    `json:"turn_state_length"`
	Message                  string                 `json:"message"`
	LastAttemptAt            *time.Time             `json:"last_attempt_at,omitempty"`
	LastCollectionAt         *time.Time             `json:"last_collection_at,omitempty"`
	CollectedAt              *time.Time             `json:"collected_at,omitempty"`
	NextAttemptAt            *time.Time             `json:"next_attempt_at,omitempty"`
	Fingerprint              string                 `json:"fingerprint,omitempty"`
	HasCodexTurnState        bool                   `json:"has_codex_turn_state"`
	HasDetails               bool                   `json:"has_details"`
	StateFileSaved           bool                   `json:"state_file_saved"`
	Attempts                 int64                  `json:"attempts"`
	Successes                int64                  `json:"successes"`
	Injections               int64                  `json:"injections"`
	Paused                   bool                   `json:"paused"`
	PauseReason              string                 `json:"pause_reason"`
	RoundID                  string                 `json:"round_id"`
	RoundAttempts            int                    `json:"round_attempts"`
	RoundSource              string                 `json:"round_source"`
	QualityStatus            string                 `json:"quality_status"`
	QualityReason            string                 `json:"quality_reason"`
	StatePreview             string                 `json:"state_preview,omitempty"`
	SavedStateLength         int                    `json:"saved_state_length"`
	RetryAttempt             int                    `json:"retry_attempt"`
	RetryLimit               int                    `json:"retry_limit"`
	CollectionProxyID        int64                  `json:"collection_proxy_id"`
	SavedProxyID             int64                  `json:"saved_proxy_id"`
	ProxyAttempt             int                    `json:"proxy_attempt"`
	ProxyCount               int                    `json:"proxy_count"`
	NextRetryAt              *time.Time             `json:"next_retry_at,omitempty"`
	AutoRetryPending         bool                   `json:"auto_retry_pending"`
	RetryReason              string                 `json:"retry_reason"`
	FailureCycles            int                    `json:"failure_cycles"`
	CooldownUntil            *time.Time             `json:"cooldown_until,omitempty"`
	HourlyRequests           int                    `json:"hourly_requests"`
	FiveMinuteRequests       int                    `json:"five_minute_requests"`
	TenMinuteRequests        int                    `json:"ten_minute_requests"`
	EffectiveConcurrency     int                    `json:"effective_concurrency"`
	Models                   []OpenAIStateKeeperRow `json:"models,omitempty"`
}

type openAIKeptState struct {
	row                    OpenAIStateKeeperRow
	value                  string
	detail                 *OpenAIStateKeeperDetail
	credentialStamp        string
	blockedCredentialStamp string
	proxyID                int64
	lastFinishedAt         time.Time
	version                string
	refreshVersion         string
	runtimeLoaded          bool
	scopeLoading           bool
	scopeID                string
	qualityRevision        string
	qualityAt              time.Time
	collections            []OpenAIStateKeeperEvent
	injections             []OpenAIStateKeeperEvent
	nextProxyID            int64
}

type OpenAIStateKeeperEvent struct {
	At              time.Time `json:"at"`
	AccountID       int64     `json:"account_id"`
	Model           string    `json:"model"`
	HTTPStatus      int       `json:"http_status"`
	TurnStateLength int       `json:"turn_state_length"`
	Result          string    `json:"result"`
	Message         string    `json:"message"`
	ID              string    `json:"id,omitempty"`
	Kind            string    `json:"kind"`
	Source          string    `json:"source"`
	RoundID         string    `json:"round_id,omitempty"`
	Attempt         int       `json:"attempt"`
	InjectedLength  int       `json:"injected_length,omitempty"`
	ProxyID         int64     `json:"proxy_id,omitempty"`
}

type OpenAIStateKeeperSnapshot struct {
	CollectionPath     string                    `json:"collection_path"`
	CollectionEndpoint string                    `json:"collection_endpoint"`
	Settings           OpenAIStateKeeperSettings `json:"settings"`
	Rows               []OpenAIStateKeeperRow    `json:"rows"`
	Events             []OpenAIStateKeeperEvent  `json:"events"`
	ServerTime         time.Time                 `json:"server_time"`
	ConfigError        string                    `json:"config_error,omitempty"`
	ProxySuccesses     map[int64]int64           `json:"proxy_successes"`
	ProxyStatsError    string                    `json:"proxy_stats_error,omitempty"`
}

type OpenAIStateKeeperDetail struct {
	AccountID       int64     `json:"account_id"`
	Model           string    `json:"model"`
	HTTPStatus      int       `json:"http_status"`
	HeaderName      string    `json:"header_name"`
	HeaderValue     string    `json:"header_value"`
	TurnStateLength int       `json:"turn_state_length"`
	CollectedAt     time.Time `json:"collected_at"`
	SaveAllowed     bool      `json:"save_allowed"`
	StateFileSaved  bool      `json:"state_file_saved"`
}

type openAIStateProbeResult struct {
	status             int
	value              string
	message            string
	result             string
	hasCodexTurnState  bool
	turnStateLength    int
	credentialStamp    string
	accountUnavailable bool
	retryAfter         time.Duration
	permanentFailure   bool
	proxyFailure       bool
}

type openAIStateKeeperJob struct {
	accountID int64
	model     string
	revision  string
	source    string
	scopeID   string
}

type OpenAIStateKeeperService struct {
	settings            SettingRepository
	accounts            AccountRepository
	proxies             ProxyRepository
	gateway             *OpenAIGatewayService
	config              atomic.Pointer[openAIStateKeeperConfig]
	saveMu              sync.Mutex
	mu                  sync.RWMutex
	rows                map[openAIStateKey]*openAIKeptState
	events              []OpenAIStateKeeperEvent
	configError         string
	queue               chan openAIStateKeeperJob
	ctx                 context.Context
	cancel              context.CancelFunc
	activeCancels       map[openAIStateKey]context.CancelFunc
	workerSlots         *sync.Cond
	workerCount         int
	wg                  sync.WaitGroup
	probe               func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult
	files               *openAIStateFileStore
	quality             *AccountQualityService
	qualityPolicy       AccountQualitySettings
	activeProbes        int
	accountProbes       map[int64]int
	collectionLimits    map[int64]*openAIStateCollectionLimit
	collectionSaves     [64]sync.Mutex
	observations        chan openAIStateObservation
	pendingSignals      map[openAIStateKey]openAIStateObservation
	signalWake          chan struct{}
	eventsStarted       bool
	dirtyRuntime        map[openAIStateKey]bool
	nextDegradationScan time.Time
	proxySuccesses      map[int64]int64
	proxyStatsLoaded    bool
	proxyStatsDirty     bool
	proxyStatsError     string
}

func ProvideOpenAIStateKeeperService(settings *SettingService, accounts AccountRepository, proxies ProxyRepository, gateway *OpenAIGatewayService, quality *AccountQualityService) *OpenAIStateKeeperService {
	s := newOpenAIStateKeeper(settings.settingRepo, accounts, proxies, gateway)
	s.files = newOpenAIStateFileStore(gateway.cfg)
	s.quality = quality
	quality.stateKeeper.Store(s)
	gateway.stateKeeper.Store(s)
	s.startWorkers()
	s.wg.Add(1)
	go s.scheduler()
	return s
}

func newOpenAIStateKeeper(settings SettingRepository, accounts AccountRepository, proxies ProxyRepository, gateway *OpenAIGatewayService) *OpenAIStateKeeperService {
	ctx, cancel := context.WithCancel(context.Background())
	s := &OpenAIStateKeeperService{settings: settings, accounts: accounts, proxies: proxies, gateway: gateway, rows: map[openAIStateKey]*openAIKeptState{}, events: []OpenAIStateKeeperEvent{}, queue: make(chan openAIStateKeeperJob, 10000), ctx: ctx, cancel: cancel, activeCancels: make(map[openAIStateKey]context.CancelFunc), accountProbes: make(map[int64]int)}
	s.workerSlots = sync.NewCond(&s.mu)
	s.collectionLimits = make(map[int64]*openAIStateCollectionLimit)
	s.observations = make(chan openAIStateObservation, 4096)
	s.pendingSignals = make(map[openAIStateKey]openAIStateObservation)
	s.signalWake = make(chan struct{}, 1)
	s.dirtyRuntime = make(map[openAIStateKey]bool)
	s.proxySuccesses = make(map[int64]int64)
	s.probe = s.collect
	initial := DefaultOpenAIStateKeeperSettings()
	initial.Models = defaultStateKeeperModels()
	s.install(initial)
	return s
}

func (s *OpenAIStateKeeperService) Stop() {
	if s != nil {
		s.cancel()
		s.mu.Lock()
		s.workerSlots.Broadcast()
		s.mu.Unlock()
		s.wg.Wait()
	}
}

func (s *OpenAIStateKeeperService) install(q OpenAIStateKeeperSettings) {
	q.AccountIDs = append([]int64{}, q.AccountIDs...)
	q.CollectionGroupIDs = append([]int64{}, q.CollectionGroupIDs...)
	q.GroupIDs = append([]int64{}, q.GroupIDs...)
	q.AllowedStateLengths = append([]int{}, q.AllowedStateLengths...)
	q.DegradedStateLengths = append([]int{}, q.DegradedStateLengths...)
	if q.Models != nil {
		q.Models = append([]string{}, q.Models...)
		q.Model = q.Models[0]
	}
	if q.ProxyIDs != nil {
		q.ProxyIDs = append([]int64{}, q.ProxyIDs...)
		if len(q.ProxyIDs) > 0 {
			q.ProxyID = q.ProxyIDs[0]
		} else {
			q.ProxyID = 0
		}
	}
	cfg := &openAIStateKeeperConfig{OpenAIStateKeeperSettings: q, accounts: map[int64]bool{}, collectionGroups: map[int64]bool{}, groups: map[int64]bool{}, models: map[string]bool{}}
	for _, id := range q.CollectionGroupIDs {
		cfg.collectionGroups[id] = true
	}
	for _, model := range q.modelNames() {
		cfg.models[model] = true
	}
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
	blockedAccounts := make(map[int64]*openAIKeptState)
	for key, entry := range oldRows {
		if entry.blockedCredentialStamp != "" {
			blockedAccounts[key.accountID] = entry
		}
	}
	s.rows = map[openAIStateKey]*openAIKeptState{}
	keepPolicy := previous != nil && previous.Enabled && q.Enabled
	selectedIDs := append([]int64{}, q.AccountIDs...)
	// Preserve resolved group members until the background membership sync.
	// Request and collection boundaries also check the account's current groups.
	if previous != nil && len(q.CollectionGroupIDs) > 0 {
		seen := make(map[int64]bool, len(selectedIDs))
		for _, id := range selectedIDs {
			seen[id] = true
		}
		for key := range oldRows {
			if !seen[key.accountID] {
				selectedIDs = append(selectedIDs, key.accountID)
				seen[key.accountID] = true
			}
		}
	}
	for _, id := range selectedIDs {
		for _, model := range q.modelNames() {
			key := openAIStateKey{id, model}
			old := oldRows[key]
			keep := keepPolicy && old != nil && (old.value == "" || q.allowsProxy(old.proxyID))
			entry := &openAIKeptState{scopeID: uuid.NewString(), row: OpenAIStateKeeperRow{AccountID: id, Model: model, Status: "empty", QualityStatus: "pending"}}
			if old := oldRows[key]; keep && old != nil {
				copy := *old
				entry = &copy
				if entry.row.Queued {
					entry.refreshVersion = ""
				}
				entry.row.Queued = false
				entry.row.Collecting = false
				if entry.detail != nil {
					detail := *entry.detail
					detail.SaveAllowed = q.allowsStateLength(detail.TurnStateLength)
					entry.detail = &detail
				}
				if entry.value != "" && !q.allowsStateLength(len(entry.value)) {
					entry.value = ""
					entry.credentialStamp = ""
					entry.row.CollectedAt = nil
					entry.row.Fingerprint = ""
					entry.row.StateFileSaved = false
					entry.row.Status = "filtered"
					entry.row.Message = "原缓存不符合当前响应头长度规则，等待重新采集"
				}
			}
			if old := oldRows[key]; old != nil && !keep {
				entry.row.Paused, entry.row.PauseReason = old.row.Paused, old.row.PauseReason
				entry.runtimeLoaded = old.runtimeLoaded
				entry.collections, entry.injections = old.collections, old.injections
				entry.row.QualityStatus, entry.row.QualityReason = old.row.QualityStatus, old.row.QualityReason
				entry.qualityRevision, entry.qualityAt = old.qualityRevision, old.qualityAt
				entry.lastFinishedAt, entry.row.LastCollectionAt = old.lastFinishedAt, old.row.LastCollectionAt
				entry.row.RoundID, entry.row.RoundAttempts, entry.row.RoundSource = old.row.RoundID, old.row.RoundAttempts, old.row.RoundSource
				entry.row.RetryAttempt, entry.row.RetryLimit = old.row.RetryAttempt, old.row.RetryLimit
				entry.row.CollectionProxyID, entry.row.ProxyAttempt, entry.row.ProxyCount = old.row.CollectionProxyID, old.row.ProxyAttempt, old.row.ProxyCount
				entry.row.Attempts, entry.row.Successes, entry.row.Injections = old.row.Attempts, old.row.Successes, old.row.Injections
				entry.row.AutoRetryPending, entry.row.NextRetryAt, entry.row.RetryReason = old.row.AutoRetryPending, old.row.NextRetryAt, old.row.RetryReason
				entry.row.FailureCycles, entry.nextProxyID = old.row.FailureCycles, old.nextProxyID
				entry.row.AccountStatus, entry.row.AccountUnavailable, entry.row.AccountUnavailableReason = old.row.AccountStatus, old.row.AccountUnavailable, old.row.AccountUnavailableReason
				entry.blockedCredentialStamp = old.blockedCredentialStamp
			}
			entry.row.Collecting = s.activeCancels[key] != nil
			if blocked := blockedAccounts[id]; blocked != nil {
				entry.blockedCredentialStamp = blocked.blockedCredentialStamp
				entry.row.AccountStatus = blocked.row.AccountStatus
				entry.row.AccountUnavailable, entry.row.AccountUnavailableReason = true, blocked.row.AccountUnavailableReason
				pauseStateCollectionForAccount(entry, blocked.row.AccountUnavailableReason)
			}
			if !entry.row.AutoRetryPending {
				entry.row.NextRetryAt = nil
			}
			if entry.row.RoundSource == "response" && (!q.InjectionEnabled || !q.ResponseRefreshEnabled) {
				entry.row.AutoRetryPending, entry.row.NextRetryAt, entry.row.RetryReason = false, nil, ""
				entry.refreshVersion = ""
			}
			entry.row.NextAttemptAt = stateKeeperNextAttempt(q, entry)
			s.rows[key] = entry
		}
	}
	s.config.Store(cfg)
	s.pendingSignals = make(map[openAIStateKey]openAIStateObservation)
	// Unrelated settings edits must not make degradation scanning immediately due.
	if previous == nil || previous.Enabled != q.Enabled || previous.DegradationScanEnabled != q.DegradationScanEnabled {
		s.nextDegradationScan = time.Time{}
	} else if !s.nextDegradationScan.IsZero() {
		s.nextDegradationScan = s.nextDegradationScan.Add(time.Duration(q.DegradationScanIntervalSeconds-previous.DegradationScanIntervalSeconds) * time.Second)
	}
	if s.workerCount > 0 {
		s.growWorkersLocked(q.Concurrency)
	}
	s.workerSlots.Broadcast()
}

func (s *OpenAIStateKeeperService) reload(ctx context.Context) {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	raw, err := s.settings.GetValue(ctx, openAIStateKeeperSettingKey)
	q := DefaultOpenAIStateKeeperSettings()
	if errors.Is(err, ErrSettingNotFound) {
		err = nil
		q.Models = defaultStateKeeperModels()
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
	var selected []*Account
	if len(q.CollectionGroupIDs) > 0 {
		selected, err = s.resolveCollectionAccounts(ctx, q, nil)
		if err != nil {
			s.mu.Lock()
			s.configError = "采集分组同步失败，保留当前范围，请稍后重试"
			s.mu.Unlock()
			return
		}
	}
	s.install(q)
	if len(q.CollectionGroupIDs) > 0 {
		s.syncCollectionScope(q, selected)
	}
	s.restoreRuntime()
	s.restoreScopeStateFiles(ctx)
}

func sameStateKeeperSettings(a, b OpenAIStateKeeperSettings) bool {
	for _, q := range []*OpenAIStateKeeperSettings{&a, &b} {
		q.Revision = ""
		q.Models, q.ProxyIDs = append([]string{}, q.modelNames()...), append([]int64{}, q.proxyIDs()...)
		q.Model, q.ProxyID = "", 0
		// Model and proxy order is meaningful; account selections and lengths are sets.
		for _, ids := range []*[]int64{&q.AccountIDs, &q.CollectionGroupIDs, &q.GroupIDs} {
			*ids = append([]int64{}, (*ids)...)
			slices.Sort(*ids)
		}
		for _, lengths := range []*[]int{&q.AllowedStateLengths, &q.DegradedStateLengths} {
			*lengths = append([]int{}, (*lengths)...)
			slices.Sort(*lengths)
		}
	}
	return reflect.DeepEqual(a, b)
}

func (s *OpenAIStateKeeperService) Save(ctx context.Context, q OpenAIStateKeeperSettings) error {
	q.Model = strings.TrimSpace(q.Model)
	if q.Models != nil {
		q.Models = append([]string{}, q.Models...)
		for i := range q.Models {
			q.Models[i] = strings.TrimSpace(q.Models[i])
		}
	}
	if err := q.Validate(); err != nil {
		return err
	}
	var selectedAccounts []*Account
	if q.Enabled {
		for _, id := range q.proxyIDs() {
			proxy, err := s.proxies.GetByID(ctx, id)
			if err != nil || proxy == nil || !proxy.IsActive() || proxy.IsExpired(time.Now()) {
				return fmt.Errorf("采集代理 #%d 不存在、已禁用或已过期", id)
			}
		}
		accounts, err := s.accounts.GetByIDs(ctx, q.AccountIDs)
		if err != nil {
			return errors.New("无法读取采集账号")
		}
		if len(accounts) != len(q.AccountIDs) {
			return errors.New("部分采集账号不存在")
		}
		for _, a := range accounts {
			if a == nil || a.Platform != PlatformOpenAI || a.Type != AccountTypeOAuth {
				return errors.New("采集仅支持 OpenAI OAuth 账号")
			}
		}
		selectedAccounts = accounts
	}
	selectedAccounts, err := s.resolveCollectionAccounts(ctx, q, selectedAccounts)
	if err != nil {
		return err
	}
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	q.Revision = uuid.NewString()
	if current := s.config.Load(); current != nil && current.Revision != "" && sameStateKeeperSettings(current.OpenAIStateKeeperSettings, q) {
		q.Revision = current.Revision
	}
	encoded, err := json.Marshal(q)
	if err != nil {
		return err
	}
	if err = s.settings.Set(ctx, openAIStateKeeperSettingKey, string(encoded)); err != nil {
		return err
	}
	s.install(q)
	s.syncCollectionScope(q, selectedAccounts)
	s.restoreRuntime()
	s.restoreScopeStateFiles(ctx)
	s.mu.Lock()
	for _, account := range selectedAccounts {
		s.syncAccountAvailabilityLocked(account)
	}
	s.configError = ""
	blockedKeys := make([]openAIStateKey, 0)
	for key, entry := range s.rows {
		if entry.blockedCredentialStamp != "" {
			blockedKeys = append(blockedKeys, key)
		}
	}
	s.mu.Unlock()
	for _, key := range blockedKeys {
		if err := s.persistRuntimeLocked(key.accountID, key.model); err != nil {
			return errors.New("配置已保存，但账号停止采集标记写入失败，请检查文件权限")
		}
	}
	return nil
}

// Reconcile persisted references with deleted resources without treating an
// unavailable database or an inactive resource as a deletion.
func (s *OpenAIStateKeeperService) SyncSelection(ctx context.Context) error {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	raw, err := s.settings.GetValue(ctx, openAIStateKeeperSettingKey)
	if errors.Is(err, ErrSettingNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	q := DefaultOpenAIStateKeeperSettings()
	if err := json.Unmarshal([]byte(raw), &q); err != nil {
		return err
	}
	if err := q.Validate(); err != nil {
		return err
	}
	ids := q.proxyIDs()
	kept := make([]int64, 0, len(ids))
	for _, id := range ids {
		proxy, err := s.proxies.GetByID(ctx, id)
		if errors.Is(err, ErrProxyNotFound) || (err == nil && proxy == nil) {
			continue
		}
		if err != nil {
			return err
		}
		kept = append(kept, id)
	}
	accounts, err := s.accounts.GetByIDs(ctx, q.AccountIDs)
	if err != nil {
		return err
	}
	existingAccounts := make(map[int64]bool, len(accounts))
	for _, account := range accounts {
		if account != nil {
			existingAccounts[account.ID] = true
		}
	}
	keptAccounts := make([]int64, 0, len(q.AccountIDs))
	for _, id := range q.AccountIDs {
		if existingAccounts[id] {
			keptAccounts = append(keptAccounts, id)
		}
	}
	selectionChanged := len(kept) != len(ids) || len(keptAccounts) != len(q.AccountIDs)
	q.ProxyIDs, q.ProxyID, q.AccountIDs = kept, 0, keptAccounts
	if len(kept) > 0 {
		q.ProxyID = kept[0]
	}
	if len(kept) == 0 || (len(keptAccounts) == 0 && len(q.CollectionGroupIDs) == 0) {
		q.Enabled = false
	}
	accounts, err = s.resolveCollectionAccounts(ctx, q, accounts)
	if err != nil {
		return err
	}
	if selectionChanged {
		q.Revision = uuid.NewString()
		encoded, err := json.Marshal(q)
		if err != nil {
			return err
		}
		if err := s.settings.Set(ctx, openAIStateKeeperSettingKey, string(encoded)); err != nil {
			return err
		}
	}
	s.install(q)
	s.syncCollectionScope(q, accounts)
	s.restoreRuntime()
	s.restoreScopeStateFiles(ctx)
	s.syncAccountAvailability(accounts)
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
	q.CollectionGroupIDs = append([]int64{}, q.CollectionGroupIDs...)
	q.GroupIDs = append([]int64{}, q.GroupIDs...)
	q.AllowedStateLengths = append([]int{}, q.AllowedStateLengths...)
	q.DegradedStateLengths = append([]int{}, q.DegradedStateLengths...)
	if q.Models != nil {
		q.Models = append([]string{}, q.Models...)
	}
	if q.ProxyIDs != nil {
		q.ProxyIDs = append([]int64{}, q.ProxyIDs...)
	}
	out := OpenAIStateKeeperSnapshot{CollectionPath: openAIStateKeeperCollectionPath, CollectionEndpoint: openAIStateKeeperCollectionURL, Settings: q, Rows: []OpenAIStateKeeperRow{}, Events: append([]OpenAIStateKeeperEvent{}, s.events...), ServerTime: time.Now().UTC(), ConfigError: s.configError}
	out.ProxySuccesses = make(map[int64]int64, len(s.proxySuccesses))
	for id, count := range s.proxySuccesses {
		out.ProxySuccesses[id] = count
	}
	out.ProxyStatsError = s.proxyStatsError
	for _, id := range s.collectionAccountIDsLocked() {
		var row OpenAIStateKeeperRow
		for i, model := range q.modelNames() {
			entry := s.entryLocked(id, model)
			if entry == nil {
				continue
			}
			item := entry.row
			s.decorateCollectionLimitLocked(&item, time.Now())
			item.SavedStateLength = len(entry.value)
			item.SavedProxyID = entry.proxyID
			if entry.value != "" {
				item.StatePreview = entry.value[:1] + "..."
				if len(entry.value) > 14 {
					item.StatePreview = entry.value[:7] + "..." + entry.value[len(entry.value)-6:]
				}
			}
			if i == 0 {
				row = item
				row.Attempts, row.Successes, row.Injections = 0, 0, 0
			}
			row.Models = append(row.Models, item)
			row.Attempts += item.Attempts
			row.Successes += item.Successes
			row.Injections += item.Injections
			row.Collecting = row.Collecting || item.Collecting
			row.Queued = row.Queued || item.Queued
		}
		out.Rows = append(out.Rows, row)
	}
	sort.Slice(out.Rows, func(i, j int) bool { return out.Rows[i].AccountID < out.Rows[j].AccountID })
	return out
}

func (s *OpenAIStateKeeperService) Detail(accountID int64, model ...string) (OpenAIStateKeeperDetail, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry := s.entryLocked(accountID, model...)
	if entry == nil || entry.detail == nil {
		return OpenAIStateKeeperDetail{}, false
	}
	return *entry.detail, true
}

func (s *OpenAIStateKeeperService) Schedule(ids []int64, models ...string) error {
	if err := s.refreshAccountAvailability(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.config.Load()
	if !cfg.Enabled {
		return errors.New("请先保存并启用采集配置")
	}
	if len(ids) > 500 {
		return errors.New("一次最多采集 500 个账号")
	}
	if len(ids) == 0 {
		ids = s.collectionAccountIDsLocked()
	}
	for _, id := range ids {
		if s.entryLocked(id) == nil {
			return errors.New("账号未加入采集范围")
		}
	}
	if len(models) > 0 && models[0] != "" && !cfg.models[models[0]] {
		return errors.New("模型未加入采集范围")
	}
	return s.enqueueSourceLocked(cfg, ids, "manual", models...)
}

func (s *OpenAIStateKeeperService) SchedulePaused() (int, error) {
	if err := s.refreshAccountAvailability(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.config.Load()
	if !cfg.Enabled {
		return 0, errors.New("请先保存并启用采集配置")
	}
	scheduled := 0
	for _, id := range s.collectionAccountIDsLocked() {
		for _, model := range cfg.modelNames() {
			key := openAIStateKey{id, model}
			entry := s.rows[key]
			if entry == nil || entry.row.AccountUnavailable || !entry.row.Paused || entry.row.Queued || entry.row.Collecting || s.activeCancels[key] != nil {
				continue
			}
			if err := s.enqueueSourceLocked(cfg, []int64{id}, "manual", model); err != nil {
				return scheduled, err
			}
			scheduled++
		}
	}
	return scheduled, nil
}

// This explicit administrator action releases local collection cooldowns.
// Normal scheduling and individual manual collection still honor all limits.
func (s *OpenAIStateKeeperService) ScheduleCooling() (int, error) {
	if err := s.refreshAccountAvailability(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.config.Load()
	if !cfg.Enabled {
		return 0, errors.New("请先保存并启用采集配置")
	}
	now := time.Now().UTC()
	var targets []openAIStateKey
	accounts := make(map[int64]bool)
	for _, id := range s.collectionAccountIDsLocked() {
		until, _ := s.accountCollectionWaitLocked(id, cfg, now)
		for _, model := range cfg.modelNames() {
			key := openAIStateKey{id, model}
			entry := s.rows[key]
			if entry == nil || entry.scopeLoading || entry.row.AccountUnavailable || entry.row.Paused || entry.row.Queued || entry.row.Collecting || s.activeCancels[key] != nil {
				continue
			}
			if !entry.row.AutoRetryPending && until.IsZero() {
				continue
			}
			targets = append(targets, key)
			accounts[id] = true
		}
	}
	// Reserve capacity before changing any limit; other producers also hold s.mu.
	if len(targets) > cap(s.queue)-len(s.queue) {
		return 0, errors.New("采集队列已满，请稍后重试")
	}
	for id := range accounts {
		limit := s.collectionLimitLocked(id)
		limit.CooldownUntil, limit.CooldownReason = time.Time{}, ""
		if cfg.AccountHourlyLimit > 0 && limit.WindowRequests >= cfg.AccountHourlyLimit {
			limit.WindowStartedAt, limit.WindowRequests = now, 0
		}
		if cfg.AccountFiveMinuteLimit > 0 && limit.FiveMinute.Requests >= cfg.AccountFiveMinuteLimit {
			limit.FiveMinute = openAIStateBudgetWindow{StartedAt: now}
		}
		if cfg.AccountTenMinuteLimit > 0 && limit.TenMinute.Requests >= cfg.AccountTenMinuteLimit {
			limit.TenMinute = openAIStateBudgetWindow{StartedAt: now}
		}
		// Preserve request pacing and reduced concurrency after a recent 429.
		limit.UpdatedAt = now
	}
	for key := range s.rows {
		if accounts[key.accountID] {
			s.dirtyRuntime[key] = true
		}
	}
	for _, key := range targets {
		entry := s.rows[key]
		s.queue <- openAIStateKeeperJob{accountID: key.accountID, model: key.model, revision: cfg.Revision, source: "manual", scopeID: entry.scopeID}
		entry.row.Queued = true
		entry.row.AutoRetryPending, entry.row.NextRetryAt, entry.row.RetryReason = false, nil, ""
		entry.row.RoundSource = "manual"
		entry.row.NextAttemptAt = stateKeeperNextAttempt(cfg.OpenAIStateKeeperSettings, entry)
		entry.row.Message = "已手动解除本地冷却，等待重新采集"
	}
	return len(targets), nil
}

// The caller holds s.mu so a configuration change cannot enqueue an old plan.
func (s *OpenAIStateKeeperService) enqueueLocked(cfg *openAIStateKeeperConfig, ids []int64) error {
	return s.enqueueSourceLocked(cfg, ids, "timer")
}

func (s *OpenAIStateKeeperService) enqueueSourceLocked(cfg *openAIStateKeeperConfig, ids []int64, source string, selected ...string) error {
	if source == "response" && (!cfg.InjectionEnabled || !cfg.ResponseRefreshEnabled) {
		return nil
	}
	models := cfg.modelNames()
	if len(selected) > 0 && selected[0] != "" {
		models = selected
	}
	for _, id := range ids {
		for _, model := range models {
			key := openAIStateKey{id, model}
			entry := s.rows[key]
			if entry == nil || entry.scopeLoading || entry.row.AccountUnavailable || (source != "manual" && entry.row.Paused) || (source == "degradation_scan" && !s.qualityEligibleLocked(entry)) {
				continue
			}
			if entry.row.Queued || entry.row.Collecting || s.activeCancels[key] != nil {
				continue
			}
			if source != "automatic_retry" && source != "manual" && entry.row.AutoRetryPending {
				continue
			}
			select {
			case s.queue <- openAIStateKeeperJob{accountID: id, model: model, revision: cfg.Revision, source: source, scopeID: entry.scopeID}:
				entry.row.Queued = true
			default:
				return errors.New("采集队列已满，请稍后重试")
			}
		}
	}
	return nil
}

func (s *OpenAIStateKeeperService) scheduleDue(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.config.Load()
	if s.ctx.Err() != nil || !cfg.Enabled {
		return
	}
	for key, entry := range s.rows {
		r := entry.row
		if r.AutoRetryPending {
			if !r.Paused && !r.Queued && !r.Collecting && r.NextRetryAt != nil && !r.NextRetryAt.After(now) {
				_ = s.enqueueSourceLocked(cfg, []int64{key.accountID}, "automatic_retry", key.model)
			}
			continue
		}
		if !cfg.AutoRefresh || cfg.AutoCollectIntervalSeconds <= 0 {
			continue
		}
		if !r.Queued && !r.Collecting && (r.NextAttemptAt == nil || !r.NextAttemptAt.After(now)) {
			_ = s.enqueueSourceLocked(cfg, []int64{key.accountID}, "timer", key.model)
		}
	}
}

func stateKeeperNextAttempt(q OpenAIStateKeeperSettings, entry *openAIKeptState) *time.Time {
	if entry.row.AutoRetryPending && !entry.row.Paused && !entry.row.AccountUnavailable {
		return entry.row.NextRetryAt
	}
	if entry.row.AccountUnavailable || entry.row.Paused || entry.lastFinishedAt.IsZero() || !q.AutoRefresh || q.AutoCollectIntervalSeconds <= 0 {
		return nil
	}
	next := entry.lastFinishedAt.Add(time.Duration(q.AutoCollectIntervalSeconds) * time.Second)
	return &next
}

func (s *OpenAIStateKeeperService) scheduler() {
	defer s.wg.Done()
	reload := func() {
		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		s.reload(ctx)
		cancel()
		syncCtx, syncCancel := context.WithTimeout(s.ctx, 5*time.Second)
		defer syncCancel()
		_ = s.SyncSelection(syncCtx)
	}
	reload()
	// Restoration is background work. Requests never wait on file or DB reads.
	restoreCtx, restoreCancel := context.WithTimeout(s.ctx, 15*time.Second)
	s.restoreStateFiles(restoreCtx)
	s.syncQuality(restoreCtx)
	restoreCancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastReload := time.Now()
	lastQuality := time.Now()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			if now.Sub(lastReload) >= 30*time.Second {
				reload()
				lastReload = now
			}
			if now.Sub(lastQuality) >= 5*time.Second {
				ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
				s.syncQuality(ctx)
				cancel()
				lastQuality = now
			}
			s.scheduleDue(now)
			s.scheduleDegradationScan(now)
		}
	}
}

func (s *OpenAIStateKeeperService) startWorkers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.growWorkersLocked(s.config.Load().Concurrency)
	if !s.eventsStarted {
		s.eventsStarted = true
		s.wg.Add(1)
		go s.observationWorker()
	}
}

func (s *OpenAIStateKeeperService) growWorkersLocked(concurrency int) {
	if s.ctx.Err() != nil {
		return
	}
	for s.workerCount < concurrency {
		s.workerCount++
		s.wg.Add(1)
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
