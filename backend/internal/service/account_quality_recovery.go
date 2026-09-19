package service

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

const QualitySchedulingPausedExtraKey = "quality_scheduling_paused"
const accountQualityRecovery = 3

func (a *Account) IsQualitySchedulingPaused() bool {
	if a == nil || a.Platform != PlatformOpenAI {
		return false
	}
	paused, _ := a.Extra[QualitySchedulingPausedExtraKey].(bool)
	return paused
}

// The repository derives the flag from committed policy/results, so an older
// detector cannot overwrite a newer decision. Manual scheduling is independent.
func (s *AccountQualityService) syncQualityScheduling(ctx context.Context, _ AccountQualitySettings) error {
	if s.tests == nil || s.tests.accountRepo == nil {
		return nil
	}
	if repo, ok := s.tests.accountRepo.(interface{ SyncQualityScheduling(context.Context) error }); ok {
		return repo.SyncQualityScheduling(ctx)
	}
	return nil
}

func (s *AccountQualityService) notifyQualityCollection(id int64) {
	if s == nil || s.recoveryWake == nil {
		return
	}
	s.recoveryMu.Lock()
	if s.collectedAccounts == nil {
		s.collectedAccounts = make(map[int64]bool)
	}
	s.collectedAccounts[id] = true
	s.recoveryMu.Unlock()
	select {
	case s.recoveryWake <- struct{}{}:
	default:
	}
}

func (s *AccountQualityService) qualityRecoveryLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		case <-s.recoveryWake:
		}
		if err := s.runQualityRecovery(s.ctx); err != nil && s.ctx.Err() == nil {
			slog.Warn("account quality recovery failed", "error", err)
		}
	}
}

func (s *AccountQualityService) runQualityRecovery(ctx context.Context) error {
	q, err := s.Settings(ctx)
	if err != nil {
		return err
	}
	if err = s.syncQualityScheduling(ctx, q); err != nil {
		return err
	}
	if !q.Enabled || !q.PauseOnDegradation {
		s.recoveryMu.Lock()
		s.collectedAccounts = nil
		s.recoveryMu.Unlock()
		return nil
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var locked bool
	err = conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", accountQualityLockID+2).Scan(&locked)
	if err != nil || !locked {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, e := conn.ExecContext(releaseCtx, "SELECT pg_advisory_unlock($1)", accountQualityLockID+2); e != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	s.recoveryMu.Lock()
	collected := s.collectedAccounts
	s.collectedAccounts = nil
	s.recoveryMu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT s.payload FROM account_quality_states s JOIN accounts a ON a.id=s.account_id
 WHERE a.deleted_at IS NULL AND a.platform='openai' AND s.payload->'scheduling'->>'paused'='true'
 ORDER BY s.updated_at,a.id`)
	if err != nil {
		return err
	}
	var jobs []AccountQualityResult
	for rows.Next() {
		var raw []byte
		var v AccountQualityResult
		if err = rows.Scan(&raw); err != nil {
			break
		}
		if err = json.Unmarshal(raw, &v); err != nil {
			break
		}
		if v.Revision != q.Revision {
			v = AccountQualityResult{AccountID: v.AccountID, Revision: q.Revision, Scheduling: v.Scheduling}
			v.Scheduling.Successes, v.Scheduling.NextAt = 0, nil
		}
		if !collected[v.AccountID] && v.Scheduling.NextAt != nil && v.Scheduling.NextAt.After(time.Now()) {
			continue
		}
		jobs = append(jobs, v)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	queue := make(chan AccountQualityResult)
	var wg sync.WaitGroup
	for i := 0; i < min(q.Concurrency, len(jobs)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for v := range queue {
				if e := s.probeQualityRecovery(ctx, q, v); e != nil && ctx.Err() == nil {
					slog.Warn("account quality recovery probe failed", "account_id", v.AccountID, "error", e)
				}
			}
		}()
	}
send:
	for _, v := range jobs {
		select {
		case queue <- v:
		case <-ctx.Done():
			break send
		}
	}
	close(queue)
	wg.Wait()
	return ctx.Err()
}

func qualityRecoveryModels(q AccountQualitySettings) []string {
	models := []string{}
	for _, kind := range q.overallConditions() {
		model := q.Model
		if kind == "model" {
			model = q.ModelAuditModel
		}
		if len(models) == 0 || models[0] != model {
			models = append(models, model)
		}
	}
	return models
}

func (s *AccountQualityService) qualityRecoveryVersions(ctx context.Context, q AccountQualitySettings, id int64) (map[string]string, error) {
	a, err := s.tests.accountRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if a == nil || !a.IsActive() || a.Platform != PlatformOpenAI || (a.AutoPauseOnExpired && a.ExpiresAt != nil && !a.ExpiresAt.After(time.Now())) {
		return nil, errors.New("账号不可用，等待账号恢复后复检")
	}
	versions := map[string]string{"credentials": stateKeeperCredentialStamp(a)}
	keeper := s.stateKeeper.Load()
	for _, model := range qualityRecoveryModels(q) {
		versions["model:"+model] = ""
		if ticket := keeper.prepareQualityState(a, model, http.Header{}); ticket != nil {
			versions["config"] = ticket.config.Revision
			versions["model:"+model] = ticket.poolVersion()
		}
	}
	return versions, nil
}

func (s *AccountQualityService) qualityRecoveryStateCurrent(ctx context.Context, q AccountQualitySettings, v AccountQualityResult) bool {
	versions, err := s.qualityRecoveryVersions(ctx, q, v.AccountID)
	return err == nil && reflect.DeepEqual(versions, v.Scheduling.StateVersions)
}

func (s *AccountQualityService) probeQualityRecovery(ctx context.Context, q AccountQualitySettings, v AccountQualityResult) error {
	current, err := s.Settings(ctx)
	if err != nil || current.Revision != q.Revision || !current.Enabled || !current.PauseOnDegradation {
		return err
	}
	versions, err := s.qualityRecoveryVersions(ctx, q, v.AccountID)
	if !reflect.DeepEqual(versions, v.Scheduling.StateVersions) {
		v.Scheduling.Successes = 0
	}
	v.Scheduling.StateVersions, v.Scheduling.Error = versions, ""
	if err != nil {
		v.Scheduling.Error = err.Error()
	} else {
		s.reconcileCollectedModelState(q, &v)
		// Each sample is evaluated independently; admission has its own persisted
		// recovery streak so old user logs cannot release a paused account.
		samplePolicy := q
		samplePolicy.FailureLimit, samplePolicy.RecoveryLimit = 1, 1
		observers := make(map[string]*upstreamResponseModelObserver)
		for _, kind := range q.overallConditions() {
			if kind != "question" {
				continue
			}
			question := q.ActiveQuestions()[0]
			for _, item := range q.ActiveQuestions() {
				if item.ID == v.Question.NextQuestionID {
					question = item
				}
			}
			observer := &upstreamResponseModelObserver{}
			started := time.Now().UTC()
			answer, duration, e := s.testAnswer(ctx, v.AccountID, q, question, observer)
			applyQualityAnswer(&v.Question, samplePolicy, question, answer, duration, e, started, time.Now().UTC())
			if e != nil {
				v.Scheduling.Error = e.Error()
			} else {
				observers[q.Model] = observer
			}
		}
		for _, kind := range q.overallConditions() {
			if kind != "model" {
				continue
			}
			observer := observers[q.ModelAuditModel]
			var modelErr error
			if observer == nil && v.Scheduling.Error == "" {
				observer = &upstreamResponseModelObserver{}
				modelPolicy := q
				modelPolicy.Model = q.ModelAuditModel
				_, _, modelErr = s.testAnswer(ctx, v.AccountID, modelPolicy, QualityQuestion{Prompt: "Reply with the number 21."}, observer)
			}
			now, next := time.Now().UTC(), time.Now().UTC().Add(time.Duration(q.ModelAuditIntervalSeconds)*time.Second)
			v.Model.CheckedAt, v.Model.NextAt = &now, &next
			if modelErr != nil || observer == nil || observer.Model() == "" {
				v.Model.Status, v.Model.Error = "error", "复检未取得有效的上游模型名"
				v.Scheduling.Error = v.Model.Error
			} else {
				sent, response := strings.ToLower(q.ModelAuditModel), strings.ToLower(observer.Model())
				variant := sent != response && qualityModelSuffix.ReplaceAllString(sent, "") == qualityModelSuffix.ReplaceAllString(response, "")
				applyQualityVerdict(&v.Model.QualityVerdict, (sent == response || variant) && !observer.Conflict(), samplePolicy, now)
				if variant && !observer.Conflict() {
					v.Model.Status = "variant"
				}
				v.Model.SentModel, v.Model.ResponseModel = q.ModelAuditModel, observer.Model()
				v.Model.NoNewSamples, v.Model.StateValidationPending = false, false
			}
		}
	}
	if v.Scheduling.Error == "" && evaluateQualityOverall(q, v).Status == "normal" {
		v.Scheduling.Successes++
	} else {
		v.Scheduling.Successes = 0
	}
	v.Scheduling.Paused = v.Scheduling.Successes < q.RecoveryLimit
	next := time.Now().UTC().Add(time.Duration(q.RetrySeconds) * time.Second)
	v.Scheduling.NextAt = &next
	return s.saveResult(ctx, q, v, accountQualityRecovery)
}
