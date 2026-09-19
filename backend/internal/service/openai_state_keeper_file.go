package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// One encrypted file per account and model. Status polling returns summaries; explicit
// administrator file inspection decrypts the saved record on demand.
type openAIStateFileStore struct {
	dir    string
	cipher SecretEncryptor
}

type openAIStateFileRecord struct {
	AccountID       int64     `json:"account_id"`
	Model           string    `json:"model"`
	ProxyID         int64     `json:"proxy_id"`
	Endpoint        string    `json:"endpoint"`
	HeaderName      string    `json:"header_name"`
	CredentialStamp string    `json:"credential_stamp"`
	IdentityStamp   string    `json:"identity_stamp,omitempty"`
	Value           string    `json:"turn_state"`
	CollectedAt     time.Time `json:"collected_at"`
	Version         string    `json:"version,omitempty"`
}

type OpenAIStateKeeperFileDetail struct {
	FileName string                `json:"file_name"`
	Content  openAIStateFileRecord `json:"content"`
}

func (s *OpenAIStateKeeperService) FileDetail(accountID int64, models ...string) (*OpenAIStateKeeperFileDetail, error) {
	// Keep the file and configuration consistent while reading; status polling
	// remains available because disk I/O does not hold the row mutex.
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	s.mu.RLock()
	cfg := s.config.Load()
	key := s.key(accountID, models...)
	entry := s.rows[key]
	saved := entry != nil && entry.row.StateFileSaved
	s.mu.RUnlock()
	if !saved || cfg == nil || s.files == nil {
		return nil, nil
	}
	record, err := s.files.load(accountID, cfg.forModel(key.model))
	if err != nil || record == nil {
		return nil, err
	}
	path := s.files.path(accountID, key.model)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		path = s.files.path(accountID)
	}
	return &OpenAIStateKeeperFileDetail{FileName: filepath.Base(path), Content: *record}, nil
}

func newOpenAIStateFileStore(cfg *config.Config) *openAIStateFileStore {
	dir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dir == "" {
		dir = "data"
	}
	s := &openAIStateFileStore{dir: filepath.Join(dir, "openai-state-keeper")}
	if cfg != nil && strings.TrimSpace(cfg.JWT.Secret) != "" {
		// Reuse the project's AES-GCM implementation with a separate purpose key.
		s.cipher = &liveAttestationAES{key: sha256.Sum256([]byte("sub2api/state-keeper/v1\x00" + cfg.JWT.Secret))}
	}
	return s
}

func stateKeeperFileStem(id int64, models ...string) string {
	if len(models) == 0 || models[0] == "" {
		return fmt.Sprintf("account-%d", id)
	}
	sum := sha256.Sum256([]byte(models[0]))
	return fmt.Sprintf("account-%d-%x", id, sum[:])
}

func (s *openAIStateFileStore) path(id int64, models ...string) string {
	return filepath.Join(s.dir, stateKeeperFileStem(id, models...)+".state")
}

func (s *openAIStateFileStore) save(record openAIStateFileRecord) error {
	if s.cipher == nil || record.AccountID <= 0 || record.HeaderName != openAICodexTurnStateHeader || !validCollectedState(record.Value) {
		return errors.New("invalid state file configuration or record")
	}
	plain, err := json.Marshal(record)
	if err != nil {
		return err
	}
	encrypted, err := s.cipher.Encrypt(string(plain))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, ".state-*.tmp")
	if err != nil {
		return err
	}
	defer func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }()
	if _, err = tmp.WriteString(encrypted); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), s.path(record.AccountID, record.Model))
}

func (s *openAIStateFileStore) load(id int64, q OpenAIStateKeeperSettings) (*openAIStateFileRecord, error) {
	if !q.Enabled || s.cipher == nil || id <= 0 {
		return nil, nil
	}
	f, err := os.Open(s.path(id, q.Model))
	if errors.Is(err, os.ErrNotExist) {
		f, err = os.Open(s.path(id))
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	encrypted, err := io.ReadAll(io.LimitReader(f, (128<<10)+1))
	if err != nil || len(encrypted) > 128<<10 {
		return nil, errors.New("cannot read state file")
	}
	plain, err := s.cipher.Decrypt(string(encrypted))
	if err != nil {
		return nil, errors.New("cannot decrypt state file")
	}
	var record openAIStateFileRecord
	if err := json.Unmarshal([]byte(plain), &record); err != nil {
		return nil, errors.New("invalid state file")
	}
	now := time.Now()
	if record.AccountID != id || record.Model != q.Model || !q.allowsProxy(record.ProxyID) || record.Endpoint != openAIStateKeeperCollectionURL || record.HeaderName != openAICodexTurnStateHeader || record.CollectedAt.After(now) || !validCollectedState(record.Value) || !q.allowsStateLength(len(record.Value)) {
		return nil, nil
	}
	return &record, nil
}

func (s *OpenAIStateKeeperService) restoreStateFiles(ctx context.Context, selectedIDs ...int64) {
	if s.files == nil {
		return
	}
	cfg := s.config.Load()
	if !cfg.Enabled {
		return
	}
	if selectedIDs == nil {
		selectedIDs = s.collectionAccountIDs()
	}
	for _, id := range selectedIDs {
		for _, model := range cfg.modelNames() {
			if ctx.Err() != nil || s.config.Load() != cfg {
				return
			}
			record, err := s.files.load(id, cfg.forModel(model))
			if err != nil {
				s.mu.Lock()
				if entry := s.entryLocked(id, model); s.config.Load() == cfg && entry != nil && entry.value == "" {
					entry.row.Message = "State 文件无法读取，需重新采集"
				}
				s.mu.Unlock()
				continue
			}
			if record == nil {
				continue
			}
			account, err := s.accounts.GetByID(ctx, id)
			// Keep a compatible saved State visible even when injection of old
			// credentials is suspended; the injection boundary enforces that setting.
			policy := cfg.OpenAIStateKeeperSettings
			policy.SuspendOldStateOnReauth = false
			if err != nil || !stateKeeperAccountEligible(account) || !stateKeeperStateMatchesAccount(policy, record.CredentialStamp, record.IdentityStamp, account) {
				continue
			}
			s.mu.Lock()
			entry := s.entryLocked(id, model)
			if s.config.Load() == cfg && entry != nil && entry.value == "" && !entry.row.Collecting && !entry.row.Queued {
				entry.value = record.Value
				entry.proxyID = record.ProxyID
				entry.version = record.Version
				if entry.version == "" {
					entry.version = record.CollectedAt.UTC().Format(time.RFC3339Nano)
				}
				entry.credentialStamp = record.CredentialStamp
				entry.identityStamp = record.IdentityStamp
				if entry.identityStamp == "" && entry.credentialStamp == stateKeeperCredentialStamp(account) {
					entry.identityStamp = stateKeeperIdentityStamp(account)
				}
				entry.row.Status = "ready"
				entry.row.HTTPStatus = http.StatusOK
				entry.row.TurnStateLength = len(record.Value)
				entry.row.HasCodexTurnState = true
				entry.row.HasDetails = true
				entry.row.StateFileSaved = true
				entry.row.CollectedAt = &record.CollectedAt
				entry.detail = &OpenAIStateKeeperDetail{
					AccountID: id, Model: record.Model, HTTPStatus: http.StatusOK,
					HeaderName: record.HeaderName, HeaderValue: record.Value,
					TurnStateLength: len(record.Value), CollectedAt: record.CollectedAt,
					SaveAllowed: true, StateFileSaved: true,
				}
				if entry.lastFinishedAt.IsZero() {
					entry.lastFinishedAt = record.CollectedAt
				}
				finished := entry.lastFinishedAt
				entry.row.LastCollectionAt = &finished
				entry.row.NextAttemptAt = stateKeeperNextAttempt(cfg.OpenAIStateKeeperSettings, entry)
				sum := sha256.Sum256([]byte(record.Value))
				entry.row.Fingerprint = fmt.Sprintf("%x", sum[:])
				entry.row.Message = "已从账号独立 State 文件恢复有效状态"
			}
			s.mu.Unlock()
		}
	}
}
