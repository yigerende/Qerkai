package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// One encrypted file per account. Only the collector/configuration worker does
// disk I/O; forwarding continues to read the in-memory copy.
type openAIStateFileStore struct {
	dir    string
	cipher SecretEncryptor
}

type openAIStateFileRecord struct {
	AccountID       int64     `json:"account_id"`
	Model           string    `json:"model"`
	ProxyID         int64     `json:"proxy_id"`
	TTLSeconds      int       `json:"ttl_seconds"`
	Endpoint        string    `json:"endpoint"`
	CredentialStamp string    `json:"credential_stamp"`
	Value           string    `json:"current_turn_state"`
	CollectedAt     time.Time `json:"collected_at"`
	ExpiresAt       time.Time `json:"expires_at"`
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

func (s *openAIStateFileStore) path(id int64) string {
	return filepath.Join(s.dir, fmt.Sprintf("account-%d.state", id))
}

func (s *openAIStateFileStore) save(record openAIStateFileRecord) error {
	if s.cipher == nil || record.AccountID <= 0 || !validCollectedState(record.Value) {
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
	return os.Rename(tmp.Name(), s.path(record.AccountID))
}

func (s *openAIStateFileStore) load(id int64, q OpenAIStateKeeperSettings) (*openAIStateFileRecord, error) {
	if !q.Enabled || s.cipher == nil || id <= 0 {
		return nil, nil
	}
	f, err := os.Open(s.path(id))
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
	if record.AccountID != id || record.Model != q.Model || record.ProxyID != q.ProxyID || record.TTLSeconds != q.TTLSeconds || record.Endpoint != openAIStateKeeperCollectionURL || !record.ExpiresAt.After(now) || record.CollectedAt.After(now) || record.ExpiresAt.Sub(record.CollectedAt) != time.Duration(q.TTLSeconds)*time.Second || !validCollectedState(record.Value) {
		return nil, nil
	}
	return &record, nil
}

func (s *OpenAIStateKeeperService) restoreStateFiles(ctx context.Context) {
	if s.files == nil {
		return
	}
	cfg := s.config.Load()
	if !cfg.Enabled {
		return
	}
	for _, id := range cfg.AccountIDs {
		if ctx.Err() != nil || s.config.Load() != cfg {
			return
		}
		record, err := s.files.load(id, cfg.OpenAIStateKeeperSettings)
		if err != nil {
			s.mu.Lock()
			if entry := s.rows[id]; s.config.Load() == cfg && entry != nil && entry.value == "" {
				entry.row.Message = "State 文件无法读取，需重新采集"
			}
			s.mu.Unlock()
			continue
		}
		if record == nil {
			continue
		}
		account, err := s.accounts.GetByID(ctx, id)
		if err != nil || !stateKeeperAccountEligible(account) || stateKeeperCredentialStamp(account) != record.CredentialStamp {
			continue
		}
		s.mu.Lock()
		entry := s.rows[id]
		if s.config.Load() == cfg && entry != nil && entry.value == "" && !entry.row.Collecting && !entry.row.Queued {
			entry.value = record.Value
			entry.credentialStamp = record.CredentialStamp
			entry.row.Status = "ready"
			entry.row.HTTPStatus = 292
			entry.row.StateFileSaved = true
			entry.row.CollectedAt = &record.CollectedAt
			entry.row.ExpiresAt = &record.ExpiresAt
			entry.lastFinishedAt = record.CollectedAt
			entry.row.NextAttemptAt = stateKeeperNextAttempt(cfg.OpenAIStateKeeperSettings, entry)
			sum := sha256.Sum256([]byte(record.Value))
			entry.row.Fingerprint = fmt.Sprintf("%x", sum[:6])
			entry.row.Message = "已从账号独立 State 文件恢复有效状态"
		}
		s.mu.Unlock()
	}
}
