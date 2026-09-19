package service

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

func (s *openAIStateFileStore) proxyStatsPath() string {
	return filepath.Join(s.dir, "proxy-successes.json")
}

// Caller holds saveMu. This file is independent of account/model lifetimes.
// Failed reads never overwrite existing totals; pending increments are merged
// when storage recovers. Failed writes retain the dirty flag for the next flush.
func (s *OpenAIStateKeeperService) flushProxySuccessesLocked() {
	if s.files == nil {
		return
	}
	err := s.persistProxySuccessesLocked()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.proxyStatsError = ""
	if err != nil {
		s.proxyStatsError = "代理成功次数暂未完成持久化，正在自动重试，请检查数据目录权限或统计文件"
	}
}

func (s *OpenAIStateKeeperService) persistProxySuccessesLocked() error {
	if !s.proxyStatsLoaded {
		body, err := os.ReadFile(s.files.proxyStatsPath())
		saved := make(map[int64]int64)
		if err == nil {
			if err = json.Unmarshal(body, &saved); err != nil {
				return err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		for id, count := range saved {
			if id <= 0 || count < 0 {
				return errors.New("invalid proxy success totals")
			}
		}
		s.mu.Lock()
		for id, count := range saved {
			s.proxySuccesses[id] += count
		}
		s.mu.Unlock()
		s.proxyStatsLoaded = true
	}
	if !s.proxyStatsDirty {
		return nil
	}
	s.mu.RLock()
	snapshot := make(map[int64]int64, len(s.proxySuccesses))
	for id, count := range s.proxySuccesses {
		snapshot[id] = count
	}
	s.mu.RUnlock()
	if err := writeKeeperRuntime(s.files.proxyStatsPath(), snapshot); err != nil {
		return err
	}
	s.proxyStatsDirty = false
	return nil
}
