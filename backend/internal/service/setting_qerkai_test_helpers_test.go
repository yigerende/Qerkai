//go:build unit

package service

// withQerkaiDefaultsForTest supplies the valid pool and retry settings that the
// admin form sends, while letting upstream tests focus on unrelated settings.
// Validation tests deliberately construct their own settings instead.
func withQerkaiDefaultsForTest(s *SystemSettings) *SystemSettings {
	d := defaultOpenAIWSPoolOptimizationSettings()
	s.OpenAIWSPrewarmIdlePerAccount = d.PrewarmIdle
	s.OpenAIWSStandbyIdlePerAccount = d.StandbyIdle
	s.OpenAIWSStandbyMaxPerAccount = d.StandbyMax
	s.OpenAIWSOptimizedQueuePerConn = d.QueuePerConn
	s.OpenAIWSOptimizedTargetUtilization = d.TargetUtilization
	s.OpenAIWSOptimizedIdleRecycleSeconds = d.IdleRecycleSeconds
	s.OpenAIWSOptimizedMaxAgeSeconds = d.MaxAgeSeconds
	s.OpenAIWSOptimizedHealthIntervalSeconds = d.HealthIntervalSeconds
	s.OpenAIWSOptimizedSessionTTLSeconds = d.SessionTTLSeconds
	s.OpenAIWSOptimizedSessionIdleSeconds = d.SessionIdleSeconds
	s.OpenAIWSOptimizedDialIntervalMS = d.DialIntervalMS
	s.OpenAIUpstream5xxRetryTotal = DefaultOpenAIUpstream5xxRetryTotal
	s.OpenAIUpstream5xxRetrySameAccount = DefaultOpenAIUpstream5xxRetrySameAccount
	s.OpenAIUpstream5xxRetryDelayMS = DefaultOpenAIUpstream5xxRetryDelayMS
	return s
}
