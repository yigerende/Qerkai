package service

import "github.com/gin-gonic/gin"

// Only a committed 502/503 business retry can bypass connection reuse. Account
// selection, retry budgets, headers and the connection pool remain authoritative.
func (state *openAIWSBusinessRetryState) forceNewConnectionForBusinessRetry(c *gin.Context) bool {
	if state == nil || !state.businessFreshConnectionPending || state.outputStarted || c == nil || c.Request == nil || c.Request.Context().Err() != nil {
		return false
	}
	config := OpenAIUpstream5xxRetrySettings()
	return config.Enabled && config.ForceNewConnection && ForceUpstreamWSEnabledForGroup(getOpenAIGroupIDFromContext(c))
}
