package service

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
)

const SettingKeyOpenAIUpstream5xxRetryRules = "openai_upstream_5xx_retry_rules"

type OpenAIUpstream5xxRetryRule struct {
	ID         string   `json:"id"`
	Name       string   `json:"name"`
	Enabled    bool     `json:"enabled"`
	StatusCode int      `json:"status_code"`
	MatchMode  string   `json:"match_mode"`
	Keywords   []string `json:"keywords"`
}

func DefaultOpenAIUpstream5xxRetryRules() []OpenAIUpstream5xxRetryRule {
	return []OpenAIUpstream5xxRetryRule{
		{ID: "processing_error", Name: "502 processing error", Enabled: true, StatusCode: 502, MatchMode: "all", Keywords: []string{"An error occurred while processing your request", "You can retry your request"}},
		{ID: "server_overloaded", Name: "503 server overloaded", Enabled: true, StatusCode: 503, MatchMode: "all", Keywords: []string{"Our servers are currently overloaded", "Please try again later"}},
	}
}

// A missing setting keeps the old defaults; an explicit empty list disables matching.
func NormalizeOpenAIUpstream5xxRetryRules(rules []OpenAIUpstream5xxRetryRule) ([]OpenAIUpstream5xxRetryRule, error) {
	if rules == nil {
		rules = DefaultOpenAIUpstream5xxRetryRules()
	}
	if len(rules) > 32 {
		return nil, fmt.Errorf("at most 32 retry rules are allowed")
	}
	result := make([]OpenAIUpstream5xxRetryRule, 0, len(rules))
	seen := make(map[string]bool, len(rules))
	for _, rule := range rules {
		rule.ID, rule.Name = strings.TrimSpace(rule.ID), strings.TrimSpace(rule.Name)
		if rule.ID == "" || len(rule.ID) > 64 || seen[rule.ID] || rule.Name == "" || len([]rune(rule.Name)) > 80 {
			return nil, fmt.Errorf("retry rules require unique IDs (1-64 bytes) and names (1-80 characters)")
		}
		seen[rule.ID] = true
		if (rule.StatusCode != 502 && rule.StatusCode != 503) || (rule.MatchMode != "all" && rule.MatchMode != "any") {
			return nil, fmt.Errorf("rule %s requires status 502/503 and match mode all/any", rule.ID)
		}
		if len(rule.Keywords) == 0 || len(rule.Keywords) > 16 {
			return nil, fmt.Errorf("rule %s requires 1-16 keywords", rule.ID)
		}
		keywords := make([]string, 0, len(rule.Keywords))
		for _, keyword := range rule.Keywords {
			keyword = strings.Join(strings.Fields(keyword), " ")
			if keyword == "" || len([]rune(keyword)) > 256 {
				return nil, fmt.Errorf("rule %s requires keywords of 1-256 characters", rule.ID)
			}
			keywords = append(keywords, keyword)
		}
		rule.Keywords = keywords
		result = append(result, rule)
	}
	return result, nil
}

func parseOpenAIUpstream5xxRetryRules(raw string) []OpenAIUpstream5xxRetryRule {
	if strings.TrimSpace(raw) == "" {
		return DefaultOpenAIUpstream5xxRetryRules()
	}
	var rules []OpenAIUpstream5xxRetryRule
	if json.Unmarshal([]byte(raw), &rules) != nil || rules == nil {
		return []OpenAIUpstream5xxRetryRule{}
	}
	normalized, err := NormalizeOpenAIUpstream5xxRetryRules(rules)
	if err != nil {
		return []OpenAIUpstream5xxRetryRule{}
	}
	return normalized
}

func hasEnabledOpenAIUpstream5xxRetryRule(rules []OpenAIUpstream5xxRetryRule) bool {
	if rules == nil {
		return true
	}
	for _, rule := range rules {
		if rule.Enabled {
			return true
		}
	}
	return false
}

type OpenAIUpstream5xxRetryMatch struct {
	Matched    bool   `json:"matched"`
	RuleID     string `json:"rule_id,omitempty"`
	RuleName   string `json:"rule_name,omitempty"`
	StatusCode int    `json:"status_code,omitempty"`
	Reason     string `json:"reason"`
}

// Preview and forwarding use the same event exclusions and first-match rule order.
func MatchOpenAIUpstream5xxRetryRules(payload []byte, rules []OpenAIUpstream5xxRetryRule) OpenAIUpstream5xxRetryMatch {
	result := OpenAIUpstream5xxRetryMatch{Reason: "no_match"}
	if !gjson.ValidBytes(payload) {
		result.Reason = "invalid_json"
		return result
	}
	event := gjson.GetBytes(payload, "type").String()
	if event != "error" && event != "response.failed" {
		result.Reason = "unsupported_event"
		return result
	}
	message := strings.ToLower(strings.Join(strings.Fields(extractOpenAISSEErrorMessage(payload)), " "))
	if isOpenAIContextWindowError(message, payload) || isOpenAIWSErrorEventRateLimited(payload) {
		result.Reason = "excluded_error"
		return result
	}
	if hit, _, _ := detectOpenAICyberPolicy(payload); hit {
		result.Reason = "excluded_error"
		return result
	}
	for _, path := range []string{"response.error.status_code", "response.error.status", "error.status_code", "error.status", "status_code", "status"} {
		if status := gjson.GetBytes(payload, path).Int(); status > 0 && status != 502 && status != 503 {
			result.Reason = "excluded_status"
			return result
		}
	}
	if rules == nil {
		rules = DefaultOpenAIUpstream5xxRetryRules()
	}
	for _, rule := range rules {
		if !rule.Enabled || len(rule.Keywords) == 0 {
			continue
		}
		hits := 0
		for _, keyword := range rule.Keywords {
			keyword = strings.ToLower(strings.Join(strings.Fields(keyword), " "))
			if keyword != "" && strings.Contains(message, keyword) {
				hits++
			}
		}
		if (rule.MatchMode == "all" && hits == len(rule.Keywords)) || (rule.MatchMode == "any" && hits > 0) {
			return OpenAIUpstream5xxRetryMatch{Matched: true, RuleID: rule.ID, RuleName: rule.Name, StatusCode: rule.StatusCode, Reason: "matched"}
		}
	}
	return result
}
