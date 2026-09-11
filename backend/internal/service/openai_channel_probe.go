package service

import (
	"regexp"
	"strings"

	"github.com/tidwall/gjson"
)

const sub2APIChannelProbePromptPrefix = "Calculate and respond with ONLY the number, nothing else.\n\nQ: 3 + 5 = ?\nA: 8\n\nQ: 12 - 7 = ?\nA: 5\n\nQ: "

var sub2APIChannelProbePromptPattern = regexp.MustCompile(`^[0-9]+ [+-] [0-9]+ = \?\nA:$`)

// IsSub2APIChannelProbeRequest recognizes the fixed arithmetic probe emitted by
// Sub2API channel monitoring. It requires the complete wire shape so ordinary
// non-streaming requests are not routed around the WS policy.
func IsSub2APIChannelProbeRequest(body []byte) bool {
	if len(body) == 0 || !gjson.ValidBytes(body) || gjson.GetBytes(body, "stream").Bool() {
		return false
	}
	if gjson.GetBytes(body, "max_tokens").Int() != 50 && gjson.GetBytes(body, "max_output_tokens").Int() != 50 {
		return false
	}
	if content := gjson.GetBytes(body, "messages.0.content"); content.Type == gjson.String {
		return isSub2APIChannelProbePrompt(content.String()) &&
			gjson.GetBytes(body, "messages.#").Int() == 1 &&
			strings.EqualFold(strings.TrimSpace(gjson.GetBytes(body, "messages.0.role").String()), "user")
	}
	if input := gjson.GetBytes(body, "input"); input.Type == gjson.String {
		return isSub2APIChannelProbePrompt(input.String()) &&
			gjson.GetBytes(body, "instructions").String() == "You are a channel health-check endpoint. Answer the arithmetic challenge exactly and briefly."
	}
	return false
}

func isSub2APIChannelProbePrompt(prompt string) bool {
	if !strings.HasPrefix(prompt, sub2APIChannelProbePromptPrefix) {
		return false
	}
	return sub2APIChannelProbePromptPattern.MatchString(strings.TrimPrefix(prompt, sub2APIChannelProbePromptPrefix))
}
