package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// The upstream System One listing change and Qerkai's BPS catalog must both
// survive the merge, including disabled, group-scoped and allowlisted requests.
func TestGatewayModels_CompositeBPSAndSystemOneScopes(t *testing.T) {
	const groupID int64 = 66
	for _, tc := range []struct {
		name       string
		enabled    bool
		groups     []int64
		allowlist  service.GroupModelAllowlist
		wantBPS    bool
		wantSystem bool
	}{
		{name: "enabled", enabled: true, wantBPS: true, wantSystem: true},
		{name: "disabled", wantSystem: true},
		{name: "other group", enabled: true, groups: []int64{67}, wantSystem: true},
		{name: "allowed alias", enabled: true, groups: []int64{groupID}, allowlist: service.GroupModelAllowlist{Enabled: true, Models: []string{"astra-bps", "jev-latest"}}, wantBPS: true, wantSystem: true},
		{name: "excluded alias", enabled: true, allowlist: service.GroupModelAllowlist{Enabled: true, Models: []string{"gpt-6-astra"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &gatewayModelsAccountRepoStub{byGroup: map[int64][]service.Account{
				groupID: {
					{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth},
					{ID: 2, Platform: service.PlatformTypeSafe, Type: service.AccountTypeAPIKey},
				},
			}}
			bps := service.DefaultOpenAIBPSSettings()
			bps.Enabled, bps.GroupIDs = tc.enabled, tc.groups
			bps.Models = []string{"astra-bps", "gpt-6-astra"}
			bps.ModelMappings = map[string]string{"astra-bps": "gpt-6-astra", "gpt-6-astra": "gpt-6-astra"}
			encoded, err := json.Marshal(bps)
			require.NoError(t, err)
			settings := service.NewSettingService(&contentModerationHandlerSettingRepo{values: map[string]string{
				service.SettingKeyOpenAIBPS: string(encoded),
			}}, nil)
			h := newGatewayModelsHandlerForTest(repo)
			h.openAIGatewayService = service.NewOpenAIGatewayService(
				repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil, nil, nil, nil, nil, settings, nil,
			)
			group := &service.Group{ID: groupID, Platform: service.PlatformComposite, ModelAllowlist: tc.allowlist}
			for _, codex := range []bool{false, true} {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
				c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{GroupID: &group.ID, Group: group})
				var models []string
				if codex {
					h.CodexModels(c)
					var got codexModelsResponseForTest
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
					models = codexModelSlugsForTest(got.Models)
				} else {
					h.Models(c)
					var got gatewayModelsResponseForTest
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
					models = modelIDsForTest(got.Data)
				}
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				if tc.wantBPS {
					require.Contains(t, models, "astra-bps", "codex=%v", codex)
				} else {
					require.NotContains(t, models, "astra-bps", "codex=%v", codex)
				}
				if tc.wantSystem && !codex {
					require.Contains(t, models, "jev-latest")
				} else {
					require.NotContains(t, models, "jev-latest")
				}
			}
		})
	}
}
