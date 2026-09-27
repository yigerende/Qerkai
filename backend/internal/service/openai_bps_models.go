package service

import (
	"context"
	"encoding/json"

	"github.com/Wei-Shaw/sub2api/internal/pkg/basispoints"
)

func (s *OpenAIGatewayService) BPSCatalogEnabled(ctx context.Context, group *Group) bool {
	return s != nil && group != nil && (group.Platform == PlatformOpenAI || group.Platform == PlatformComposite) && s.settingService.bpsSettings(ctx).groupMatches(group.ID)
}

// BPSCatalogModelIDs never adds models that are unavailable to this group's
// schedulable OAuth accounts. Ordinary API-key catalogs stay untouched.
func (s *OpenAIGatewayService) BPSCatalogModelIDs(ctx context.Context, group *Group) []string {
	if s == nil {
		return nil
	}
	return s.bpsCatalogModelIDs(ctx, group, s.settingService.bpsSettings(ctx))
}

func (s *OpenAIGatewayService) bpsCatalogModelIDs(ctx context.Context, group *Group, cfg *cachedOpenAIBPS) []string {
	if group == nil || (group.Platform != PlatformOpenAI && group.Platform != PlatformComposite) || !cfg.groupMatches(group.ID) || s.accountRepo == nil {
		return nil
	}
	accounts, err := s.accountRepo.ListSchedulableByGroupID(ctx, group.ID)
	if err != nil {
		return nil
	}
	var models []string
	for _, model := range cfg.settings.Models {
		for _, account := range accounts {
			if account.IsOpenAIOAuth() && account.IsModelSupported(model) {
				models = append(models, model)
				break
			}
		}
	}
	return models
}

// ApplyBPSModelCatalog runs last, after all ordinary Codex capability defaults.
// Canonical capacities are taken from the same catalog (or Qerkai's canonical
// catalog builder), then the CPA interceptor enforces its precise limitations.
func (s *OpenAIGatewayService) ApplyBPSModelCatalog(ctx context.Context, group *Group, body []byte) ([]byte, error) {
	if s == nil {
		return body, nil
	}
	snapshot := s.settingService.bpsSettings(ctx)
	models := s.bpsCatalogModelIDs(ctx, group, snapshot)
	if len(models) == 0 {
		return body, nil
	}
	cfg := snapshot.settings.Config
	if group.CustomModelsListEnabled() {
		allowed := make(map[string]bool)
		for _, model := range group.ModelsListConfig.Models {
			allowed[model] = true
		}
		filtered := models[:0]
		for _, model := range models {
			if allowed[model] {
				filtered = append(filtered, model)
			}
		}
		models = filtered
	}
	if len(models) == 0 {
		return body, nil
	}
	cfg.Models = models
	cfg.ModelMappings = make(map[string]string, len(models))
	for _, model := range models {
		cfg.ModelMappings[model], _ = snapshot.settings.Config.UpstreamModelFor(model)
	}
	var catalog map[string]json.RawMessage
	if err := json.Unmarshal(body, &catalog); err != nil {
		return nil, err
	}
	var entries []map[string]json.RawMessage
	if err := json.Unmarshal(catalog["models"], &entries); err != nil {
		return nil, err
	}
	bySlug := make(map[string]map[string]json.RawMessage)
	for _, entry := range entries {
		var slug string
		_ = json.Unmarshal(entry["slug"], &slug)
		bySlug[slug] = entry
	}
	hidden := map[string]bool{}
	for _, model := range models {
		canonical := cfg.ModelMappings[model]
		if bySlug[canonical] == nil {
			raw, err := buildCodexModelsManifestForAccounts(PlatformOpenAI, []string{canonical}, nil, nil, true)
			if err != nil {
				return nil, err
			}
			var fallback struct {
				Models []map[string]json.RawMessage `json:"models"`
			}
			if err := json.Unmarshal(raw, &fallback); err != nil {
				return nil, err
			}
			if len(fallback.Models) > 0 {
				bySlug[canonical] = fallback.Models[0]
				entries = append(entries, fallback.Models[0])
				hidden[canonical] = canonical != model
			}
		}
		if bySlug[model] == nil && bySlug[canonical] != nil {
			entry := make(map[string]json.RawMessage)
			for k, v := range bySlug[canonical] {
				entry[k] = v
			}
			entry["slug"], _ = json.Marshal(model)
			entry["display_name"], _ = json.Marshal(model)
			bySlug[model] = entry
			entries = append(entries, entry)
		}
	}
	catalog["models"], _ = json.Marshal(entries)
	prepared, _ := json.Marshal(catalog)
	out, err := basispoints.Catalog(cfg, prepared)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(out, &catalog); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(catalog["models"], &entries); err != nil {
		return nil, err
	}
	filtered := entries[:0]
	for _, entry := range entries {
		var slug string
		_ = json.Unmarshal(entry["slug"], &slug)
		_, requested := cfg.ModelMappings[slug]
		if hidden[slug] && !requested {
			continue
		}
		if _, matched := cfg.ModelMappings[slug]; matched {
			entry["tool_mode"] = json.RawMessage("null")
			entry["use_responses_lite"] = json.RawMessage("false")
			entry["input_modalities"] = json.RawMessage(`["text","image"]`)
			entry["default_reasoning_level"] = json.RawMessage(`"medium"`)
			levels := []map[string]string{}
			for _, effort := range []string{"low", "medium", "high", "xhigh", "max", "ultra"} {
				levels = append(levels, map[string]string{"effort": effort, "description": effort})
			}
			entry["supported_reasoning_levels"], _ = json.Marshal(levels)
		}
		filtered = append(filtered, entry)
	}
	catalog["models"], _ = json.Marshal(filtered)
	return json.Marshal(catalog)
}
