package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/stretchr/testify/require"
)

type qualityListRepo struct {
	AccountRepository
	status string
	policy AccountQualitySettings
	params pagination.PaginationParams
}

func (r *qualityListRepo) ListWithQualityFilter(_ context.Context, params pagination.PaginationParams, _, _, _, _ string, _ int64, _, status string, policy AccountQualitySettings) ([]Account, *pagination.PaginationResult, error) {
	r.status, r.policy, r.params = status, policy, params
	return []Account{{ID: 17}}, &pagination.PaginationResult{Total: 1}, nil
}

func TestAccountQualityListAndBulkScope(t *testing.T) {
	settings, policy := qualityTestSettings(t)
	repo := &qualityListRepo{}
	svc := &adminServiceImpl{accountRepo: repo, settingService: settings}
	for _, status := range []string{"degraded", "normal", "pending"} {
		rows, total, err := svc.ListAccounts(context.Background(), 2, 10, "", "", "", "", 0, "", "name", "asc", status)
		require.NoError(t, err)
		require.Equal(t, int64(1), total)
		require.Equal(t, int64(17), rows[0].ID)
		require.Equal(t, status, repo.status)
		require.Equal(t, policy.Revision, repo.policy.Revision)
		require.Equal(t, 2, repo.params.Page)
		ids, err := svc.resolveBulkUpdateTargetIDs(context.Background(), &BulkUpdateAccountFilters{QualityStatus: status})
		require.NoError(t, err)
		require.Equal(t, []int64{17}, ids)
		require.Equal(t, status, repo.status)
	}
	_, _, err := svc.ListAccounts(context.Background(), 1, 20, "", "", "", "", 0, "", "", "", "invalid")
	require.ErrorContains(t, err, "invalid account quality filter")
}

func TestAccountQualityPostgresFilterMatchesVerdicts(t *testing.T) {
	dsn := os.Getenv("QUALITY_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("isolated PostgreSQL URL not provided")
	}
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"127.0.0.1", "localhost"}, parsed.Hostname())
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now()
	verdicts := []QualityVerdict{
		{}, {Status: "normal"}, {Status: "normal", CheckedAt: &now},
		{Status: "variant", CheckedAt: &now}, {Status: "normal", Degraded: true, CheckedAt: &now},
		{Status: "degraded", Degraded: true, CheckedAt: &now}, {Status: "suspect", CheckedAt: &now},
		{Status: "error", Degraded: true, CheckedAt: &now}, {Status: "no_samples", CheckedAt: &now},
		{Status: "normal", Error: "failed probe", CheckedAt: &now},
	}
	for _, mode := range []string{"any", "all"} {
		for _, conditions := range [][]string{{"question", "model"}, {"question"}, {"model"}, {}} {
			for _, enabled := range []bool{true, false} {
				q := AccountQualitySettings{Enabled: enabled, Revision: "current", QuestionEnabled: true, ModelAuditEnabled: true, DegradationMode: mode, DegradationConditions: conditions}
				results := []AccountQualityResult{}
				for _, question := range verdicts {
					for _, model := range verdicts {
						for _, revision := range []string{"current", "old"} {
							results = append(results, AccountQualityResult{Revision: revision, Question: QualityQuestionResult{QualityVerdict: question}, Model: QualityModelResult{QualityVerdict: model}})
						}
					}
				}
				raw, err := json.Marshal(results)
				require.NoError(t, err)
				query := `SELECT CASE WHEN COALESCE(s.payload->>'revision','')<>$2 THEN 'pending' WHEN ` + AccountQualityStatusSQL(q, "degraded") + ` THEN 'degraded' WHEN ` + AccountQualityStatusSQL(q, "normal") + ` THEN 'normal' ELSE 'pending' END FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS s(payload,idx) ORDER BY idx`
				rows, err := db.QueryContext(ctx, query, string(raw), q.Revision)
				require.NoError(t, err)
				index := 0
				for rows.Next() {
					var actual string
					require.NoError(t, rows.Scan(&actual))
					require.Equal(t, evaluateQualityOverall(q, results[index]).Status, actual, "mode=%s conditions=%v enabled=%t result=%+v", mode, conditions, enabled, results[index])
					index++
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.Equal(t, len(results), index)
			}
		}
	}
}
