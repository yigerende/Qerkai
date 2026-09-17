//go:build unit

package repository

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestModelAuditBoundedQueryNoCountOrHydration(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	since := time.Now().UTC()
	input := service.ModelAuditInput{Model: "gpt-6-astra", Accounts: []service.ModelAuditAccount{{AccountID: 1, Since: since}, {AccountID: 2, Since: since}}}
	query, args := modelAuditQuery(input)
	require.Len(t, args, 5)
	require.Contains(t, query, "LIMIT 3")
	require.Contains(t, query, "account_id=wanted.account_id")
	require.Contains(t, query, "created_at>=wanted.since")
	for _, forbidden := range []string{"COUNT(", "OFFSET", "JOIN accounts", "JOIN users", "SELECT *"} {
		require.NotContains(t, query, forbidden)
	}
	rows := sqlmock.NewRows([]string{"id", "account_id", "created_at", "requested_model", "sent_model", "response_model", "upstream_model_mismatch"}).AddRow(9, 1, since, "alias", "gpt-6-astra", "other-model", true)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs("gpt-6-astra", int64(1), since, int64(2), since).WillReturnRows(rows)
	repo := newUsageLogRepositoryWithSQL(nil, db)
	out, err := repo.LatestModelAudit(context.Background(), input)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Len(t, out[0].Logs, 1)
	require.Empty(t, out[1].Logs)
	require.Equal(t, "alias", out[0].Logs[0].RequestedModel)
	require.True(t, *out[0].Logs[0].Mismatch)
	require.NoError(t, mock.ExpectationsWereMet())
}
func TestModelAuditValidationLimits(t *testing.T) {
	now := time.Now()
	input := service.ModelAuditInput{Model: "gpt-6-astra", Accounts: []service.ModelAuditAccount{{AccountID: 1, Since: now}}}
	require.NoError(t, input.Validate())
	input.Accounts = append(input.Accounts, input.Accounts[0])
	require.Error(t, input.Validate())
	input.Accounts = nil
	require.Error(t, input.Validate())
	for i := 1; i <= 11; i++ {
		input.Accounts = append(input.Accounts, service.ModelAuditAccount{AccountID: int64(i), Since: now})
	}
	require.Error(t, input.Validate())
	input.Accounts = input.Accounts[:10]
	require.NoError(t, input.Validate())
	input.Model = strings.Repeat("x", 201)
	require.Error(t, input.Validate())
}
