package repository

import (
	"context"
	"testing"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestAccountQualityFilterAppliesBeforeCountAndPagination(t *testing.T) {
	for _, status := range []string{"degraded", "normal", "pending"} {
		t.Run(status, func(t *testing.T) {
			var queries []string
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(_, actual string) error {
				queries = append(queries, actual)
				return nil
			})))
			require.NoError(t, err)
			defer db.Close()
			client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
			defer client.Close()
			repo := newAccountRepositoryWithSQL(client, db, nil)
			mock.ExpectQuery("count").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(31))
			mock.ExpectQuery("page").WillReturnRows(sqlmock.NewRows([]string{"id"}))
			policy := service.AccountQualitySettings{Enabled: true, Revision: "current", QuestionEnabled: true, DegradationConditions: []string{"question"}}
			_, page, err := repo.ListWithQualityFilter(context.Background(), pagination.PaginationParams{Page: 2, PageSize: 10}, "", "oauth", "active", "match", 7, "training_off", status, policy)
			require.NoError(t, err)
			require.Equal(t, int64(31), page.Total)
			require.Len(t, queries, 2)
			for _, query := range queries {
				require.Contains(t, query, `s.account_id = "accounts"."id"`)
				require.Contains(t, query, "s.payload->>'revision'")
				require.Contains(t, query, `"accounts"."platform" =`)
				require.Contains(t, query, `"accounts"."type" =`)
				require.Contains(t, query, `"account_groups"`)
				require.Contains(t, query, `"accounts"."deleted_at" IS NULL`)
				if status == "pending" {
					require.Contains(t, query, "NOT EXISTS")
				}
			}
			require.NotContains(t, queries[0], "LIMIT")
			require.Contains(t, queries[1], "LIMIT")
			require.Contains(t, queries[1], "OFFSET")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
