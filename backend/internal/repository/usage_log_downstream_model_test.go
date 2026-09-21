//go:build unit

package repository

import (
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUsageLogDownstreamModelRoundTrip(t *testing.T) {
	model := "gpt-6-astra"
	for _, value := range []*string{nil, &model} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		prepared := prepareUsageLogInsert(&service.UsageLog{DownstreamModel: value})
		values := append([]driver.Value{int64(1)}, anySliceToDriverValues(prepared.args)...)
		mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(strings.Split(usageLogSelectColumns, ", ")).AddRow(values...))
		log, err := scanUsageLog(db.QueryRow("SELECT " + usageLogSelectColumns + " FROM usage_logs"))
		require.NoError(t, err)
		require.Equal(t, value, log.DownstreamModel)
		require.NoError(t, mock.ExpectationsWereMet())
	}
}
