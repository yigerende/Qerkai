//go:build unit

package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccountQualityGroupSwitchValidationAndScope(t *testing.T) {
	db := qualitySchedulingDB(t)
	_, err := db.Exec(`ALTER TABLE groups ADD COLUMN platform TEXT NOT NULL DEFAULT 'openai',ADD COLUMN status TEXT NOT NULL DEFAULT 'active';
 ALTER TABLE accounts ADD COLUMN extra JSONB NOT NULL DEFAULT '{}';
 INSERT INTO groups(id) VALUES(10),(20),(30),(40);
 UPDATE groups SET platform='claude' WHERE id=30;
 UPDATE groups SET deleted_at=NOW() WHERE id=40;
 INSERT INTO account_groups(account_id,group_id) VALUES(1,10)`)
	require.NoError(t, err)
	u := &qualitySchedulingUpstream{calls: map[int64]int{}}
	s, q := qualitySchedulingService(t, db, u)
	ctx := context.Background()
	require.False(t, q.SwitchGroupOnDegradation)
	q.SwitchGroupOnDegradation = true
	require.ErrorContains(t, q.Validate(), "目标分组")
	for _, id := range []int64{30, 40, 99} {
		q.DegradationGroupID = id
		_, err = s.SaveSettings(ctx, q)
		require.ErrorContains(t, err, "OpenAI 分组")
	}
	q.DegradationGroupID, q.AllGroups, q.GroupIDs = 20, false, []int64{10}
	q.PauseOnDegradation = true
	q, err = s.SaveSettings(ctx, q)
	require.NoError(t, err, "both measures may be enabled together")
	require.Empty(t, u.calls, "saving the measure never triggers detection")
	q.PauseOnDegradation = false
	q, err = s.SaveSettings(ctx, q)
	require.NoError(t, err)
	actions := &qualityActionRepo{AccountRepository: s.tests.accountRepo}
	s.tests.accountRepo = actions
	// Simulate a committed automatic move outside the selected scheduled group.
	_, err = db.Exec(`UPDATE account_groups SET group_id=20 WHERE account_id=1;
 UPDATE accounts SET extra='{"quality_group_switch":{"account_id":1,"active":true,"target":20}}' WHERE id=1;
 INSERT INTO account_groups(account_id,group_id) VALUES(2,20)`)
	require.NoError(t, err)
	inScope, err := s.qualityAccountInScope(ctx, q, 1, 0)
	require.NoError(t, err)
	require.True(t, inScope, "automatically moved accounts keep their detector")
	inScope, err = s.qualityAccountInScope(ctx, q, 2, 0)
	require.NoError(t, err)
	require.False(t, inScope, "unrelated accounts in the target group do not get enrolled")
	require.NoError(t, s.runDue(ctx, 0))
	require.Equal(t, map[int64]int{1: 1}, u.calls)
	require.Positive(t, actions.groups, "saving a detection result triggers the group action even with pausing disabled")
	summary, err := s.Summary(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, summary["total"])
	// Disabled preserves the old scope; manual takeover also ends the exception.
	q.SwitchGroupOnDegradation = false
	inScope, err = s.qualityAccountInScope(ctx, q, 1, 0)
	require.NoError(t, err)
	require.False(t, inScope)
	q.SwitchGroupOnDegradation = true
	_, err = db.Exec(`UPDATE accounts SET extra='{"quality_group_switch":{"account_id":1,"active":false,"target":20}}' WHERE id=1`)
	require.NoError(t, err)
	inScope, err = s.qualityAccountInScope(ctx, q, 1, 0)
	require.NoError(t, err)
	require.False(t, inScope)
	// A deleted/invalid target must never prevent disabling the detector.
	q.Enabled, q.DegradationGroupID = false, 99
	_, err = s.SaveSettings(ctx, q)
	require.NoError(t, err)
}

type qualityActionRepo struct {
	AccountRepository
	groups, pauses int
	groupErr       error
}

func (r *qualityActionRepo) SyncQualityGroupSwitches(context.Context) error {
	r.groups++
	return r.groupErr
}

func (r *qualityActionRepo) SyncQualityScheduling(context.Context) error {
	r.pauses++
	return nil
}

func TestAccountQualityGroupFailureDoesNotBlockPauseOrRecovery(t *testing.T) {
	repo := &qualityActionRepo{groupErr: errors.New("target group unavailable")}
	s := &AccountQualityService{tests: &AccountTestService{accountRepo: repo}}
	require.NoError(t, s.syncQualityScheduling(context.Background(), DefaultAccountQualitySettings()))
	require.Equal(t, 1, repo.groups)
	require.Equal(t, 1, repo.pauses)
}
