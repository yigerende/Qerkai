package service

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStateKeeperResponseRefreshDefaultAndSavedSettings(t *testing.T) {
	require.False(t, DefaultOpenAIStateKeeperSettings().ResponseRefreshEnabled)
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	encoded, err := json.Marshal(q)
	require.NoError(t, err)
	var legacy map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &legacy))
	delete(legacy, "response_refresh_enabled")
	encoded, err = json.Marshal(legacy)
	require.NoError(t, err)
	require.NoError(t, s.settings.Set(context.Background(), openAIStateKeeperSettingKey, string(encoded)))
	s.reload(context.Background())
	require.False(t, s.Snapshot().Settings.ResponseRefreshEnabled, "old configuration must default to off")
	for _, enabled := range []bool{true, false} {
		q.ResponseRefreshEnabled = enabled
		require.NoError(t, s.Save(context.Background(), q))
		restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
		t.Cleanup(restarted.Stop)
		restarted.reload(context.Background())
		require.Equal(t, enabled, restarted.Snapshot().Settings.ResponseRefreshEnabled)
	}
}

func TestStateKeeperResponseRefreshSwitchDiscardsQueuedAndStaleSignals(t *testing.T) {
	s, gateway, a := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.ResponseRefreshEnabled, q.DegradedStateLengths = true, []int{356}
	require.NoError(t, s.Save(context.Background(), q))
	ticket := gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, http.Header{})
	ticket.observe(strings.Repeat("d", 356), 200)
	keeperDrainObservations(s)
	require.Len(t, s.queue, 1)
	job := <-s.queue
	for range cap(s.observations) {
		s.observations <- openAIStateObservation{}
	}
	ticket.observe(strings.Repeat("d", 356), 200)
	require.Len(t, s.pendingSignals, 1)
	q.ResponseRefreshEnabled = false
	require.NoError(t, s.Save(context.Background(), q))
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		t.Fatal("disabled response refresh must not collect")
		return openAIStateProbeResult{}
	}
	keeperDrainObservations(s)
	s.processPendingSignals()
	s.run(job)
	s.run(openAIStateKeeperJob{accountID: a.ID, model: q.Model, revision: s.config.Load().Revision, source: "response"})
	headers := http.Header{}
	current := gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, headers)
	require.Equal(t, "collected-secret", headers.Get(openAICodexTurnStateHeader))
	current.observe(strings.Repeat("d", 356), 200)
	require.Zero(t, current.responseLength.Load())
	require.Empty(t, s.observations)
	require.Empty(t, s.pendingSignals)
	require.Empty(t, s.queue)
	current.noteSent()
	keeperDrainObservations(s)
	require.Equal(t, int64(1), s.Snapshot().Rows[0].Injections)
	require.Equal(t, "sent", s.Recent([]int64{a.ID})[0].Injections[0].Result)
	q.ResponseRefreshEnabled = true
	require.NoError(t, s.Save(context.Background(), q))
	current.observe(strings.Repeat("d", 356), 200)
	require.Empty(t, s.observations, "requests started with the switch off must remain unobserved")
	fresh := gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, http.Header{})
	fresh.observe(strings.Repeat("d", 356), 200)
	keeperDrainObservations(s)
	require.Len(t, s.queue, 1, "a fresh response can collect after re-enabling")
}
