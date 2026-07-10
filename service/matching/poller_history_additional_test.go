package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPollerHistoryAdditional(t *testing.T) {
	t.Run("poller visibility immediately after update", func(t *testing.T) {
		t.Parallel()
		history := newPollerHistory(5 * time.Minute)

		identity := pollerIdentity("worker-active")
		metadata := &pollMetadata{}
		history.updatePollerInfo(identity, metadata)

		pollers := history.getPollerInfo(time.Time{})
		require.Len(t, pollers, 1)
		require.Equal(t, "worker-active", pollers[0].Identity)
	})

	t.Run("pollMetadata concurrent read safety", func(t *testing.T) {
		t.Parallel()
		history := newPollerHistory(5 * time.Minute)

		identity := pollerIdentity("worker-shared")
		metadata := &pollMetadata{}
		history.updatePollerInfo(identity, metadata)

		done := make(chan struct{})
		go func() {
			for range 100 {
				_ = history.getPollerInfo(time.Time{})
			}
			close(done)
		}()
		<-done
	})

	t.Run("pollMetadata refresh TTL and LastAccessTime on identical update", func(t *testing.T) {
		t.Parallel()
		history := newPollerHistory(5 * time.Minute)

		identity := pollerIdentity("worker-active")
		metadata := &pollMetadata{}
		history.updatePollerInfo(identity, metadata)

		pollers1 := history.getPollerInfo(time.Time{})
		require.Len(t, pollers1, 1)
		time1 := pollers1[0].LastAccessTime.AsTime()

		// Sleep for a short duration to ensure time advances
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo

		history.updatePollerInfo(identity, metadata)

		pollers2 := history.getPollerInfo(time.Time{})
		require.Len(t, pollers2, 1)
		time2 := pollers2[0].LastAccessTime.AsTime()

		// Assert that LastAccessTime has successfully updated (time2 > time1)
		require.True(t, time2.After(time1), "Expected LastAccessTime to be updated on identical metadata update, got %v and %v", time1, time2)
	})
}
