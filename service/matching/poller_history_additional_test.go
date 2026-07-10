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
}
