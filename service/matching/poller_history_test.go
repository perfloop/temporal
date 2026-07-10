package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPollerHistory(t *testing.T) {
	t.Parallel()

	t.Run("removePoller removes entry", func(t *testing.T) {
		t.Parallel()
		history := newPollerHistory(5 * time.Minute)

		identity := pollerIdentity("worker-1@host")
		history.updatePollerInfo(identity, &pollMetadata{})

		// Verify it exists
		pollers := history.getPollerInfo(time.Time{})
		require.Len(t, pollers, 1)
		require.Equal(t, "worker-1@host", pollers[0].Identity)

		// Remove it
		history.removePoller(identity)

		// Verify it's gone
		pollers = history.getPollerInfo(time.Time{})
		require.Empty(t, pollers)
	})

	t.Run("removePoller is idempotent", func(t *testing.T) {
		t.Parallel()
		history := newPollerHistory(5 * time.Minute)

		identity := pollerIdentity("worker-1@host")
		history.updatePollerInfo(identity, &pollMetadata{})

		// Remove twice - should not panic
		history.removePoller(identity)
		history.removePoller(identity)

		pollers := history.getPollerInfo(time.Time{})
		require.Empty(t, pollers)
	})

	t.Run("removePoller does not affect other entries", func(t *testing.T) {
		t.Parallel()
		history := newPollerHistory(5 * time.Minute)

		history.updatePollerInfo(pollerIdentity("worker-1@host"), &pollMetadata{})
		history.updatePollerInfo(pollerIdentity("worker-2@host"), &pollMetadata{})

		history.removePoller(pollerIdentity("worker-1@host"))

		pollers := history.getPollerInfo(time.Time{})
		require.Len(t, pollers, 1)
		require.Equal(t, "worker-2@host", pollers[0].Identity)
	})

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
