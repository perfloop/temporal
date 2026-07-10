package matching

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkUpdatePollerInfo(b *testing.B) {
	history := newPollerHistory(5 * time.Minute)
	metadata := &pollMetadata{}

	identities := make([]pollerIdentity, 100)
	for i := 0; i < 100; i++ {
		identities[i] = pollerIdentity("worker-" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		history.updatePollerInfo(identities[i%100], metadata)
	}
}
