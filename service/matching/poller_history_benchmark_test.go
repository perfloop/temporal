package matching

import (
	"strconv"
	"testing"
	"time"
)

var escapeSink any

func BenchmarkUpdatePollerInfo(b *testing.B) {
	history := newPollerHistory(5 * time.Minute)

	identities := make([]pollerIdentity, 100)
	for i := range 100 {
		identities[i] = pollerIdentity("worker-" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metadata := &pollMetadata{}
		escapeSink = metadata
		history.updatePollerInfo(identities[i%100], metadata)
	}
}
