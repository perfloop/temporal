//go:build perfloop_buffer_size_counter

package historybuilder

import (
	"sync/atomic"

	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
)

var perfloopBufferedEventSizeCalls atomic.Uint64

func bufferedEventProtoSize(event *historypb.HistoryEvent) int {
	perfloopBufferedEventSizeCalls.Add(1)
	return proto.Size(event)
}

// ResetPerfloopBufferedEventSizeCalls resets the build-tagged exact counter
// used only by the benchmark mechanism check.
func ResetPerfloopBufferedEventSizeCalls() {
	perfloopBufferedEventSizeCalls.Store(0)
}

// PerfloopBufferedEventSizeCalls returns the build-tagged exact counter used
// only by the benchmark mechanism check.
func PerfloopBufferedEventSizeCalls() uint64 {
	return perfloopBufferedEventSizeCalls.Load()
}
