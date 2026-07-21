//go:build perfloop_buffer_size_counter

package workflow

import (
	"testing"

	"go.temporal.io/server/service/history/historybuilder"
)

// TestBufferSizeAcceptableSizingCounter uses a build-tagged wrapper around the
// production buffered-event sizing call. It distinguishes the baseline's
// full-buffer scan from a cache implementation without adding counter work to
// the benchmark build.
func TestBufferSizeAcceptableSizingCounter(t *testing.T) {
	small := newBufferedTransactionFixture(t, "counter-small", 1, 4<<10, 128, 1)
	t.Cleanup(small.close)
	large := newBufferedTransactionFixture(t, "counter-large", 50, 4<<10, 128, 1)
	t.Cleanup(large.close)

	smallCalls := bufferSizeAcceptableSizingCalls(t, small)
	largeCalls := bufferSizeAcceptableSizingCalls(t, large)

	switch {
	case smallCalls == 0 || largeCalls == 0:
		t.Fatalf("buffered proto.Size calls = (%d, %d), want nonzero exact work", smallCalls, largeCalls)
	case largeCalls == smallCalls:
		t.Logf("buffered proto.Size calls are cache-stable: %d at 1 and 50 persisted events", smallCalls)
	case largeCalls-smallCalls >= 40:
		t.Logf("buffered proto.Size calls grow with the full scan: %d -> %d", smallCalls, largeCalls)
	default:
		t.Fatalf("buffered proto.Size calls = (%d, %d), want full-scan growth or cache stability", smallCalls, largeCalls)
	}
}

func bufferSizeAcceptableSizingCalls(t *testing.T, fixture *bufferedTransactionFixture) uint64 {
	t.Helper()

	historybuilder.ResetPerfloopBufferedEventSizeCalls()
	event, err := addBufferedSignalToMutableState(fixture.state, fixture.input, fixture.requestIDs[0])
	if err != nil {
		t.Fatalf("%s: add buffered signal: %v", fixture.name, err)
	}
	fixture.maximumBufferedEvents = fixture.bufferedEvents + 1
	fixture.maximumBufferedBytes = fixture.bufferedBytes + fixture.addedEventSize
	if !fixture.state.BufferSizeAcceptable() {
		t.Fatalf("%s: buffer exactly at both limits is not acceptable after adding %v", fixture.name, event.GetEventType())
	}
	return historybuilder.PerfloopBufferedEventSizeCalls()
}
