package historybuilder

import (
	"fmt"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/proto"
)

const (
	bufferedSizeBenchmarkEventCount = 100
	bufferedSizeBenchmarkPayload    = 4 << 10
)

func TestEventStoreBufferedSizeLifecycle(t *testing.T) {
	t.Run("loaded and newly buffered events", func(t *testing.T) {
		dbEvents := append(
			bufferedSignalEvents(2, 128),
			bufferedTimerFiredEvent("remove-me"),
		)
		historyBuilder := newBufferedSizeHistoryBuilder(dbEvents)
		assertBufferedSizeMatchesEvents(t, historyBuilder)

		addBufferedSignals(historyBuilder, 2, 256)
		assertBufferedSizeMatchesEvents(t, historyBuilder)

		if event := historyBuilder.GetAndRemoveTimerFireEvent("remove-me"); event == nil {
			t.Fatal("expected the persisted timer-fired event to be removed")
		}
		assertBufferedSizeMatchesEvents(t, historyBuilder)

		historyBuilder.FlushBufferToCurrentBatch()
		assertBufferedSizeMatchesEvents(t, historyBuilder)
		if historyBuilder.NumBufferedEvents() != 0 {
			t.Fatalf("buffered event count after flush = %d, want 0", historyBuilder.NumBufferedEvents())
		}
	})

	t.Run("finish and reload", func(t *testing.T) {
		historyBuilder := newBufferedSizeHistoryBuilder(bufferedSignalEvents(2, 96))
		addBufferedSignals(historyBuilder, 3, 192)
		assertBufferedSizeMatchesEvents(t, historyBuilder)

		mutation, err := historyBuilder.Finish(false)
		if err != nil {
			t.Fatalf("Finish() error = %v", err)
		}
		assertBufferedSizeMatchesEvents(t, historyBuilder)
		if historyBuilder.NumBufferedEvents() != 0 {
			t.Fatalf("buffered event count after finish = %d, want 0", historyBuilder.NumBufferedEvents())
		}

		reloaded := newBufferedSizeHistoryBuilder(mutation.MemBufferBatch)
		assertBufferedSizeMatchesEvents(t, reloaded)
		if got, want := reloaded.NumBufferedEvents(), len(mutation.MemBufferBatch); got != want {
			t.Fatalf("reloaded buffered event count = %d, want %d", got, want)
		}
	})

	t.Run("finished workflow discards buffers", func(t *testing.T) {
		historyBuilder := newBufferedSizeHistoryBuilder(bufferedSignalEvents(1, 64))
		addBufferedSignals(historyBuilder, 1, 64)
		historyBuilder.workflowFinished = true
		historyBuilder.FlushBufferToCurrentBatch()
		assertBufferedSizeMatchesEvents(t, historyBuilder)
		if historyBuilder.NumBufferedEvents() != 0 {
			t.Fatalf("buffered event count after finished-workflow flush = %d, want 0", historyBuilder.NumBufferedEvents())
		}
	})
}

func BenchmarkEventStoreBufferedSizeMixed100x4KiB(b *testing.B) {
	dbEvents := bufferedSignalEvents(bufferedSizeBenchmarkEventCount/2, bufferedSizeBenchmarkPayload)
	historyBuilder := newBufferedSizeHistoryBuilder(dbEvents)
	addBufferedSignals(historyBuilder, bufferedSizeBenchmarkEventCount/2, bufferedSizeBenchmarkPayload)

	want := serializedBufferedEventsSize(historyBuilder)
	if got := historyBuilder.SizeInBytesOfBufferedEvents(); got != want {
		b.Fatalf("initial buffered size = %d, want %d", got, want)
	}

	b.ReportAllocs()
	total := 0
	for b.Loop() {
		total += historyBuilder.SizeInBytesOfBufferedEvents()
	}
	if total == 0 {
		b.Fatal("benchmark did not consume a buffered event size")
	}
}

func newBufferedSizeHistoryBuilder(dbEvents []*historypb.HistoryEvent) *HistoryBuilder {
	return New(
		clock.NewRealTimeSource(),
		func(n int) ([]int64, error) {
			ids := make([]int64, n)
			for i := range ids {
				ids[i] = int64(i + 1)
			}
			return ids, nil
		},
		1,
		1,
		dbEvents,
		metrics.NoopMetricsHandler,
		func() int { return 0 },
	)
}

func bufferedSignalEvents(count, payloadSize int) []*historypb.HistoryEvent {
	historyBuilder := newBufferedSizeHistoryBuilder(nil)
	addBufferedSignals(historyBuilder, count, payloadSize)
	return historyBuilder.memBufferBatch
}

func addBufferedSignals(historyBuilder *HistoryBuilder, count, payloadSize int) {
	for i := 0; i < count; i++ {
		payload := make([]byte, payloadSize)
		for j := range payload {
			payload[j] = byte(i + j)
		}
		historyBuilder.AddWorkflowExecutionSignaledEvent(
			"buffered-size-signal",
			&commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: payload}}},
			"benchmark",
			nil,
			nil,
			fmt.Sprintf("buffered-size-%d", i),
			nil,
		)
	}
}

func bufferedTimerFiredEvent(timerID string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
		Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{
			TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{TimerId: timerID},
		},
	}
}

func assertBufferedSizeMatchesEvents(t *testing.T, historyBuilder *HistoryBuilder) {
	t.Helper()
	want := serializedBufferedEventsSize(historyBuilder)
	if got := historyBuilder.SizeInBytesOfBufferedEvents(); got != want {
		t.Fatalf("buffered event size = %d, want %d", got, want)
	}
}

func serializedBufferedEventsSize(historyBuilder *HistoryBuilder) int {
	size := 0
	for _, event := range historyBuilder.dbBufferBatch {
		size += proto.Size(event)
	}
	for _, event := range historyBuilder.memBufferBatch {
		size += proto.Size(event)
	}
	return size
}
