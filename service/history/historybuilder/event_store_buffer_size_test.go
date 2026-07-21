package historybuilder

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/proto"
)

func TestEventStoreBufferedSizeLifecycle(t *testing.T) {
	t.Run("initialization append finish and reload", func(t *testing.T) {
		dbEvent := newBufferedSizeSignal("db")
		builder := newBufferedSizeHistoryBuilder([]*historypb.HistoryEvent{dbEvent})
		assertBufferedEventSize(t, builder)

		memEvent := addBufferedSizeSignal(builder, "mem")
		if memEvent.EventId != common.BufferedEventID {
			t.Fatalf("signal event ID = %d, want buffered event ID %d", memEvent.EventId, common.BufferedEventID)
		}
		assertBufferedEventSize(t, builder)

		mutation, err := builder.Finish(false)
		if err != nil {
			t.Fatalf("Finish() failed: %v", err)
		}
		if len(mutation.MemBufferBatch) != 2 {
			t.Fatalf("Finish() returned %d buffered events, want 2", len(mutation.MemBufferBatch))
		}

		reloaded := newBufferedSizeHistoryBuilder(mutation.MemBufferBatch)
		assertBufferedEventSize(t, reloaded)
	})

	t.Run("timer removal from persisted and in-memory buffers", func(t *testing.T) {
		dbTimer := newBufferedSizeTimer("db-timer")
		dbOtherTimer := newBufferedSizeTimer("db-other-timer")
		builder := newBufferedSizeHistoryBuilder([]*historypb.HistoryEvent{dbTimer, dbOtherTimer})
		memTimer := builder.AddTimerFiredEvent(1, "mem-timer")
		assertBufferedEventSize(t, builder)

		if got := builder.GetAndRemoveTimerFireEvent("db-timer"); got != dbTimer {
			t.Fatalf("removed persisted timer = %p, want %p", got, dbTimer)
		}
		assertBufferedEventSize(t, builder)

		if got := builder.GetAndRemoveTimerFireEvent("mem-timer"); got != memTimer {
			t.Fatalf("removed in-memory timer = %p, want %p", got, memTimer)
		}
		assertBufferedEventSize(t, builder)
	})

	t.Run("normal flush transfers buffered events", func(t *testing.T) {
		builder := newBufferedSizeHistoryBuilder([]*historypb.HistoryEvent{newBufferedSizeSignal("db")})
		addBufferedSizeSignal(builder, "mem")
		assertBufferedEventSize(t, builder)

		builder.FlushBufferToCurrentBatch()
		if got := builder.NumBufferedEvents(); got != 0 {
			t.Fatalf("buffered event count after flush = %d, want 0", got)
		}
		assertBufferedEventSize(t, builder)
	})

	t.Run("workflow finish discards buffered events", func(t *testing.T) {
		builder := newBufferedSizeHistoryBuilder([]*historypb.HistoryEvent{newBufferedSizeSignal("db")})
		addBufferedSizeSignal(builder, "mem")
		assertBufferedEventSize(t, builder)

		builder.workflowFinished = true
		builder.FlushBufferToCurrentBatch()
		if got := builder.NumBufferedEvents(); got != 0 {
			t.Fatalf("buffered event count after finished-workflow flush = %d, want 0", got)
		}
		assertBufferedEventSize(t, builder)
	})
}

func newBufferedSizeHistoryBuilder(dbBufferBatch []*historypb.HistoryEvent) *HistoryBuilder {
	return New(
		clock.NewEventTimeSource(),
		func(number int) ([]int64, error) {
			return make([]int64, number), nil
		},
		1,
		1,
		dbBufferBatch,
		nil,
		func() int { return 0 },
	)
}

func newBufferedSizeSignal(id string) *historypb.HistoryEvent {
	builder := newBufferedSizeHistoryBuilder(nil)
	return addBufferedSizeSignal(builder, id)
}

func addBufferedSizeSignal(builder *HistoryBuilder, id string) *historypb.HistoryEvent {
	return builder.AddWorkflowExecutionSignaledEvent(
		"signal-"+id,
		&commonpb.Payloads{Payloads: []*commonpb.Payload{{
			Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
			Data:     []byte("payload-" + id),
		}}},
		"identity-"+id,
		&commonpb.Header{Fields: map[string]*commonpb.Payload{
			"trace": {
				Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
				Data:     []byte("trace-" + id),
			},
		}},
		nil,
		"request-"+id,
		nil,
	)
}

func newBufferedSizeTimer(id string) *historypb.HistoryEvent {
	builder := newBufferedSizeHistoryBuilder(nil)
	return builder.AddTimerFiredEvent(1, id)
}

func assertBufferedEventSize(t *testing.T, builder *HistoryBuilder) {
	t.Helper()

	want := 0
	for _, event := range builder.dbBufferBatch {
		want += proto.Size(event)
	}
	for _, event := range builder.memBufferBatch {
		want += proto.Size(event)
	}

	if got := builder.SizeInBytesOfBufferedEvents(); got != want {
		t.Fatalf("buffered event size = %d, want fresh protobuf-size total %d", got, want)
	}
}
