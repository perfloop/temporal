package historybuilder

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/protobuf/proto"
)

func TestEventStoreBufferedSizeLifecycle(t *testing.T) {
	persistedTimer := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
		Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{
			TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
				TimerId: "persisted-timer",
			},
		},
	}
	persistedSignal := bufferedSizeSignalEvent("persisted-signal", 256)

	builder := newBufferedSizeHistoryBuilder([]*historypb.HistoryEvent{persistedTimer, persistedSignal})
	assertBufferedSizeMatchesScan(t, builder)

	builder.AddWorkflowExecutionSignaledEvent(
		"new-signal",
		bufferedSizePayloads(1024),
		"worker",
		&commonpb.Header{Fields: map[string]*commonpb.Payload{
			"source": {Data: []byte("buffer-size-lifecycle")},
		}},
		nil,
		"new-request",
		nil,
	)
	assertBufferedSizeMatchesScan(t, builder)

	builder.AddTimerFiredEvent(1, "memory-timer")
	assertBufferedSizeMatchesScan(t, builder)

	if got := builder.GetAndRemoveTimerFireEvent("memory-timer"); got == nil {
		t.Fatal("expected removal from the in-memory buffered batch")
	}
	assertBufferedSizeMatchesScan(t, builder)

	if got := builder.GetAndRemoveTimerFireEvent("persisted-timer"); got == nil {
		t.Fatal("expected removal from the persisted buffered batch")
	}
	assertBufferedSizeMatchesScan(t, builder)

	builder.FlushBufferToCurrentBatch()
	assertBufferedSizeMatchesScan(t, builder)

	builder.AddWorkflowExecutionSignaledEvent(
		"post-flush-signal",
		bufferedSizePayloads(512),
		"worker",
		nil,
		nil,
		"post-flush-request",
		nil,
	)
	assertBufferedSizeMatchesScan(t, builder)

	mutation, err := builder.Finish(false)
	if err != nil {
		t.Fatalf("finish history builder: %v", err)
	}
	assertBufferedSizeMatchesScan(t, builder)

	if got, want := bufferedEventSliceSize(mutation.MemBufferBatch), bufferedEventSliceSize(mutation.DBBufferBatch); got != want {
		t.Fatalf("finish buffered batches disagree: mem=%d db=%d", got, want)
	}

	reloaded := newBufferedSizeHistoryBuilder(mutation.MemBufferBatch)
	assertBufferedSizeMatchesScan(t, reloaded)

	finishedBuilder := newBufferedSizeHistoryBuilder([]*historypb.HistoryEvent{bufferedSizeSignalEvent("finished-persisted", 128)})
	finishedBuilder.AddWorkflowExecutionSignaledEvent(
		"finished-memory",
		bufferedSizePayloads(128),
		"worker",
		nil,
		nil,
		"finished-request",
		nil,
	)
	finishedBuilder.workflowFinished = true
	finishedBuilder.FlushBufferToCurrentBatch()
	assertBufferedSizeMatchesScan(t, finishedBuilder)
}

func newBufferedSizeHistoryBuilder(dbBufferBatch []*historypb.HistoryEvent) *HistoryBuilder {
	return New(
		clock.NewEventTimeSource(),
		func(count int) ([]int64, error) {
			ids := make([]int64, count)
			for i := range ids {
				ids[i] = int64(i + 1)
			}
			return ids, nil
		},
		1,
		1,
		dbBufferBatch,
		metrics.NoopMetricsHandler,
		tests.NewDynamicConfig().MaximumEventBatchSizeInBytes,
	)
}

func assertBufferedSizeMatchesScan(t *testing.T, builder *HistoryBuilder) {
	t.Helper()
	want := bufferedEventSliceSize(builder.dbBufferBatch) + bufferedEventSliceSize(builder.memBufferBatch)
	if got := builder.SizeInBytesOfBufferedEvents(); got != want {
		t.Fatalf("buffered event size mismatch: got %d, want %d", got, want)
	}
}

func bufferedEventSliceSize(events []*historypb.HistoryEvent) int {
	size := 0
	for _, event := range events {
		size += proto.Size(event)
	}
	return size
}

func bufferedSizeSignalEvent(requestID string, payloadSize int) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: "signal",
				Input:      bufferedSizePayloads(payloadSize),
				RequestId:  requestID,
			},
		},
	}
}

func bufferedSizePayloads(size int) *commonpb.Payloads {
	return &commonpb.Payloads{Payloads: []*commonpb.Payload{{
		Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
		Data:     make([]byte, size),
	}}}
}
