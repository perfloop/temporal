package workflow_test

import (
	"context"
	"math"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type bufferedEventMaterializationShape struct {
	name          string
	eventCount    int
	payloadBytes  int
	payloadOffset byte
}

type bufferedEventMaterializationFixture struct {
	shape  bufferedEventMaterializationShape
	record *persistencespb.WorkflowMutableState
}

// BenchmarkBufferedEventBatchReload measures loading the persisted buffered
// batch at the same count and payload shapes as the active-cadence benchmark.
func BenchmarkBufferedEventBatchReload(b *testing.B) {
	h, fixtures := newBufferedEventMaterializationFixtures(b)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, fixture := range fixtures {
			state := loadBufferedEventMaterializationState(b, h, fixture.record)
			if got := state.GetNextEventID(); got != fixture.record.GetNextEventId() {
				b.Fatalf("%s reload next event ID = %d, want %d", fixture.shape.name, got, fixture.record.GetNextEventId())
			}
		}
	}
}

// BenchmarkBufferedEventBatchCloneToProto measures immutable persistence
// output materialization for the same buffered-event records.
func BenchmarkBufferedEventBatchCloneToProto(b *testing.B) {
	h, fixtures := newBufferedEventMaterializationFixtures(b)
	states := make([]*workflow.MutableStateImpl, len(fixtures))
	for i, fixture := range fixtures {
		states[i] = loadBufferedEventMaterializationState(b, h, fixture.record)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for i, state := range states {
			snapshot := state.CloneToProto()
			if got := len(snapshot.GetBufferedEvents()); got != fixtures[i].shape.eventCount {
				b.Fatalf("%s snapshot retained %d buffered events, want %d", fixtures[i].shape.name, got, fixtures[i].shape.eventCount)
			}
		}
	}
}

func newBufferedEventMaterializationFixtures(b *testing.B) (*bufferedEventSizeHarness, []bufferedEventMaterializationFixture) {
	b.Helper()

	shapes := []bufferedEventMaterializationShape{
		{name: "1x256B", eventCount: 1, payloadBytes: 256, payloadOffset: 1},
		{name: "25x1KiB", eventCount: 25, payloadBytes: 1024, payloadOffset: 2},
		{name: "50x4KiB", eventCount: 50, payloadBytes: 4 * 1024, payloadOffset: 3},
		{name: "100x4KiB", eventCount: 100, payloadBytes: 4 * 1024, payloadOffset: 4},
	}
	h := newBufferedEventSizeHarness(b, math.MaxInt, math.MaxInt)
	fixtures := make([]bufferedEventMaterializationFixture, 0, len(shapes))
	for _, shape := range shapes {
		state, _ := h.newActiveState(b)
		for i := 0; i < shape.eventCount; i++ {
			addBufferedSizeSignal(b, state, bufferedSignalPayload(shape.payloadBytes, shape.payloadOffset+byte(i)))
		}
		closeBufferedSizeTransaction(b, state, context.Background())
		record := state.CloneToProto()
		// NewMutableStateFromDB backfills this field for old records. Set it here
		// so every timed reload receives the same current-format record.
		record.ExecutionState.FirstExecutionRunId = "buffered-event-materialization-run"
		fixtures = append(fixtures, bufferedEventMaterializationFixture{shape: shape, record: record})
	}
	return h, fixtures
}

func loadBufferedEventMaterializationState(
	b *testing.B,
	h *bufferedEventSizeHarness,
	record *persistencespb.WorkflowMutableState,
) *workflow.MutableStateImpl {
	b.Helper()

	state, err := workflow.NewMutableStateFromDB(
		h.shardContext,
		h.eventsCache,
		log.NewNoopLogger(),
		h.namespaceEntry,
		record,
		1,
	)
	if err != nil {
		b.Fatalf("load materialization fixture: %v", err)
	}
	return state
}

// BenchmarkBufferedEventBatchRichSignalCadence exercises the structural signal
// inputs whose ownership is snapshotted before buffered insertion: populated
// headers, external-workflow metadata, external-payload details, unknown fields,
// and link fanout.
func BenchmarkBufferedEventBatchRichSignalCadence(b *testing.B) {
	h := newBufferedEventSizeHarness(b, 100, math.MaxInt)

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		b.StopTimer()
		state, _ := h.newActiveState(b)
		input, header, external, links := richBufferedSignalInput(byte(i))
		ctx := headers.SetPrincipal(context.Background(), &commonpb.Principal{Type: "user", Name: "rich-buffered-signal"})
		b.StartTimer()

		event, err := state.AddWorkflowExecutionSignaledEvent(
			"rich-buffered-size-signal",
			input,
			"rich-buffered-size-test",
			header,
			external,
			"rich-buffered-size-request",
			links,
		)
		if err != nil {
			b.Fatalf("add rich buffered signal: %v", err)
		}
		mutation := closeBufferedSizeTransaction(b, state, ctx)

		b.StopTimer()
		if event.GetWorkflowExecutionSignaledEventAttributes() == nil || len(mutation.NewBufferedEvents) != 1 {
			b.Fatal("rich signal cadence did not persist exactly one buffered event")
		}
		if got := len(state.CloneToProto().GetBufferedEvents()); got != 1 {
			b.Fatalf("rich signal cadence retained %d buffered events, want one", got)
		}
		b.StartTimer()
	}
}

func richBufferedSignalInput(seed byte) (*commonpb.Payloads, *commonpb.Header, *commonpb.WorkflowExecution, []*commonpb.Link) {
	input := &commonpb.Payloads{Payloads: []*commonpb.Payload{
		{
			Metadata: map[string][]byte{
				"encoding": []byte("binary/plain"),
				"codec":    []byte("rich-buffered-signal"),
			},
			Data: []byte{seed, seed + 1, seed + 2, seed + 3},
			ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
				{SizeBytes: 1024},
				{SizeBytes: 2048},
			},
		},
		{
			Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
			Data:     []byte{seed + 4, seed + 5},
		},
	}}
	input.Payloads[0].ProtoReflect().SetUnknown(protoreflect.RawFields{0x98, 0x06, 0x01})

	header := &commonpb.Header{Fields: map[string]*commonpb.Payload{
		"routing": {
			Metadata: map[string][]byte{
				"encoding": []byte("binary/plain"),
				"source":   []byte("rich-benchmark"),
			},
			Data: []byte{seed + 6, seed + 7},
		},
		"trace": {
			Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
			Data:     []byte{seed + 8},
		},
	}}

	external := &commonpb.WorkflowExecution{WorkflowId: "external-buffered-workflow", RunId: "external-buffered-run"}
	links := []*commonpb.Link{
		{Variant: &commonpb.Link_Workflow_{Workflow: &commonpb.Link_Workflow{
			Namespace: "external-namespace", WorkflowId: "linked-workflow-a", RunId: "linked-run-a", Reason: "rich-benchmark",
		}}},
		{Variant: &commonpb.Link_Workflow_{Workflow: &commonpb.Link_Workflow{
			Namespace: "external-namespace", WorkflowId: "linked-workflow-b", RunId: "linked-run-b", Reason: "rich-benchmark",
		}}},
	}
	return input, header, external, links
}
