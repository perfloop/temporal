package historybuilder

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/protobuf/proto"
)

func TestBufferedEventBatchOwnsEvents(t *testing.T) {
	input := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Principal: &commonpb.Principal{
			Type: "user",
			Name: "initial-principal",
		},
	}
	wantSize := proto.Size(input)
	batch := NewBufferedEventBatch([]*historypb.HistoryEvent{input})
	builder := newBufferedEventBatchHistoryBuilder(batch)

	input.Principal.Name = "caller-mutated-principal"
	if got := builder.SizeInBytesOfBufferedEvents(); got != wantSize {
		t.Fatalf("input mutation changed cached buffered size: got %d, want %d", got, wantSize)
	}

	exposedEvents := batch.Events()
	exposedEvents[0].Principal.Name = "output-mutated-principal"
	if got := builder.SizeInBytesOfBufferedEvents(); got != wantSize {
		t.Fatalf("output mutation changed cached buffered size: got %d, want %d", got, wantSize)
	}

	if got := proto.Size(batch.Events()[0]); got != wantSize {
		t.Fatalf("batch retained mutated event: got %d, want %d", got, wantSize)
	}
}

func TestBufferedEventBatchUsesCurrentSizeForNewEvents(t *testing.T) {
	builder := newBufferedEventBatchHistoryBuilder(NewBufferedEventBatch(nil))
	event := builder.AddWorkflowExecutionSignaledEvent("signal", nil, "identity", nil, nil, "request-id", nil)
	event.Principal = &commonpb.Principal{Type: "user", Name: "caller-mutated-principal"}

	if got, want := builder.SizeInBytesOfBufferedEvents(), proto.Size(event); got != want {
		t.Fatalf("new buffered event size is stale after caller mutation: got %d, want %d", got, want)
	}
}

func TestBufferedEventBatchCopiesAndStampsNewEvents(t *testing.T) {
	builder := newBufferedEventBatchHistoryBuilder(NewBufferedEventBatch(nil))
	event := builder.AddWorkflowExecutionSignaledEvent("signal", nil, "identity", nil, nil, "request-id", nil)
	builder.SetBufferedEventPrincipal("user", "header-principal")
	mutation, err := builder.Finish(false)
	if err != nil {
		t.Fatalf("finish history builder: %v", err)
	}

	cachedEvent := mutation.BufferedEventBatch.Events()[0]
	if got := cachedEvent.Principal.GetName(); got != "header-principal" {
		t.Fatalf("cached event principal: got %q, want %q", got, "header-principal")
	}
	if got, want := newBufferedEventBatchHistoryBuilder(mutation.BufferedEventBatch).SizeInBytesOfBufferedEvents(), proto.Size(cachedEvent); got != want {
		t.Fatalf("stamped cached buffered size: got %d, want %d", got, want)
	}

	event.Principal.Name = "caller-mutated-after-finish"
	if got := mutation.BufferedEventBatch.Events()[0].Principal.GetName(); got != "header-principal" {
		t.Fatalf("cached event aliased caller-owned event: got %q, want %q", got, "header-principal")
	}
}

func newBufferedEventBatchHistoryBuilder(batch *BufferedEventBatch) *HistoryBuilder {
	return NewWithBufferedEventBatch(
		clock.NewRealTimeSource(),
		func(int) ([]int64, error) { return nil, nil },
		1,
		1,
		batch,
		StubHandler{},
		tests.NewDynamicConfig().MaximumEventBatchSizeInBytes,
	)
}
