package workflow

import (
	"fmt"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/historybuilder"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

var bufferedEventTimeSource = clock.NewEventTimeSource().Update(time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC))

type bufferedSignalInput struct {
	payloads *commonpb.Payloads
	header   *commonpb.Header
}

type bufferedTransactionFixture struct {
	name             string
	input            bufferedSignalInput
	requestIDs       []string
	cycleLength      int
	seed             *persistencespb.WorkflowMutableState
	seedBufferedSize int
	addedEventSize   int

	controller   *gomock.Controller
	shardContext *shard.ContextTest
	eventsCache  *events.MockCache
	state        *MutableStateImpl

	maximumBufferedEvents int
	maximumBufferedBytes  int
	bufferedEvents        int
	bufferedBytes         int
	commits               int
}

func TestBufferedEventSizeLifecycle(t *testing.T) {
	input := newBufferedSignalInput(4<<10, 256)
	seed := newBufferedEventHistoryBuilder(nil)
	persistedTimer := seed.AddTimerFiredEvent(42, "persisted-timer")
	persistedSignal := addBufferedSignal(seed, input, "persisted-request")
	seedMutation, err := seed.Finish(false)
	if err != nil {
		t.Fatalf("finish seed history builder: %v", err)
	}

	builder := newBufferedEventHistoryBuilder(seedMutation.MemBufferBatch)
	assertBufferedEventSize(t, builder, persistedTimer, persistedSignal)

	if removed := builder.GetAndRemoveTimerFireEvent("persisted-timer"); removed != persistedTimer {
		t.Fatalf("removed persisted timer = %p, want %p", removed, persistedTimer)
	}
	assertBufferedEventSize(t, builder, persistedSignal)

	inMemorySignal := addBufferedSignal(builder, input, "in-memory-request")
	assertBufferedEventSize(t, builder, persistedSignal, inMemorySignal)

	inMemoryTimer := builder.AddTimerFiredEvent(43, "in-memory-timer")
	assertBufferedEventSize(t, builder, persistedSignal, inMemorySignal, inMemoryTimer)

	if removed := builder.GetAndRemoveTimerFireEvent("in-memory-timer"); removed != inMemoryTimer {
		t.Fatalf("removed in-memory timer = %p, want %p", removed, inMemoryTimer)
	}
	assertBufferedEventSize(t, builder, persistedSignal, inMemorySignal)

	builder.FlushBufferToCurrentBatch()
	assertBufferedEventSize(t, builder)

	postFlushSignal := addBufferedSignal(builder, input, "post-flush-request")
	assertBufferedEventSize(t, builder, postFlushSignal)

	mutation, err := builder.Finish(false)
	if err != nil {
		t.Fatalf("finish history builder: %v", err)
	}
	assertBufferedEventSize(t, builder)

	reloaded := newBufferedEventHistoryBuilder(mutation.MemBufferBatch)
	assertBufferedEventSize(t, reloaded, mutation.MemBufferBatch...)

	finished := newBufferedEventHistoryBuilder(nil)
	discardedSignal := addBufferedSignal(finished, input, "discarded-after-finish")
	assertBufferedEventSize(t, finished, discardedSignal)
	finished.AddWorkflowExecutionTerminatedEvent("test completion", nil, "historybuilder-test", nil)
	assertBufferedEventSize(t, finished, discardedSignal)
	finished.FlushBufferToCurrentBatch()
	assertBufferedEventSize(t, finished)
}

func TestBufferSizeAcceptableBoundaries(t *testing.T) {
	input := newBufferedSignalInput(512, 64)
	seed := newBufferedEventHistoryBuilder(nil)
	first := addBufferedSignal(seed, input, "boundary-first")
	second := addBufferedSignal(seed, input, "boundary-second")
	mutation, err := seed.Finish(false)
	if err != nil {
		t.Fatalf("finish seed history builder: %v", err)
	}

	state := newBufferedSizeState(
		mutation.MemBufferBatch,
		len(mutation.MemBufferBatch)+1,
		0,
	)
	third, err := addBufferedSignalToMutableState(state, input, "boundary-third")
	if err != nil {
		t.Fatalf("add buffered signal: %v", err)
	}

	serializedSize := bufferedEventsSerializedSize([]*historypb.HistoryEvent{first, second, third})
	state.config.MaximumBufferedEventsSizeInBytes = func() int { return serializedSize }
	if !state.BufferSizeAcceptable() {
		t.Fatal("buffer exactly at both limits is not acceptable")
	}

	state.config.MaximumBufferedEventsSizeInBytes = func() int { return serializedSize - 1 }
	if state.BufferSizeAcceptable() {
		t.Fatal("buffer over the byte limit is acceptable")
	}

	state.config.MaximumBufferedEventsBatch = func() int { return len(mutation.MemBufferBatch) }
	state.config.MaximumBufferedEventsSizeInBytes = func() int {
		t.Fatal("byte limit evaluated after the count limit rejected the buffer")
		return 0
	}
	if state.BufferSizeAcceptable() {
		t.Fatalf("buffer with %d events is acceptable at count limit %d", len(mutation.MemBufferBatch)+1, len(mutation.MemBufferBatch))
	}

	if got := bufferedEventsSerializedSize(mutation.MemBufferBatch); got != bufferedEventsSerializedSize([]*historypb.HistoryEvent{first, second}) {
		t.Fatalf("independent persisted serialized size = %d, want %d", got, bufferedEventsSerializedSize([]*historypb.HistoryEvent{first, second}))
	}
}

func TestBufferedEventSizeAcrossActiveTransactions(t *testing.T) {
	fixture := newBufferedTransactionFixture(t, "transaction", 25, 4<<10, 128, 1)
	t.Cleanup(fixture.close)

	event, err := addBufferedSignalToMutableState(fixture.state, fixture.input, fixture.requestIDs[0])
	if err != nil {
		t.Fatalf("add buffered signal: %v", err)
	}
	fixture.maximumBufferedEvents = fixture.bufferedEvents + 1
	fixture.maximumBufferedBytes = fixture.bufferedBytes + fixture.addedEventSize
	if !fixture.state.BufferSizeAcceptable() {
		t.Fatal("buffer exactly at both limits is not acceptable")
	}

	mutation, err := fixture.state.hBuilder.Finish(false)
	if err != nil {
		t.Fatalf("finish history builder: %v", err)
	}
	if got := len(mutation.MemBufferBatch); got != fixture.bufferedEvents+1 {
		t.Fatalf("persisted buffered events = %d, want %d", got, fixture.bufferedEvents+1)
	}
	if mutation.MemBufferBatch[len(mutation.MemBufferBatch)-1] != event {
		t.Fatal("finished mutation does not preserve the buffered signal")
	}

	// This is the buffered portion of closeTransactionPrepareEvents followed by
	// cleanupTransaction, which creates the builder used by the next active commit.
	fixture.state.bufferEventsInDB = mutation.MemBufferBatch
	if err := fixture.state.cleanupTransaction(); err != nil {
		t.Fatalf("clean up transaction: %v", err)
	}
	if got, want := fixture.state.hBuilder.SizeInBytesOfBufferedEvents(), fixture.bufferedBytes+fixture.addedEventSize; got != want {
		t.Fatalf("buffered event size after cleanup = %d, want %d", got, want)
	}
}

// BenchmarkBufferSizeAcceptableLifecycle measures accepted buffered-signal commits
// while a workflow task remains started. Each timed unit adds a real signal event,
// checks the exact count and byte boundaries, finishes its buffered mutation, and
// recreates the HistoryBuilder used by the next active commit. The fixture reset is
// outside timing so each sample repeatedly visits the 1, 25, 50, and 100-event
// domain without charging unrelated workflow construction to the decision.
func BenchmarkBufferSizeAcceptableLifecycle(b *testing.B) {
	fixtures := []*bufferedTransactionFixture{
		newBufferedTransactionFixture(b, "one-small", 1, 256, 32, 50),
		newBufferedTransactionFixture(b, "twenty-five-medium", 25, 4<<10, 128, 50),
		newBufferedTransactionFixture(b, "fifty-count-boundary", 50, 24<<10, 512, 50),
	}
	for _, fixture := range fixtures {
		b.Cleanup(fixture.close)
	}

	b.ReportAllocs()
	completed := 0
	for i := 0; b.Loop(); i++ {
		fixture := fixtures[i%len(fixtures)]
		event, err := addBufferedSignalToMutableState(
			fixture.state,
			fixture.input,
			fixture.requestIDs[fixture.commits%len(fixture.requestIDs)],
		)
		if err != nil {
			b.Fatalf("%s: add buffered signal: %v", fixture.name, err)
		}
		fixture.maximumBufferedEvents = fixture.bufferedEvents + 1
		fixture.maximumBufferedBytes = fixture.bufferedBytes + fixture.addedEventSize
		if !fixture.state.BufferSizeAcceptable() {
			b.Fatalf("%s: buffer exactly at both limits is not acceptable", fixture.name)
		}

		mutation, err := fixture.state.hBuilder.Finish(false)
		if err != nil {
			b.Fatalf("%s: finish history builder: %v", fixture.name, err)
		}
		if got := len(mutation.MemBufferBatch); got != fixture.bufferedEvents+1 {
			b.Fatalf("%s: persisted buffered events = %d, want %d", fixture.name, got, fixture.bufferedEvents+1)
		}
		if mutation.MemBufferBatch[len(mutation.MemBufferBatch)-1] != event {
			b.Fatalf("%s: signal event was not preserved in the finished buffer", fixture.name)
		}

		fixture.state.bufferEventsInDB = mutation.MemBufferBatch
		if err := fixture.state.cleanupTransaction(); err != nil {
			b.Fatalf("%s: clean up transaction: %v", fixture.name, err)
		}
		fixture.bufferedEvents++
		fixture.bufferedBytes += fixture.addedEventSize
		fixture.commits++
		completed += len(mutation.MemBufferBatch)

		if fixture.commits == fixture.cycleLength {
			b.StopTimer()
			if err := fixture.reset(); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	}
	if completed == 0 {
		b.Fatal("benchmark did not finish a buffered-event lifecycle")
	}
}

func newBufferedTransactionFixture(
	t testing.TB,
	name string,
	persistedEventCount int,
	payloadSize int,
	headerSize int,
	cycleLength int,
) *bufferedTransactionFixture {
	t.Helper()
	fixture := &bufferedTransactionFixture{
		name:        name,
		input:       newBufferedSignalInput(payloadSize, headerSize),
		requestIDs:  []string{"benchmark-request-0000", "benchmark-request-0001", "benchmark-request-0002", "benchmark-request-0003"},
		cycleLength: cycleLength,
	}
	persisted := makeBufferedSignals(persistedEventCount, fixture.input)
	fixture.seedBufferedSize = bufferedEventsSerializedSize(persisted)

	fixture.controller = gomock.NewController(t)
	config := tests.NewDynamicConfig()
	config.MaximumBufferedEventsBatch = func() int { return fixture.maximumBufferedEvents }
	config.MaximumBufferedEventsSizeInBytes = func() int { return fixture.maximumBufferedBytes }
	fixture.shardContext = shard.NewTestContextWithTimeSource(
		fixture.controller,
		&persistencespb.ShardInfo{ShardId: 1, RangeId: 1},
		config,
		bufferedEventTimeSource,
	)
	registry := hsm.NewRegistry()
	if err := RegisterStateMachine(registry); err != nil {
		t.Fatal(err)
	}
	if err := callbacks.RegisterStateMachine(registry); err != nil {
		t.Fatal(err)
	}
	if err := nexusoperations.RegisterStateMachines(registry); err != nil {
		t.Fatal(err)
	}
	fixture.shardContext.SetStateMachineRegistry(registry)
	fixture.eventsCache = events.NewMockCache(fixture.controller)
	fixture.shardContext.SetEventsCacheForTesting(fixture.eventsCache)
	fixture.shardContext.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	fixture.shardContext.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(tests.GlobalNamespaceEntry.IsGlobalNamespace(), gomock.Any()).Return(cluster.TestCurrentClusterName).AnyTimes()
	fixture.shardContext.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	fixture.shardContext.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()

	base := NewMutableState(
		fixture.shardContext,
		fixture.eventsCache,
		fixture.shardContext.GetLogger(),
		tests.GlobalNamespaceEntry,
		"buffer-size-workflow",
		"buffer-size-run",
		bufferedEventTimeSource.Now(),
	)
	base.executionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	base.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	base.executionInfo.WorkflowTaskScheduledEventId = 1
	base.executionInfo.WorkflowTaskStartedEventId = 2
	fixture.seed = &persistencespb.WorkflowMutableState{
		ExecutionInfo:  base.executionInfo,
		ExecutionState: base.executionState,
		NextEventId:    base.hBuilder.NextEventID(),
		BufferedEvents: persisted,
	}
	if err := fixture.reset(); err != nil {
		t.Fatal(err)
	}
	probe, err := addBufferedSignalToMutableState(fixture.state, fixture.input, fixture.requestIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	fixture.addedEventSize = proto.Size(probe)
	if err := fixture.reset(); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (f *bufferedTransactionFixture) reset() error {
	record := proto.Clone(f.seed).(*persistencespb.WorkflowMutableState)
	state, err := NewMutableStateFromDB(
		f.shardContext,
		f.eventsCache,
		f.shardContext.GetLogger(),
		tests.GlobalNamespaceEntry,
		record,
		1,
	)
	if err != nil {
		return fmt.Errorf("load mutable state: %w", err)
	}
	f.state = state
	f.bufferedEvents = len(record.BufferedEvents)
	f.bufferedBytes = f.seedBufferedSize
	f.commits = 0
	return nil
}

func (f *bufferedTransactionFixture) close() {
	f.controller.Finish()
	f.shardContext.StopForTest()
}

func makeBufferedSignals(count int, input bufferedSignalInput) []*historypb.HistoryEvent {
	builder := newBufferedEventHistoryBuilder(nil)
	for i := 0; i < count; i++ {
		addBufferedSignal(builder, input, fmt.Sprintf("persisted-request-%04d", i))
	}
	mutation, err := builder.Finish(false)
	if err != nil {
		panic(fmt.Sprintf("finish buffered signal fixture: %v", err))
	}
	return mutation.MemBufferBatch
}

func newBufferedSizeState(
	persisted []*historypb.HistoryEvent,
	maximumBufferedEvents int,
	maximumBufferedBytes int,
) *MutableStateImpl {
	return &MutableStateImpl{
		executionInfo: &persistencespb.WorkflowExecutionInfo{},
		executionState: &persistencespb.WorkflowExecutionState{
			State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		hBuilder:  newBufferedEventHistoryBuilder(persisted),
		chasmTree: &noopChasmTree{},
		config: &configs.Config{
			MaximumBufferedEventsBatch:       func() int { return maximumBufferedEvents },
			MaximumBufferedEventsSizeInBytes: func() int { return maximumBufferedBytes },
		},
	}
}

func newBufferedEventHistoryBuilder(persisted []*historypb.HistoryEvent) *historybuilder.HistoryBuilder {
	return historybuilder.New(
		bufferedEventTimeSource,
		func(count int) ([]int64, error) {
			ids := make([]int64, count)
			for i := range ids {
				ids[i] = int64(i + 1)
			}
			return ids, nil
		},
		1,
		1,
		persisted,
		nil,
		func() int { return 0 },
	)
}

func newBufferedSignalInput(payloadSize int, headerSize int) bufferedSignalInput {
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	header := make([]byte, headerSize)
	for i := range header {
		header[i] = byte(i)
	}

	return bufferedSignalInput{
		payloads: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
			Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
			Data:     payload,
		}}},
		header: &commonpb.Header{Fields: map[string]*commonpb.Payload{
			"trace": {Data: header},
		}},
	}
}

func addBufferedSignal(
	builder *historybuilder.HistoryBuilder,
	input bufferedSignalInput,
	requestID string,
) *historypb.HistoryEvent {
	return builder.AddWorkflowExecutionSignaledEvent(
		"buffer-size-signal",
		input.payloads,
		"buffer-size-benchmark",
		input.header,
		nil,
		requestID,
		nil,
	)
}

func addBufferedSignalToMutableState(
	state *MutableStateImpl,
	input bufferedSignalInput,
	requestID string,
) (*historypb.HistoryEvent, error) {
	return state.AddWorkflowExecutionSignaledEvent(
		"buffer-size-signal",
		input.payloads,
		"buffer-size-benchmark",
		input.header,
		nil,
		requestID,
		nil,
	)
}

func assertBufferedEventSize(
	t *testing.T,
	builder *historybuilder.HistoryBuilder,
	events ...*historypb.HistoryEvent,
) {
	t.Helper()
	want := bufferedEventsSerializedSize(events)
	if got := builder.SizeInBytesOfBufferedEvents(); got != want {
		t.Fatalf("buffered event size = %d, want %d", got, want)
	}
}

func bufferedEventsSerializedSize(events []*historypb.HistoryEvent) int {
	total := 0
	for _, event := range events {
		total += proto.Size(event)
	}
	return total
}
