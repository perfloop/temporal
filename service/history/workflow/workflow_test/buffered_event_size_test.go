package workflow_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	historyservice "go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
)

const (
	bufferedSignalBenchmarkEvents       = 50
	bufferedSignalBenchmarkPayloadBytes = 4 * 1024
	bufferedSignalBenchmarkByteLimit    = 2 * 1024 * 1024
)

func TestMutableStateBufferedEventSizeLimitsAndPersistedSize(t *testing.T) {
	config := tests.NewDynamicConfig()
	config.MaximumBufferedEventsBatch = func() int { return 1 }
	config.MaximumBufferedEventsSizeInBytes = func() int { return math.MaxInt }

	mutableState, _ := createMutableState(t, tests.LocalNamespaceEntry, config)
	startBufferedEventSizeWorkflow(t, mutableState, tests.LocalNamespaceEntry)
	closeBufferedEventSizeTransaction(t, context.Background(), mutableState)
	startBufferedEventSizeTransaction(t, mutableState, tests.LocalNamespaceEntry)

	event := addBufferedEventSizeSignal(t, mutableState, bufferedEventSizeSignalInput{index: 0, payloadBytes: 256})
	prePrincipalSize := proto.Size(event)

	config.MaximumBufferedEventsSizeInBytes = func() int { return prePrincipalSize }
	if !mutableState.BufferSizeAcceptable() {
		t.Fatal("buffer size at the exact byte limit was rejected")
	}
	atByteLimit := mutableState.GetApproximatePersistedSize()

	config.MaximumBufferedEventsSizeInBytes = func() int { return prePrincipalSize - 1 }
	if mutableState.BufferSizeAcceptable() {
		t.Fatal("buffer size one byte above the limit was accepted")
	}
	aboveByteLimit := mutableState.GetApproximatePersistedSize()
	if got, want := atByteLimit-aboveByteLimit, prePrincipalSize; got != want {
		t.Fatalf("persisted-size buffered-event contribution = %d, want %d", got, want)
	}

	config.MaximumBufferedEventsBatch = func() int { return 0 }
	config.MaximumBufferedEventsSizeInBytes = func() int { return math.MaxInt }
	if mutableState.BufferSizeAcceptable() {
		t.Fatal("one buffered event exceeded a zero-event limit but was accepted")
	}
	config.MaximumBufferedEventsBatch = func() int { return 1 }
	if !mutableState.BufferSizeAcceptable() {
		t.Fatal("one buffered event at the exact count limit was rejected")
	}

	principalContext := headers.SetPrincipal(context.Background(), &commonpb.Principal{Type: "user", Name: "buffered-size-test"})
	mutation := closeBufferedEventSizeTransaction(t, principalContext, mutableState)
	if len(mutation.NewBufferedEvents) != 1 {
		t.Fatalf("new buffered events = %d, want 1", len(mutation.NewBufferedEvents))
	}

	postPrincipalSize := proto.Size(mutation.NewBufferedEvents[0])
	config.MaximumBufferedEventsSizeInBytes = func() int { return postPrincipalSize }
	if !mutableState.BufferSizeAcceptable() {
		t.Fatal("principal-stamped buffered event at exact byte limit was rejected")
	}
	atStampedByteLimit := mutableState.GetApproximatePersistedSize()

	config.MaximumBufferedEventsSizeInBytes = func() int { return postPrincipalSize - 1 }
	if mutableState.BufferSizeAcceptable() {
		t.Fatal("principal-stamped buffered event one byte above the limit was accepted")
	}
	aboveStampedByteLimit := mutableState.GetApproximatePersistedSize()
	if got, want := atStampedByteLimit-aboveStampedByteLimit, postPrincipalSize; got != want {
		t.Fatalf("persisted-size stamped buffered-event contribution = %d, want %d", got, want)
	}
}

func TestMutableStateBufferedEventSizeReload(t *testing.T) {
	config := tests.NewDynamicConfig()
	config.MaximumBufferedEventsBatch = func() int { return 100 }
	config.MaximumBufferedEventsSizeInBytes = func() int { return math.MaxInt }
	fixture := newBufferedEventSizeFixture(t, config)

	mutableState := fixture.newMutableState()
	startBufferedEventSizeWorkflow(t, mutableState, fixture.namespaceEntry)
	closeBufferedEventSizeTransaction(t, context.Background(), mutableState)
	startBufferedEventSizeTransaction(t, mutableState, fixture.namespaceEntry)
	addBufferedEventSizeSignal(t, mutableState, bufferedEventSizeSignalInput{index: 1, payloadBytes: 512})
	mutation := closeBufferedEventSizeTransaction(
		t,
		headers.SetPrincipal(context.Background(), &commonpb.Principal{Type: "user", Name: "reload-test"}),
		mutableState,
	)
	if len(mutation.NewBufferedEvents) != 1 {
		t.Fatalf("new buffered events = %d, want 1", len(mutation.NewBufferedEvents))
	}
	bufferedEventSize := proto.Size(mutation.NewBufferedEvents[0])

	reloaded, err := workflow.NewMutableStateFromDB(
		fixture.shardContext,
		fixture.eventsCache,
		fixture.logger,
		fixture.namespaceEntry,
		mutableState.CloneToProto(),
		1,
	)
	if err != nil {
		t.Fatalf("NewMutableStateFromDB() failed: %v", err)
	}

	config.MaximumBufferedEventsSizeInBytes = func() int { return bufferedEventSize }
	if !reloaded.BufferSizeAcceptable() {
		t.Fatal("reloaded buffered event at exact byte limit was rejected")
	}
	withBufferedEvent := reloaded.GetApproximatePersistedSize()

	config.MaximumBufferedEventsSizeInBytes = func() int { return bufferedEventSize - 1 }
	if reloaded.BufferSizeAcceptable() {
		t.Fatal("reloaded buffered event one byte above the limit was accepted")
	}
	withoutBufferedEvent := reloaded.GetApproximatePersistedSize()
	if got, want := withBufferedEvent-withoutBufferedEvent, bufferedEventSize; got != want {
		t.Fatalf("reloaded persisted-size buffered-event contribution = %d, want %d", got, want)
	}
}

// BenchmarkMutableStateBufferSizeAcceptableSignalBurst measures active workflow
// signal commits while a workflow task remains open. Every signal starts and closes
// its own transaction, so each close reaches BufferSizeAcceptable and each next
// transaction rebuilds the HistoryBuilder from the current buffered-event state.
func BenchmarkMutableStateBufferSizeAcceptableSignalBurst(b *testing.B) {
	for _, scenario := range []bufferedSignalBenchmarkScenario{
		{
			name:           "events=1/payload=256B/within-byte-limit",
			events:         1,
			payload:        256,
			maxEvents:      100,
			maxBytes:       bufferedSignalBenchmarkByteLimit,
			expectBuffered: true,
		},
		{
			name:           "events=25/payload=1KiB/within-byte-limit",
			events:         25,
			payload:        1024,
			maxEvents:      100,
			maxBytes:       bufferedSignalBenchmarkByteLimit,
			expectBuffered: true,
		},
		{
			name:           "events=50/payload=4KiB/within-byte-limit",
			events:         bufferedSignalBenchmarkEvents,
			payload:        bufferedSignalBenchmarkPayloadBytes,
			maxEvents:      100,
			maxBytes:       bufferedSignalBenchmarkByteLimit,
			expectBuffered: true,
		},
		{
			name:           "events=100/payload=4KiB/count-boundary",
			events:         100,
			payload:        bufferedSignalBenchmarkPayloadBytes,
			maxEvents:      100,
			maxBytes:       bufferedSignalBenchmarkByteLimit,
			expectBuffered: true,
		},
		{
			name:           "events=50/payload=4KiB/over-byte-limit",
			events:         bufferedSignalBenchmarkEvents,
			payload:        bufferedSignalBenchmarkPayloadBytes,
			maxEvents:      100,
			maxBytes:       64 * 1024,
			expectBuffered: false,
		},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkMutableStateBufferSizeAcceptableSignalBurst(b, scenario)
		})
	}
}

type bufferedSignalBenchmarkScenario struct {
	name           string
	events         int
	payload        int
	maxEvents      int
	maxBytes       int
	expectBuffered bool
}

type bufferedEventSizeSignalInput struct {
	index        int
	payloadBytes int
	payload      *commonpb.Payloads
	header       *commonpb.Header
	signalName   string
	identity     string
	requestID    string
}

func benchmarkMutableStateBufferSizeAcceptableSignalBurst(b *testing.B, scenario bufferedSignalBenchmarkScenario) {
	config := tests.NewDynamicConfig()
	config.MaximumBufferedEventsBatch = func() int { return scenario.maxEvents }
	config.MaximumBufferedEventsSizeInBytes = func() int { return scenario.maxBytes }
	fixture := newBufferedEventSizeFixture(b, config)
	inputs := newBufferedEventSizeSignalInputs(scenario.events, scenario.payload)
	principalContext := headers.SetPrincipal(context.Background(), &commonpb.Principal{Type: "user", Name: "benchmark"})

	b.ReportAllocs()
	for b.Loop() {
		mutableState := fixture.newMutableState()
		startBufferedEventSizeWorkflow(b, mutableState, fixture.namespaceEntry)
		closeBufferedEventSizeTransaction(b, context.Background(), mutableState)

		for _, input := range inputs {
			startBufferedEventSizeTransaction(b, mutableState, fixture.namespaceEntry)
			addBufferedEventSizeSignal(b, mutableState, input)
			mutation := closeBufferedEventSizeTransaction(b, principalContext, mutableState)
			if scenario.expectBuffered && len(mutation.NewBufferedEvents) != 1 {
				b.Fatalf("new buffered events = %d, want 1", len(mutation.NewBufferedEvents))
			}
			if len(mutation.NewBufferedEvents) > 0 && mutation.NewBufferedEvents[0].GetPrincipal().GetName() != "benchmark" {
				b.Fatal("new buffered event did not retain the transaction principal")
			}
		}
	}
}

func newBufferedEventSizeSignalInputs(count, payloadBytes int) []bufferedEventSizeSignalInput {
	inputs := make([]bufferedEventSizeSignalInput, count)
	for i := range inputs {
		payload := make([]byte, payloadBytes)
		for j := range payload {
			payload[j] = byte((i + j) % 251)
		}
		inputs[i] = bufferedEventSizeSignalInput{
			index:        i,
			payloadBytes: payloadBytes,
			payload: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
				Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
				Data:     payload,
			}}},
			header: &commonpb.Header{Fields: map[string]*commonpb.Payload{
				"trace": {
					Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
					Data:     []byte(fmt.Sprintf("trace-%03d", i)),
				},
			}},
			signalName: fmt.Sprintf("signal-%03d", i),
			identity:   fmt.Sprintf("identity-%03d", i),
			requestID:  fmt.Sprintf("request-%03d", i),
		}
	}
	return inputs
}

func startBufferedEventSizeWorkflow(tb testing.TB, mutableState *workflow.MutableStateImpl, namespaceEntry *namespace.Namespace) {
	tb.Helper()

	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{
			WorkflowId: mutableState.GetWorkflowKey().WorkflowID,
			RunId:      mutableState.GetWorkflowKey().RunID,
		},
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: namespaceEntry.ID().String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: "buffered-size-workflow"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: "buffered-size-task-queue"},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(time.Second),
			},
		},
	)
	if err != nil {
		tb.Fatalf("AddWorkflowExecutionStartedEvent() failed: %v", err)
	}

	workflowTask, err := mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	if err != nil {
		tb.Fatalf("AddWorkflowTaskScheduledEvent() failed: %v", err)
	}
	_, _, err = mutableState.AddWorkflowTaskStartedEvent(
		workflowTask.ScheduledEventID,
		workflowTask.RequestID,
		workflowTask.TaskQueue,
		"",
		nil,
		nil,
		nil,
		false,
		nil,
		0,
	)
	if err != nil {
		tb.Fatalf("AddWorkflowTaskStartedEvent() failed: %v", err)
	}
}

func startBufferedEventSizeTransaction(tb testing.TB, mutableState *workflow.MutableStateImpl, namespaceEntry *namespace.Namespace) {
	tb.Helper()

	flushBeforeReady, err := mutableState.StartTransaction(namespaceEntry)
	if err != nil {
		tb.Fatalf("StartTransaction() failed: %v", err)
	}
	if flushBeforeReady {
		tb.Fatal("StartTransaction() unexpectedly requested a workflow-task flush")
	}
}

func addBufferedEventSizeSignal(tb testing.TB, mutableState *workflow.MutableStateImpl, input bufferedEventSizeSignalInput) *historypb.HistoryEvent {
	tb.Helper()

	event, err := mutableState.AddWorkflowExecutionSignaled(
		input.signalName,
		input.payload,
		input.identity,
		input.header,
		input.requestID,
		nil,
	)
	if err != nil {
		tb.Fatalf("AddWorkflowExecutionSignaled() failed: %v", err)
	}
	return event
}

func closeBufferedEventSizeTransaction(
	tb testing.TB,
	ctx context.Context,
	mutableState *workflow.MutableStateImpl,
) *persistence.WorkflowMutation {
	tb.Helper()

	mutation, _, err := mutableState.CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive)
	if err != nil {
		tb.Fatalf("CloseTransactionAsMutation() failed: %v", err)
	}
	if mutation == nil {
		tb.Fatal("CloseTransactionAsMutation() returned a nil mutation")
	}
	return mutation
}

type bufferedEventSizeFixture struct {
	shardContext   *shard.ContextTest
	eventsCache    *events.MockCache
	logger         log.Logger
	namespaceEntry *namespace.Namespace
}

func newBufferedEventSizeFixture(tb testing.TB, config *configs.Config) *bufferedEventSizeFixture {
	tb.Helper()

	controller := gomock.NewController(tb)
	shardContext := shard.NewTestContext(controller, &persistencespb.ShardInfo{}, config)
	registry := hsm.NewRegistry()
	if err := workflow.RegisterStateMachine(registry); err != nil {
		tb.Fatalf("RegisterStateMachine() failed: %v", err)
	}
	shardContext.SetStateMachineRegistry(registry)

	namespaceEntry := tests.LocalNamespaceEntry
	shardContext.Resource.NamespaceCache.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().
		ClusterNameForFailoverVersion(namespaceEntry.IsGlobalNamespace(), namespaceEntry.FailoverVersion(tests.WorkflowID)).
		Return(cluster.TestCurrentClusterName).AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	shardContext.Resource.ExecutionMgr.EXPECT().GetHistoryBranchUtil().
		Return(persistence.NewHistoryBranchUtil(serialization.NewSerializer())).AnyTimes()

	eventsCache := events.NewMockCache(controller)
	eventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	tb.Cleanup(shardContext.StopForTest)

	return &bufferedEventSizeFixture{
		shardContext:   shardContext,
		eventsCache:    eventsCache,
		logger:         log.NewNoopLogger(),
		namespaceEntry: namespaceEntry,
	}
}

func (f *bufferedEventSizeFixture) newMutableState() *workflow.MutableStateImpl {
	mutableState := workflow.NewMutableState(
		f.shardContext,
		f.eventsCache,
		f.logger,
		f.namespaceEntry,
		tests.WorkflowID,
		tests.RunID,
		time.Time{},
	)
	mutableState.GetExecutionInfo().NamespaceId = f.namespaceEntry.ID().String()
	mutableState.GetExecutionInfo().VersionHistories.Histories[0].Items = []*historyspb.VersionHistoryItem{{
		Version: 0,
		EventId: 1,
	}}
	return mutableState
}
