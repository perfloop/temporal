package workflow

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
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
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type bufferedSizeLimits struct {
	maxEvents int
	maxBytes  int
}

type bufferedSizeTestEnvironment struct {
	controller *gomock.Controller
	limits     *bufferedSizeLimits
	namespace  *namespace.Namespace
	shard      *shard.ContextTest
}

type bufferedSizeSignal struct {
	header   *commonpb.Header
	identity string
	input    *commonpb.Payloads
	name     string
}

func newBufferedSizeTestEnvironment(
	tb testing.TB,
	limits *bufferedSizeLimits,
) *bufferedSizeTestEnvironment {
	tb.Helper()

	controller := gomock.NewController(tb)
	config := tests.NewDynamicConfig()
	config.MaximumBufferedEventsBatch = func() int { return limits.maxEvents }
	config.MaximumBufferedEventsSizeInBytes = func() int { return limits.maxBytes }
	shardContext := shard.NewTestContext(controller, &persistencespb.ShardInfo{}, config)
	registry := hsm.NewRegistry()
	require.NoError(tb, RegisterStateMachine(registry))
	shardContext.SetStateMachineRegistry(registry)

	namespaceEntry := tests.LocalNamespaceEntry
	shardContext.Resource.NamespaceCache.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().
		ClusterNameForFailoverVersion(namespaceEntry.IsGlobalNamespace(), namespaceEntry.FailoverVersion(tests.WorkflowID)).
		Return(cluster.TestCurrentClusterName).
		AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	shardContext.Resource.ExecutionMgr.EXPECT().
		GetHistoryBranchUtil().
		Return(persistence.NewHistoryBranchUtil(serialization.NewSerializer())).
		AnyTimes()

	tb.Cleanup(shardContext.StopForTest)
	return &bufferedSizeTestEnvironment{
		controller: controller,
		limits:     limits,
		namespace:  namespaceEntry,
		shard:      shardContext,
	}
}

func (e *bufferedSizeTestEnvironment) newMutableState(tb testing.TB) *MutableStateImpl {
	tb.Helper()

	eventsCache := events.NewMockCache(e.controller)
	eventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	mutableState := NewMutableState(
		e.shard,
		eventsCache,
		log.NewNoopLogger(),
		e.namespace,
		tests.WorkflowID,
		tests.RunID,
		time.Time{},
	)
	mutableState.GetExecutionInfo().NamespaceId = e.namespace.ID().String()
	mutableState.GetExecutionInfo().VersionHistories.Histories[0].Items = []*historyspb.VersionHistoryItem{
		{EventId: 1, Version: 0},
	}
	return mutableState
}

func (e *bufferedSizeTestEnvironment) newMutableStateFromDB(
	tb testing.TB,
	record *persistencespb.WorkflowMutableState,
) *MutableStateImpl {
	tb.Helper()

	eventsCache := events.NewMockCache(e.controller)
	eventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	mutableState, err := NewMutableStateFromDB(
		e.shard,
		eventsCache,
		log.NewNoopLogger(),
		e.namespace,
		record,
		1,
	)
	require.NoError(tb, err)
	return mutableState
}

func startBufferedSizeWorkflow(
	tb testing.TB,
	mutableState *MutableStateImpl,
	namespaceEntry *namespace.Namespace,
) {
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
				TaskQueue:           &taskqueue.TaskQueue{Name: "buffered-size-task-queue"},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(time.Second),
			},
		},
	)
	require.NoError(tb, err)

	workflowTask, err := mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	require.NoError(tb, err)
	_, _, err = mutableState.AddWorkflowTaskStartedEvent(
		workflowTask.ScheduledEventID,
		workflowTask.RequestID,
		workflowTask.TaskQueue,
		"buffered-size-worker",
		nil,
		nil,
		nil,
		false,
		nil,
		0,
	)
	require.NoError(tb, err)
}

func newBufferedSizeSignal(index int, payloadSize int) bufferedSizeSignal {
	payload := make([]byte, payloadSize)
	for offset := range payload {
		payload[offset] = byte(index + offset)
	}
	headerPayload := []byte(fmt.Sprintf("buffered-size-header-%03d", index))
	return bufferedSizeSignal{
		header: &commonpb.Header{Fields: map[string]*commonpb.Payload{
			"routing": {
				Metadata: map[string][]byte{"encoding": []byte("json/plain")},
				Data:     headerPayload,
			},
		}},
		identity: fmt.Sprintf("buffered-size-sender-%03d", index),
		input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
			Metadata: map[string][]byte{"encoding": []byte("json/plain")},
			Data:     payload,
		}}},
		name: fmt.Sprintf("buffered-size-signal-%03d", index),
	}
}

func addBufferedSizeSignal(
	tb testing.TB,
	mutableState *MutableStateImpl,
	signal bufferedSizeSignal,
) *historypb.HistoryEvent {
	tb.Helper()

	event, err := mutableState.AddWorkflowExecutionSignaled(
		signal.name,
		signal.input,
		signal.identity,
		signal.header,
		signal.identity,
		nil,
	)
	require.NoError(tb, err)
	return event
}

func closeBufferedSizeMutation(
	tb testing.TB,
	mutableState *MutableStateImpl,
	ctx context.Context,
) *persistence.WorkflowMutation {
	tb.Helper()

	mutation, _, err := mutableState.CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive)
	require.NoError(tb, err)
	require.NotNil(tb, mutation)
	return mutation
}

func assertBufferedSize(t *testing.T, mutableState *MutableStateImpl, want int) {
	t.Helper()
	require.Equal(t, want, mutableState.hBuilder.SizeInBytesOfBufferedEvents())
}

func TestBufferedEventSizeLifecycle(t *testing.T) {
	limits := &bufferedSizeLimits{maxEvents: 1000, maxBytes: math.MaxInt}
	env := newBufferedSizeTestEnvironment(t, limits)
	principal := &commonpb.Principal{Type: "user", Name: "buffered-size-test"}
	ctx := headers.SetPrincipal(context.Background(), principal)

	mutableState := env.newMutableState(t)
	startBufferedSizeWorkflow(t, mutableState, env.namespace)

	addBufferedSizeSignal(t, mutableState, newBufferedSizeSignal(1, 256))
	firstMutation := closeBufferedSizeMutation(t, mutableState, ctx)
	require.Len(t, firstMutation.NewBufferedEvents, 1)
	require.Equal(t, principal, firstMutation.NewBufferedEvents[0].Principal)
	firstSize := proto.Size(firstMutation.NewBufferedEvents[0])
	assertBufferedSize(t, mutableState, firstSize)

	_, err := mutableState.StartTransaction(env.namespace)
	require.NoError(t, err)
	secondSignal := addBufferedSizeSignal(t, mutableState, newBufferedSizeSignal(2, 1024))
	assertBufferedSize(t, mutableState, firstSize+proto.Size(secondSignal))

	memoryTimer := mutableState.hBuilder.AddTimerFiredEvent(10, "memory-timer")
	assertBufferedSize(t, mutableState, firstSize+proto.Size(secondSignal)+proto.Size(memoryTimer))
	require.Equal(t, memoryTimer, mutableState.hBuilder.GetAndRemoveTimerFireEvent("memory-timer"))
	assertBufferedSize(t, mutableState, firstSize+proto.Size(secondSignal))

	secondMutation := closeBufferedSizeMutation(t, mutableState, ctx)
	require.Len(t, secondMutation.NewBufferedEvents, 1)
	secondSize := proto.Size(secondMutation.NewBufferedEvents[0])
	expectedSize := firstSize + secondSize
	assertBufferedSize(t, mutableState, expectedSize)

	_, err = mutableState.StartTransaction(env.namespace)
	require.NoError(t, err)
	databaseTimer := mutableState.hBuilder.AddTimerFiredEvent(11, "database-timer")
	databaseMutation := closeBufferedSizeMutation(t, mutableState, ctx)
	require.Len(t, databaseMutation.NewBufferedEvents, 1)
	databaseTimerSize := proto.Size(databaseMutation.NewBufferedEvents[0])
	expectedSize += databaseTimerSize
	assertBufferedSize(t, mutableState, expectedSize)

	_, err = mutableState.StartTransaction(env.namespace)
	require.NoError(t, err)
	require.Equal(t, databaseTimer, mutableState.hBuilder.GetAndRemoveTimerFireEvent("database-timer"))
	expectedSize -= databaseTimerSize
	assertBufferedSize(t, mutableState, expectedSize)
	closeBufferedSizeMutation(t, mutableState, ctx)
	assertBufferedSize(t, mutableState, expectedSize)

	reloaded := env.newMutableStateFromDB(t, mutableState.CloneToProto())
	assertBufferedSize(t, reloaded, expectedSize)
	_, err = reloaded.StartTransaction(env.namespace)
	require.NoError(t, err)
	thirdSignal := addBufferedSizeSignal(t, reloaded, newBufferedSizeSignal(3, 512))
	assertBufferedSize(t, reloaded, expectedSize+proto.Size(thirdSignal))
}

func TestBufferedEventSizeCloneOutputIsolated(t *testing.T) {
	limits := &bufferedSizeLimits{maxEvents: 1000, maxBytes: math.MaxInt}
	env := newBufferedSizeTestEnvironment(t, limits)
	ctx := headers.SetPrincipal(context.Background(), &commonpb.Principal{Type: "user", Name: "buffered-size-test"})

	mutableState := env.newMutableState(t)
	startBufferedSizeWorkflow(t, mutableState, env.namespace)
	addBufferedSizeSignal(t, mutableState, newBufferedSizeSignal(4, 256))
	mutation := closeBufferedSizeMutation(t, mutableState, ctx)
	require.Len(t, mutation.NewBufferedEvents, 1)
	expectedSize := mutableState.hBuilder.SizeInBytesOfBufferedEvents()

	externalRecord := mutableState.CloneToProto()
	require.Len(t, externalRecord.BufferedEvents, 1)
	externalRecord.BufferedEvents[0].EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
	externalRecord.BufferedEvents[0].Attributes = nil
	assertBufferedSize(t, mutableState, expectedSize)
}

func TestBufferedEventSizeFlushAndDiscard(t *testing.T) {
	limits := &bufferedSizeLimits{maxEvents: 1000, maxBytes: math.MaxInt}
	env := newBufferedSizeTestEnvironment(t, limits)

	normalFlush := env.newMutableState(t)
	startBufferedSizeWorkflow(t, normalFlush, env.namespace)
	addBufferedSizeSignal(t, normalFlush, newBufferedSizeSignal(4, 256))
	require.Positive(t, normalFlush.hBuilder.SizeInBytesOfBufferedEvents())
	normalFlush.hBuilder.FlushBufferToCurrentBatch()
	assertBufferedSize(t, normalFlush, 0)

	finishedDiscard := env.newMutableState(t)
	startBufferedSizeWorkflow(t, finishedDiscard, env.namespace)
	addBufferedSizeSignal(t, finishedDiscard, newBufferedSizeSignal(5, 256))
	require.Positive(t, finishedDiscard.hBuilder.SizeInBytesOfBufferedEvents())
	_, _ = finishedDiscard.hBuilder.AddWorkflowExecutionTerminatedEvent("finished", nil, "buffered-size-test", nil)
	finishedDiscard.hBuilder.FlushBufferToCurrentBatch()
	assertBufferedSize(t, finishedDiscard, 0)
}

func TestBufferedEventSizeLimitBoundaries(t *testing.T) {
	limits := &bufferedSizeLimits{maxEvents: 1000, maxBytes: math.MaxInt}
	env := newBufferedSizeTestEnvironment(t, limits)

	byteBoundary := env.newMutableState(t)
	startBufferedSizeWorkflow(t, byteBoundary, env.namespace)
	byteEvent := addBufferedSizeSignal(t, byteBoundary, newBufferedSizeSignal(6, 256))
	limits.maxBytes = proto.Size(byteEvent)
	require.True(t, byteBoundary.BufferSizeAcceptable())
	limits.maxBytes--
	require.False(t, byteBoundary.BufferSizeAcceptable())

	limits.maxBytes = math.MaxInt
	limits.maxEvents = 1
	countBoundary := env.newMutableState(t)
	startBufferedSizeWorkflow(t, countBoundary, env.namespace)
	addBufferedSizeSignal(t, countBoundary, newBufferedSizeSignal(7, 64))
	require.True(t, countBoundary.BufferSizeAcceptable())
	addBufferedSizeSignal(t, countBoundary, newBufferedSizeSignal(8, 64))
	require.False(t, countBoundary.BufferSizeAcceptable())
}

func BenchmarkBufferSizeAcceptableActiveSignalCadence(b *testing.B) {
	limits := &bufferedSizeLimits{maxEvents: 1000, maxBytes: math.MaxInt}
	env := newBufferedSizeTestEnvironment(b, limits)
	ctx := headers.SetPrincipal(context.Background(), &commonpb.Principal{Type: "user", Name: "buffered-size-benchmark"})

	type scenario struct {
		count       int
		payloadSize int
	}
	scenarios := []scenario{
		{count: 1, payloadSize: 256},
		{count: 25, payloadSize: 1024},
		{count: 50, payloadSize: 4096},
	}
	signals := make([][]bufferedSizeSignal, len(scenarios))
	for scenarioIndex, scenario := range scenarios {
		signals[scenarioIndex] = make([]bufferedSizeSignal, scenario.count)
		for signalIndex := range signals[scenarioIndex] {
			signals[scenarioIndex][signalIndex] = newBufferedSizeSignal(
				scenarioIndex*100+signalIndex,
				scenario.payloadSize,
			)
		}
	}

	for b.Loop() {
		for _, scenarioSignals := range signals {
			mutableState := env.newMutableState(b)
			startBufferedSizeWorkflow(b, mutableState, env.namespace)
			for signalIndex, signal := range scenarioSignals {
				addBufferedSizeSignal(b, mutableState, signal)
				mutation := closeBufferedSizeMutation(b, mutableState, ctx)
				if len(mutation.NewBufferedEvents) != 1 || mutation.NewBufferedEvents[0].GetPrincipal() == nil {
					b.Fatalf("signal %d did not produce a principal-stamped buffered event", signalIndex)
				}
				if signalIndex+1 < len(scenarioSignals) {
					_, err := mutableState.StartTransaction(env.namespace)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		}
	}
}
