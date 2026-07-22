package workflow_test

import (
	"context"
	"math"
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
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
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type bufferedEventSizeHarness struct {
	shardContext   *shard.ContextTest
	eventsCache    *events.MockCache
	namespaceEntry *namespace.Namespace
	maxEvents      int
	maxBytes       int
}

func newBufferedEventSizeHarness(tb testing.TB, maxEvents, maxBytes int) *bufferedEventSizeHarness {
	tb.Helper()

	h := &bufferedEventSizeHarness{
		namespaceEntry: tests.LocalNamespaceEntry,
		maxEvents:      maxEvents,
		maxBytes:       maxBytes,
	}
	config := tests.NewDynamicConfig()
	config.MaximumBufferedEventsBatch = func() int { return h.maxEvents }
	config.MaximumBufferedEventsSizeInBytes = func() int { return h.maxBytes }

	controller := gomock.NewController(tb)
	h.shardContext = shard.NewTestContext(controller, &persistencespb.ShardInfo{}, config)
	registry := hsm.NewRegistry()
	if err := workflow.RegisterStateMachine(registry); err != nil {
		tb.Fatalf("register workflow state machine: %v", err)
	}
	h.shardContext.SetStateMachineRegistry(registry)
	tb.Cleanup(h.shardContext.StopForTest)

	namespaceCache := h.shardContext.Resource.NamespaceCache
	namespaceCache.EXPECT().GetNamespaceByID(h.namespaceEntry.ID()).Return(h.namespaceEntry, nil).AnyTimes()

	clusterMetadata := h.shardContext.Resource.ClusterMetadata
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(
		h.namespaceEntry.IsGlobalNamespace(),
		h.namespaceEntry.FailoverVersion(tests.WorkflowID),
	).Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()

	executionManager := h.shardContext.Resource.ExecutionMgr
	executionManager.EXPECT().GetHistoryBranchUtil().Return(
		persistence.NewHistoryBranchUtil(serialization.NewSerializer()),
	).AnyTimes()

	h.eventsCache = events.NewMockCache(controller)
	h.eventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	h.shardContext.SetEventsCacheForTesting(h.eventsCache)

	return h
}

func (h *bufferedEventSizeHarness) newActiveState(tb testing.TB) (*workflow.MutableStateImpl, *historyi.WorkflowTaskInfo) {
	tb.Helper()

	mutableState := workflow.NewMutableState(
		h.shardContext,
		h.eventsCache,
		log.NewNoopLogger(),
		h.namespaceEntry,
		tests.WorkflowID,
		tests.RunID,
		time.Time{},
	)
	mutableState.GetExecutionInfo().NamespaceId = h.namespaceEntry.ID().String()
	mutableState.GetExecutionInfo().VersionHistories.Histories[0].Items = []*historyspb.VersionHistoryItem{
		{Version: 0, EventId: 1},
	}

	if _, err := mutableState.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{
			WorkflowId: mutableState.GetWorkflowKey().WorkflowID,
			RunId:      mutableState.GetWorkflowKey().RunID,
		},
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: h.namespaceEntry.ID().String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: "buffered-size-workflow"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: "buffered-size-task-queue"},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(time.Second),
			},
		},
	); err != nil {
		tb.Fatalf("start workflow: %v", err)
	}

	workflowTask, err := mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	if err != nil {
		tb.Fatalf("schedule workflow task: %v", err)
	}
	_, workflowTask, err = mutableState.AddWorkflowTaskStartedEvent(
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
	if err != nil {
		tb.Fatalf("start workflow task: %v", err)
	}
	return mutableState, workflowTask
}

func (h *bufferedEventSizeHarness) loadActiveState(
	tb testing.TB,
	record *persistencespb.WorkflowMutableState,
) *workflow.MutableStateImpl {
	tb.Helper()

	mutableState, err := workflow.NewMutableStateFromDB(
		h.shardContext,
		h.eventsCache,
		log.NewNoopLogger(),
		h.namespaceEntry,
		record,
		1,
	)
	if err != nil {
		tb.Fatalf("load mutable state: %v", err)
	}
	if _, err := mutableState.StartTransaction(h.namespaceEntry); err != nil {
		tb.Fatalf("start loaded transaction: %v", err)
	}
	return mutableState
}

func addBufferedSizeSignal(
	tb testing.TB,
	mutableState *workflow.MutableStateImpl,
	payload *commonpb.Payloads,
) *historypb.HistoryEvent {
	tb.Helper()

	event, err := mutableState.AddWorkflowExecutionSignaled(
		"buffered-size-signal",
		payload,
		"buffered-size-test",
		nil,
		"",
		nil,
	)
	if err != nil {
		tb.Fatalf("add buffered signal: %v", err)
	}
	return event
}

func closeBufferedSizeTransaction(
	tb testing.TB,
	mutableState *workflow.MutableStateImpl,
	ctx context.Context,
) *persistence.WorkflowMutation {
	tb.Helper()

	mutation, _, err := mutableState.CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive)
	if err != nil {
		tb.Fatalf("close active transaction: %v", err)
	}
	return mutation
}

func startBufferedSizeTransaction(
	tb testing.TB,
	h *bufferedEventSizeHarness,
	mutableState *workflow.MutableStateImpl,
) {
	tb.Helper()

	if _, err := mutableState.StartTransaction(h.namespaceEntry); err != nil {
		tb.Fatalf("start transaction: %v", err)
	}
}

func bufferedSignalPayload(size int, seed byte) *commonpb.Payloads {
	data := make([]byte, size)
	for i := range data {
		data[i] = seed + byte(i)
	}
	return &commonpb.Payloads{
		Payloads: []*commonpb.Payload{{
			Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
			Data:     data,
		}},
	}
}

func bufferedEventsSize(events []*historypb.HistoryEvent) int {
	total := 0
	for _, event := range events {
		total += proto.Size(event)
	}
	return total
}

func assertBufferedSizeLimit(
	tb testing.TB,
	h *bufferedEventSizeHarness,
	mutableState *workflow.MutableStateImpl,
	want int,
) {
	tb.Helper()

	oldMaxEvents, oldMaxBytes := h.maxEvents, h.maxBytes
	h.maxEvents = math.MaxInt
	h.maxBytes = want
	if !mutableState.BufferSizeAcceptable() {
		tb.Fatalf("buffer total %d should be accepted at its exact byte limit", want)
	}
	if want > 0 {
		h.maxBytes = want - 1
		if mutableState.BufferSizeAcceptable() {
			tb.Fatalf("buffer total %d should exceed byte limit %d", want, want-1)
		}
	}
	h.maxEvents, h.maxBytes = oldMaxEvents, oldMaxBytes
}

func TestBufferedEventSizeLifecycle(t *testing.T) {
	t.Run("count limit remains strict-greater-than", func(t *testing.T) {
		h := newBufferedEventSizeHarness(t, 1, math.MaxInt)
		mutableState, _ := h.newActiveState(t)

		addBufferedSizeSignal(t, mutableState, bufferedSignalPayload(256, 1))
		if !mutableState.BufferSizeAcceptable() {
			t.Fatal("one buffered event should be accepted at a count limit of one")
		}
		addBufferedSizeSignal(t, mutableState, bufferedSignalPayload(256, 2))
		if mutableState.BufferSizeAcceptable() {
			t.Fatal("two buffered events should exceed a count limit of one")
		}
	})

	t.Run("reload isolates source and preserves byte limits through active commits", func(t *testing.T) {
		h := newBufferedEventSizeHarness(t, 100, math.MaxInt)
		mutableState, _ := h.newActiveState(t)
		addBufferedSizeSignal(t, mutableState, bufferedSignalPayload(1024, 3))
		closeBufferedSizeTransaction(t, mutableState, context.Background())

		sourceRecord := mutableState.CloneToProto()
		expectedRecord := proto.Clone(sourceRecord).(*persistencespb.WorkflowMutableState)
		loaded := h.loadActiveState(t, sourceRecord)

		sourcePayload := sourceRecord.GetBufferedEvents()[0].GetWorkflowExecutionSignaledEventAttributes().GetInput().GetPayloads()[0]
		sourcePayload.Data = append(sourcePayload.Data, 4)

		if got := loaded.CloneToProto(); !proto.Equal(expectedRecord, got) {
			t.Fatal("reloaded mutable state changed after a size-affecting mutation of its source record")
		}
		assertBufferedSizeLimit(t, h, loaded, bufferedEventsSize(expectedRecord.GetBufferedEvents()))

		added := addBufferedSizeSignal(t, loaded, bufferedSignalPayload(1024, 5))
		assertBufferedSizeLimit(
			t,
			h,
			loaded,
			bufferedEventsSize(expectedRecord.GetBufferedEvents())+proto.Size(added),
		)

		principal := &commonpb.Principal{Type: "user", Name: "buffered-size-principal"}
		mutation := closeBufferedSizeTransaction(t, loaded, headers.SetPrincipal(context.Background(), principal))
		if len(mutation.NewBufferedEvents) != 1 || mutation.NewBufferedEvents[0].GetPrincipal().GetName() != principal.GetName() {
			t.Fatal("active signal commit did not retain its principal-stamped buffered event")
		}

		postCommit := loaded.CloneToProto()
		assertBufferedSizeLimit(t, h, loaded, bufferedEventsSize(postCommit.GetBufferedEvents()))
		startBufferedSizeTransaction(t, h, loaded)
		assertBufferedSizeLimit(t, h, loaded, bufferedEventsSize(postCommit.GetBufferedEvents()))

		addBufferedSizeSignal(t, loaded, bufferedSignalPayload(1024, 6))
		h.maxBytes = 0
		if loaded.BufferSizeAcceptable() {
			t.Fatal("non-empty buffered events should exceed a zero byte limit")
		}
		closeBufferedSizeTransaction(t, loaded, headers.SetPrincipal(context.Background(), principal))
		if got := loaded.CloneToProto().GetBufferedEvents(); len(got) != 0 {
			t.Fatalf("forced recovery should flush buffered events, got %d", len(got))
		}
		if !loaded.BufferSizeAcceptable() {
			t.Fatal("an empty buffer should be accepted after forced recovery")
		}
	})

	t.Run("timer removal updates persisted and in-memory buffer totals", func(t *testing.T) {
		h := newBufferedEventSizeHarness(t, 100, math.MaxInt)
		mutableState, workflowTask := h.newActiveState(t)

		const persistedTimerID = "persisted-buffered-timer"
		if _, _, err := mutableState.AddTimerStartedEvent(workflowTask.StartedEventID, &commandpb.StartTimerCommandAttributes{
			TimerId:            persistedTimerID,
			StartToFireTimeout: durationpb.New(time.Second),
		}); err != nil {
			t.Fatalf("start persisted timer: %v", err)
		}
		if _, err := mutableState.AddTimerFiredEvent(persistedTimerID); err != nil {
			t.Fatalf("fire persisted timer: %v", err)
		}
		closeBufferedSizeTransaction(t, mutableState, context.Background())
		startBufferedSizeTransaction(t, h, mutableState)

		workflowTask = mutableState.GetStartedWorkflowTask()
		if workflowTask == nil {
			t.Fatal("expected started workflow task after reload")
		}
		if _, err := mutableState.AddTimerCanceledEvent(
			workflowTask.StartedEventID,
			&commandpb.CancelTimerCommandAttributes{TimerId: persistedTimerID},
			"buffered-size-test",
		); err != nil {
			t.Fatalf("remove persisted timer fired event: %v", err)
		}

		const inMemoryTimerID = "in-memory-buffered-timer"
		if _, _, err := mutableState.AddTimerStartedEvent(workflowTask.StartedEventID, &commandpb.StartTimerCommandAttributes{
			TimerId:            inMemoryTimerID,
			StartToFireTimeout: durationpb.New(time.Second),
		}); err != nil {
			t.Fatalf("start in-memory timer: %v", err)
		}
		if _, err := mutableState.AddTimerFiredEvent(inMemoryTimerID); err != nil {
			t.Fatalf("fire in-memory timer: %v", err)
		}
		if _, err := mutableState.AddTimerCanceledEvent(
			workflowTask.StartedEventID,
			&commandpb.CancelTimerCommandAttributes{TimerId: inMemoryTimerID},
			"buffered-size-test",
		); err != nil {
			t.Fatalf("remove in-memory timer fired event: %v", err)
		}

		h.maxBytes = 0
		if !mutableState.BufferSizeAcceptable() {
			t.Fatal("removing both timer-fired events should leave a zero-byte buffer")
		}
		closeBufferedSizeTransaction(t, mutableState, context.Background())
		if got := mutableState.CloneToProto().GetBufferedEvents(); len(got) != 0 {
			t.Fatalf("timer removals should persist an empty buffered-event output, got %d", len(got))
		}
		assertBufferedSizeLimit(t, h, mutableState, 0)
	})
}
