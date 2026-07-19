package history

import (
	"context"
	"fmt"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

// Keep the generated Engine mock synchronized with its interface. The handler
// test below exercises the real implementation through that same interface.
var _ historyi.Engine = (*historyEngineImpl)(nil)
var _ historyi.Engine = (*historyi.MockEngine)(nil)

type activityCompletionWork struct {
	execution        *commonpb.WorkflowExecution
	request          *historyservice.RespondActivityTaskCompletedRequest
	scheduledEventID int64
}

type activityCompletionHarness struct {
	shardContext     *shard.ContextTest
	handler          *Handler
	workflowCache    wcache.Cache
	eventsCache      events.Cache
	executionManager *persistence.MockExecutionManager
	states           map[string]*persistencespb.WorkflowMutableState
	currentRuns      map[string]string
}

func newActivityCompletionHarness(t testing.TB, cacheSize int, verifyUpdate bool) *activityCompletionHarness {
	t.Helper()

	if cacheSize < 16 {
		cacheSize = 16
	}
	controller := gomock.NewController(t)
	config := tests.NewDynamicConfig()
	config.HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(cacheSize)

	shardContext := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
	)
	shardContext.SetLoggers(log.NewNoopLogger())
	t.Cleanup(shardContext.StopForTest)

	executionManager := shardContext.Resource.ExecutionMgr
	eventsCache := events.NewHostLevelEventsCache(
		executionManager,
		shardContext.GetConfig(),
		shardContext.GetMetricsHandler(),
		shardContext.GetLogger(),
		false,
	)
	shardContext.SetEventsCacheForTesting(eventsCache)

	registry := hsm.NewRegistry()
	if err := workflow.RegisterStateMachine(registry); err != nil {
		t.Fatalf("register workflow state machines: %v", err)
	}
	shardContext.SetStateMachineRegistry(registry)

	clusterMetadata := shardContext.Resource.ClusterMetadata
	clusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, int64(0)).Return(cluster.TestCurrentClusterName).AnyTimes()

	namespaceRegistry := shardContext.Resource.NamespaceCache
	namespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	namespaceRegistry.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	workflowCache := wcache.NewHostLevelCache(
		shardContext.GetConfig(),
		shardContext.GetLogger(),
		metrics.NoopMetricsHandler,
	)
	h := &activityCompletionHarness{
		shardContext:     shardContext,
		workflowCache:    workflowCache,
		eventsCache:      eventsCache,
		executionManager: executionManager,
		states:           make(map[string]*persistencespb.WorkflowMutableState),
		currentRuns:      make(map[string]string),
	}

	executionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
			state, ok := h.states[activityCompletionExecutionKey(request.WorkflowID, request.RunID)]
			if !ok {
				return nil, fmt.Errorf("missing prepared execution %q/%q", request.WorkflowID, request.RunID)
			}
			return &persistence.GetWorkflowExecutionResponse{
				State: proto.Clone(state).(*persistencespb.WorkflowMutableState),
			}, nil
		},
	)
	executionManager.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, request *persistence.GetCurrentExecutionRequest) (*persistence.GetCurrentExecutionResponse, error) {
			runID, ok := h.currentRuns[request.WorkflowID]
			if !ok {
				return nil, fmt.Errorf("missing prepared current execution %q", request.WorkflowID)
			}
			return &persistence.GetCurrentExecutionResponse{RunID: runID}, nil
		},
	)
	if verifyUpdate {
		executionManager.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
			func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
				if len(request.UpdateWorkflowMutation.DeleteActivityInfos) != 1 {
					return nil, fmt.Errorf("activity completion persisted %d deleted activities, want 1", len(request.UpdateWorkflowMutation.DeleteActivityInfos))
				}
				if len(request.UpdateWorkflowEvents) == 0 {
					return nil, fmt.Errorf("activity completion persisted no history events")
				}
				return tests.UpdateWorkflowExecutionResponse, nil
			},
		)
	} else {
		executionManager.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes().Return(tests.UpdateWorkflowExecutionResponse, nil)
	}
	shardContext.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	eventNotifier := events.NewNotifier(
		clock.NewRealTimeSource(),
		shardContext.GetMetricsHandler(),
		func(namespaceID namespace.ID, workflowID string) int32 {
			return int32(len(namespaceID.String() + "_" + workflowID))
		},
	)
	eventNotifier.Start()
	t.Cleanup(eventNotifier.Stop)

	queueProcessors := make(map[tasks.Category]queues.Queue)
	for _, category := range []tasks.Category{
		tasks.CategoryTransfer,
		tasks.CategoryTimer,
		tasks.CategoryVisibility,
		tasks.CategoryArchival,
		tasks.CategoryMemoryTimer,
		tasks.CategoryOutbound,
	} {
		queue := queues.NewMockQueue(controller)
		queue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
		queueProcessors[category] = queue
	}
	engine := &historyEngineImpl{
		shardContext:               shardContext,
		logger:                     log.NewNoopLogger(),
		eventNotifier:              eventNotifier,
		tokenSerializer:            tasktoken.NewSerializer(),
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(shardContext, workflowCache),
		queueProcessors:            queueProcessors,
	}
	shardContext.SetEngineForTesting(engine)

	shardController := shard.NewMockController(controller)
	shardController.EXPECT().GetShardByNamespaceWorkflow(gomock.Any(), gomock.Any()).Return(shardContext, nil).AnyTimes()
	h.handler = &Handler{
		tokenSerializer: tasktoken.NewSerializer(),
		controller:      shardController,
	}

	return h
}

func (h *activityCompletionHarness) newWork(t testing.TB, index int, omitRunID bool) activityCompletionWork {
	t.Helper()

	workflowID := fmt.Sprintf("activity-completion-benchmark-workflow-%d", index)
	runID := fmt.Sprintf("00000000-0000-0000-0000-%012d", index+1)
	execution := &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID}
	const (
		activityID   = "activity-completion-benchmark-activity"
		activityType = "activity-completion-benchmark-type"
		taskQueue    = "activity-completion-benchmark-queue"
		identity     = "activity-completion-benchmark-worker"
	)

	mutableState := workflow.TestLocalMutableState(
		h.shardContext,
		h.eventsCache,
		tests.LocalNamespaceEntry,
		workflowID,
		runID,
		log.NewNoopLogger(),
	)
	addWorkflowExecutionStartedEvent(
		mutableState,
		execution,
		"activity-completion-benchmark-workflow-type",
		taskQueue,
		payloads.EncodeString("workflow input"),
		time.Minute,
		time.Minute,
		time.Minute,
		identity,
	)
	workflowTask := addWorkflowTaskScheduledEvent(mutableState)
	workflowTaskStarted := addWorkflowTaskStartedEvent(mutableState, workflowTask.ScheduledEventID, taskQueue, identity)
	workflowTaskCompleted := completeBenchmarkWorkflowTask(
		t,
		mutableState,
		workflowTask.ScheduledEventID,
		workflowTaskStarted.EventId,
		identity,
	)
	activityScheduled, activityInfo := addActivityTaskScheduledEvent(
		mutableState,
		workflowTaskCompleted.EventId,
		activityID,
		activityType,
		taskQueue,
		payloads.EncodeString("activity input"),
		time.Minute,
		time.Minute,
		time.Minute,
		time.Minute,
	)
	activityStarted := addActivityTaskStartedEvent(mutableState, activityScheduled.EventId, identity)
	if activityStarted == nil {
		t.Fatal("start prepared activity")
	}
	if activityInfo.GetAttempt() == 0 {
		t.Fatal("prepared activity has no attempt")
	}

	state := workflow.TestCloneToProto(context.Background(), mutableState)
	h.states[activityCompletionExecutionKey(workflowID, runID)] = state
	h.currentRuns[workflowID] = runID

	tokenRunID := runID
	if omitRunID {
		tokenRunID = ""
	}
	clock := proto.Clone(h.shardContext.CurrentVectorClock()).(*clockspb.VectorClock)
	token, err := tasktoken.NewSerializer().Serialize(tasktoken.NewActivityTaskToken(
		tests.NamespaceID.String(),
		workflowID,
		tokenRunID,
		activityScheduled.EventId,
		activityID,
		activityType,
		activityInfo.GetAttempt(),
		clock,
		activityInfo.GetVersion(),
		activityInfo.GetStartVersion(),
		nil,
	))
	if err != nil {
		t.Fatalf("serialize activity task token: %v", err)
	}

	work := activityCompletionWork{
		execution:        execution,
		scheduledEventID: activityScheduled.EventId,
		request: &historyservice.RespondActivityTaskCompletedRequest{
			NamespaceId: tests.NamespaceID.String(),
			CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
				TaskToken: token,
				Result:    payloads.EncodeString("activity completion result"),
				Identity:  identity,
			},
		},
	}
	h.preloadPendingActivity(t, work)
	return work
}

func completeBenchmarkWorkflowTask(
	t testing.TB,
	mutableState historyi.MutableState,
	scheduledEventID int64,
	startedEventID int64,
	identity string,
) *historypb.HistoryEvent {
	t.Helper()

	workflowTask := mutableState.GetWorkflowTaskByID(scheduledEventID)
	if workflowTask == nil {
		t.Fatalf("missing prepared workflow task %d", scheduledEventID)
	}
	if workflowTask.StartedEventID != startedEventID {
		t.Fatalf("prepared workflow task started event = %d, want %d", workflowTask.StartedEventID, startedEventID)
	}
	event, err := mutableState.AddWorkflowTaskCompletedEvent(
		workflowTask,
		&workflowservice.RespondWorkflowTaskCompletedRequest{Identity: identity},
		defaultWorkflowTaskCompletionLimits,
	)
	if err != nil {
		t.Fatalf("complete prepared workflow task: %v", err)
	}
	mutableState.FlushBufferedEvents()
	return event
}

func (h *activityCompletionHarness) preloadPendingActivity(t testing.TB, work activityCompletionWork) {
	t.Helper()

	workflowContext, release, err := h.workflowCache.GetOrCreateWorkflowExecution(
		context.Background(),
		h.shardContext,
		tests.NamespaceID,
		work.execution,
		locks.PriorityHigh,
	)
	if err != nil {
		t.Fatalf("preload workflow context: %v", err)
	}
	_, loadErr := workflowContext.LoadMutableState(context.Background(), h.shardContext)
	release(loadErr)
	if loadErr != nil {
		t.Fatalf("preload mutable state: %v", loadErr)
	}
}

func (h *activityCompletionHarness) assertCompleted(t testing.TB, work activityCompletionWork) {
	t.Helper()

	workflowContext, release, err := h.workflowCache.GetOrCreateWorkflowExecution(
		context.Background(),
		h.shardContext,
		tests.NamespaceID,
		work.execution,
		locks.PriorityHigh,
	)
	if err != nil {
		t.Fatalf("load completed workflow context: %v", err)
	}
	mutableState, loadErr := workflowContext.LoadMutableState(context.Background(), h.shardContext)
	release(loadErr)
	if loadErr != nil {
		t.Fatalf("load completed mutable state: %v", loadErr)
	}
	if _, pending := mutableState.GetActivityInfo(work.scheduledEventID); pending {
		t.Fatalf("activity %d is still pending after completion", work.scheduledEventID)
	}
	if !mutableState.HasPendingWorkflowTask() {
		t.Fatal("activity completion did not schedule the next workflow task")
	}
}

func activityCompletionExecutionKey(workflowID, runID string) string {
	return workflowID + "\x00" + runID
}

func TestRespondActivityTaskCompletedHandlerRejectsInvalidToken(t *testing.T) {
	controller := gomock.NewController(t)
	handler := &Handler{
		tokenSerializer: tasktoken.NewSerializer(),
		controller:      shard.NewMockController(controller),
	}

	response, err := handler.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: []byte{0xff},
		},
	})
	if response != nil {
		t.Fatal("invalid task token returned a response")
	}
	if err != consts.ErrDeserializingToken {
		t.Fatalf("invalid task token error = %v, want %v", err, consts.ErrDeserializingToken)
	}
}

func TestRespondActivityTaskCompletedHandlerCompletesFreshPendingActivity(t *testing.T) {
	h := newActivityCompletionHarness(t, 2, true)
	for index, omitRunID := range []bool{false, true} {
		t.Run(fmt.Sprintf("omit-run-id=%t", omitRunID), func(t *testing.T) {
			work := h.newWork(t, index, omitRunID)
			response, err := h.handler.RespondActivityTaskCompleted(context.Background(), work.request)
			if err != nil {
				t.Fatalf("complete activity through handler: %v", err)
			}
			if response == nil {
				t.Fatal("complete activity through handler returned a nil response")
			}
			h.assertCompleted(t, work)
		})
	}
}

// BenchmarkRespondActivityTaskCompletedTokenHandoff measures one normal
// mutable-state-backed completion through Handler -> Engine -> API. Every
// timed operation uses a distinct, preloaded workflow context with one valid
// started activity. That makes completion consume a real pending activity and
// execute the production mutable-state update, while setup and state loading
// remain outside the timed operation.
func BenchmarkRespondActivityTaskCompletedTokenHandoff(b *testing.B) {
	// B.Loop chooses its iteration count after setup. Use b.N so all distinct
	// valid executions can be prepared before timing rather than reusing a
	// completed activity or timing its reset.
	h := newActivityCompletionHarness(b, b.N+1, false)
	work := make([]activityCompletionWork, b.N)
	for i := range work {
		work[i] = h.newWork(b, i, false)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, err := h.handler.RespondActivityTaskCompleted(context.Background(), work[i].request)
		if err != nil {
			b.Fatalf("complete activity through handler: %v", err)
		}
		if response == nil {
			b.Fatal("complete activity through handler returned a nil response")
		}
	}
}
