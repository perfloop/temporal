package history

import (
	"context"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

// BenchmarkRespondActivityTaskCompletedFullPath measures the non-component
// handler path through the production history engine, workflow cache, mutable
// state, and persistence update construction. The test persistence manager
// returns the same pre-completion mutable state after each operation so the
// benchmark can reuse one immutable request without mutating its serialized
// token bytes.
func BenchmarkRespondActivityTaskCompletedFullPath(b *testing.B) {
	fixture := newActivityCompletionFullPathFixture(b, activityCompletionFullPathTokenShapeNormal)
	defer fixture.close()

	benchmarkActivityCompletionFullPath(b, fixture, fixture.handler.RespondActivityTaskCompleted)
}

// BenchmarkRespondActivityTaskCompletedFullPathByID exercises the mutable-state
// activity-ID lookup branch with the same production cache and persistence path.
func BenchmarkRespondActivityTaskCompletedFullPathByID(b *testing.B) {
	fixture := newActivityCompletionFullPathFixture(b, activityCompletionFullPathTokenShapeByID)
	defer fixture.close()

	benchmarkActivityCompletionFullPath(b, fixture, fixture.handler.RespondActivityTaskCompleted)
}

// BenchmarkRespondActivityTaskCompletedDirectEngineFullPath is a neutral guard
// for callers of the legacy Engine method, which must continue decoding raw
// token bytes before delegating to the typed handoff.
func BenchmarkRespondActivityTaskCompletedDirectEngineFullPath(b *testing.B) {
	fixture := newActivityCompletionFullPathFixture(b, activityCompletionFullPathTokenShapeNormal)
	defer fixture.close()

	benchmarkActivityCompletionFullPath(b, fixture, fixture.historyEngine.RespondActivityTaskCompleted)
}

func benchmarkActivityCompletionFullPath(
	b *testing.B,
	fixture *activityCompletionFullPathFixture,
	complete func(context.Context, *historyservice.RespondActivityTaskCompletedRequest) (*historyservice.RespondActivityTaskCompletedResponse, error),
) {
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		response, err := complete(b.Context(), fixture.request)
		if err != nil {
			b.Fatal(err)
		}
		if response == nil {
			b.Fatal("RespondActivityTaskCompleted returned a nil response")
		}

		// The completion mutates its mutable state. Clear only the cached test
		// context between iterations so each operation reloads the same persisted
		// pre-completion state; this reset is outside the measured operation.
		b.StopTimer()
		fixture.resetWorkflowContext()
		b.StartTimer()
	}
}

type activityCompletionFullPathTokenShape uint8

const (
	activityCompletionFullPathTokenShapeNormal activityCompletionFullPathTokenShape = iota
	activityCompletionFullPathTokenShapeByID
)

type activityCompletionFullPathController struct {
	shard.Controller

	shardContext historyi.ShardContext
}

func (c *activityCompletionFullPathController) GetShardByNamespaceWorkflow(
	_ namespace.ID,
	_ string,
) (historyi.ShardContext, error) {
	return c.shardContext, nil
}

type activityCompletionFullPathFixture struct {
	controller         *gomock.Controller
	shardContext       *shard.ContextTest
	eventNotifier      events.Notifier
	historyEngine      *historyEngineImpl
	handler            *Handler
	request            *historyservice.RespondActivityTaskCompletedRequest
	consistencyChecker *activityCompletionBenchmarkConsistencyChecker
}

func newActivityCompletionFullPathFixture(
	b *testing.B,
	tokenShape activityCompletionFullPathTokenShape,
) *activityCompletionFullPathFixture {
	controller := gomock.NewController(b)
	config := tests.NewDynamicConfig()
	shardContext := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
	)

	transferQueue := queues.NewMockQueue(controller)
	timerQueue := queues.NewMockQueue(controller)
	visibilityQueue := queues.NewMockQueue(controller)
	archivalQueue := queues.NewMockQueue(controller)
	memoryTimerQueue := queues.NewMockQueue(controller)
	outboundQueue := queues.NewMockQueue(controller)
	for _, queue := range []struct {
		queue    *queues.MockQueue
		category tasks.Category
	}{
		{transferQueue, tasks.CategoryTransfer},
		{timerQueue, tasks.CategoryTimer},
		{visibilityQueue, tasks.CategoryVisibility},
		{archivalQueue, tasks.CategoryArchival},
		{memoryTimerQueue, tasks.CategoryMemoryTimer},
		{outboundQueue, tasks.CategoryOutbound},
	} {
		queue.queue.EXPECT().Category().Return(queue.category).AnyTimes()
		queue.queue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	}

	workflowCache := wcache.NewHostLevelCache(config, shardContext.GetLogger(), metrics.NoopMetricsHandler)
	eventsCache := events.NewHostLevelEventsCache(
		shardContext.GetExecutionManager(),
		config,
		shardContext.GetMetricsHandler(),
		shardContext.GetLogger(),
		false,
	)
	shardContext.SetEventsCacheForTesting(eventsCache)

	stateMachineRegistry := hsm.NewRegistry()
	if err := workflow.RegisterStateMachine(stateMachineRegistry); err != nil {
		b.Fatal(err)
	}
	shardContext.SetStateMachineRegistry(stateMachineRegistry)

	clusterMetadata := shardContext.Resource.ClusterMetadata
	clusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()

	namespaceRegistry := shardContext.Resource.NamespaceCache
	namespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	namespaceRegistry.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	eventNotifier := events.NewNotifier(
		clock.NewRealTimeSource(),
		shardContext.Resource.MetricsHandler,
		func(namespaceID namespace.ID, workflowID string) int32 {
			return int32(len(namespaceID.String() + "_" + workflowID))
		},
	)

	historyEngine := &historyEngineImpl{
		currentClusterName: clusterMetadata.GetCurrentClusterName(),
		shardContext:       shardContext,
		clusterMetadata:    clusterMetadata,
		executionManager:   shardContext.Resource.ExecutionMgr,
		logger:             shardContext.GetLogger(),
		metricsHandler:     shardContext.GetMetricsHandler(),
		tokenSerializer:    tasktoken.NewSerializer(),
		eventNotifier:      eventNotifier,
		config:             config,
		queueProcessors: map[tasks.Category]queues.Queue{
			transferQueue.Category():    transferQueue,
			timerQueue.Category():       timerQueue,
			visibilityQueue.Category():  visibilityQueue,
			archivalQueue.Category():    archivalQueue,
			memoryTimerQueue.Category(): memoryTimerQueue,
			outboundQueue.Category():    outboundQueue,
		},
		eventsReapplier:          ndc.NewMockEventsReapplier(controller),
		workflowResetter:         ndc.NewMockWorkflowResetter(controller),
		throttledLogger:          log.NewNoopLogger(),
		persistenceVisibilityMgr: shardContext.Resource.VisibilityManager,
		versionChecker:           headers.NewDefaultVersionChecker(),
		workerDeploymentClient:   noopWorkerDeploymentClient{},
	}

	request, state := newActivityCompletionFullPathRequest(b, shardContext, eventsCache, tokenShape)
	setActivityCompletionFullPathPersistenceExpectations(shardContext, state)

	consistencyChecker := &activityCompletionBenchmarkConsistencyChecker{
		WorkflowConsistencyChecker: api.NewWorkflowConsistencyChecker(shardContext, workflowCache),
	}
	historyEngine.workflowConsistencyChecker = consistencyChecker
	shardContext.SetEngineForTesting(historyEngine)
	eventNotifier.Start()

	return &activityCompletionFullPathFixture{
		controller:         controller,
		shardContext:       shardContext,
		eventNotifier:      eventNotifier,
		historyEngine:      historyEngine,
		handler:            &Handler{tokenSerializer: tasktoken.NewSerializer(), controller: &activityCompletionFullPathController{shardContext: shardContext}},
		request:            request,
		consistencyChecker: consistencyChecker,
	}
}

func newActivityCompletionFullPathRequest(
	b *testing.B,
	shardContext historyi.ShardContext,
	eventsCache events.Cache,
	tokenShape activityCompletionFullPathTokenShape,
) (*historyservice.RespondActivityTaskCompletedRequest, *persistencespb.WorkflowMutableState) {
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "activity-completion-full-path-workflow",
		RunId:      tests.RunID,
	}
	const (
		identity     = "activity-completion-full-path-worker"
		taskQueue    = "activity-completion-full-path-task-queue"
		activityID   = "activity-completion-full-path-activity"
		activityType = "activity-completion-full-path-activity-type"
	)

	mutableState := workflow.TestLocalMutableState(
		shardContext,
		eventsCache,
		tests.LocalNamespaceEntry,
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
		log.NewTestLogger(),
	)
	addWorkflowExecutionStartedEvent(
		mutableState,
		&workflowExecution,
		"activity-completion-full-path-workflow-type",
		taskQueue,
		payloads.EncodeString("input"),
		100*time.Second,
		100*time.Second,
		100*time.Second,
		identity,
	)
	workflowTask := addWorkflowTaskScheduledEvent(mutableState)
	addWorkflowTaskStartedEvent(mutableState, workflowTask.ScheduledEventID, taskQueue, identity)
	workflowTaskInfo := mutableState.GetWorkflowTaskByID(workflowTask.ScheduledEventID)
	if workflowTaskInfo == nil {
		b.Fatal("missing scheduled workflow task")
	}
	workflowTaskCompleted, err := mutableState.AddWorkflowTaskCompletedEvent(
		workflowTaskInfo,
		&workflowservice.RespondWorkflowTaskCompletedRequest{Identity: identity},
		defaultWorkflowTaskCompletionLimits,
	)
	if err != nil {
		b.Fatal(err)
	}
	mutableState.FlushBufferedEvents()
	activityScheduled, _ := addActivityTaskScheduledEvent(
		mutableState,
		workflowTaskCompleted.EventId,
		activityID,
		activityType,
		taskQueue,
		payloads.EncodeString("activity input"),
		100*time.Second,
		10*time.Second,
		time.Second,
		5*time.Second,
	)
	addActivityTaskStartedEvent(mutableState, activityScheduled.EventId, identity)

	token := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      tests.NamespaceID.String(),
		WorkflowId:       workflowExecution.GetWorkflowId(),
		RunId:            workflowExecution.GetRunId(),
		ScheduledEventId: activityScheduled.EventId,
	}
	if tokenShape == activityCompletionFullPathTokenShapeByID {
		token.ScheduledEventId = common.EmptyEventID
		token.ActivityId = activityID
	}
	serializedToken, err := tasktoken.NewSerializer().Serialize(token)
	if err != nil {
		b.Fatal(err)
	}
	return &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: serializedToken,
			Result:    payloads.EncodeString("activity completion"),
			Identity:  identity,
		},
	}, workflow.TestCloneToProto(context.Background(), mutableState)
}

func setActivityCompletionFullPathPersistenceExpectations(
	shardContext *shard.ContextTest,
	state *persistencespb.WorkflowMutableState,
) {
	shardContext.Resource.ExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
			return &persistence.GetWorkflowExecutionResponse{State: proto.Clone(state).(*persistencespb.WorkflowMutableState)}, nil
		},
	).AnyTimes()
	shardContext.Resource.ExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil).AnyTimes()
}

func (f *activityCompletionFullPathFixture) resetWorkflowContext() {
	if f.consistencyChecker.workflowContext == nil {
		panic("activity completion did not acquire a workflow context")
	}
	f.consistencyChecker.workflowContext.Clear()
	f.consistencyChecker.workflowContext = nil
}

func (f *activityCompletionFullPathFixture) close() {
	f.eventNotifier.Stop()
	f.shardContext.StopForTest()
	f.controller.Finish()
}

type activityCompletionBenchmarkConsistencyChecker struct {
	api.WorkflowConsistencyChecker

	workflowContext historyi.WorkflowContext
}

func (c *activityCompletionBenchmarkConsistencyChecker) GetWorkflowLease(
	ctx context.Context,
	requestClock *clockspb.VectorClock,
	workflowKey definition.WorkflowKey,
	lockPriority locks.Priority,
) (api.WorkflowLease, error) {
	lease, err := c.WorkflowConsistencyChecker.GetWorkflowLease(ctx, requestClock, workflowKey, lockPriority)
	if err != nil {
		return nil, err
	}
	c.workflowContext = lease.GetContext()
	return lease, nil
}
