package history

import (
	"context"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	activityCompletionBenchmarkWorkflowID = "activity-completion-benchmark-workflow"
	activityCompletionBenchmarkTaskQueue  = "activity-completion-benchmark-queue"
)

type activityCompletionHandoffFixture struct {
	handler          *Handler
	request          *historyservice.RespondActivityTaskCompletedRequest
	workflowCache    wcache.Cache
	workflowCacheKey wcache.Key
}

type activityCompletionHandoffQueue struct{}

func (activityCompletionHandoffQueue) Category() tasks.Category {
	return tasks.CategoryTransfer
}

func (activityCompletionHandoffQueue) NotifyNewTasks([]tasks.Task) {}
func (activityCompletionHandoffQueue) FailoverNamespace(string)    {}
func (activityCompletionHandoffQueue) Start()                      {}
func (activityCompletionHandoffQueue) Stop()                       {}

type activityCompletionHandoffNotifier struct{}

func (activityCompletionHandoffNotifier) NotifyNewHistoryEvent(*events.Notification) {}
func (activityCompletionHandoffNotifier) WatchHistoryEvent(definition.WorkflowKey) (string, chan *events.Notification, error) {
	return "", nil, nil
}
func (activityCompletionHandoffNotifier) UnwatchHistoryEvent(definition.WorkflowKey, string) error {
	return nil
}
func (activityCompletionHandoffNotifier) Start() {}
func (activityCompletionHandoffNotifier) Stop()  {}

func newActivityCompletionHandoffFixture(
	t testing.TB,
	omitRunID bool,
	completeActivity bool,
) *activityCompletionHandoffFixture {
	t.Helper()

	controller := gomock.NewController(t)
	config := tests.NewDynamicConfig()
	shardContext := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
	)
	t.Cleanup(shardContext.StopForTest)

	clusterMetadata := shardContext.Resource.ClusterMetadata
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, int64(0)).Return(cluster.TestCurrentClusterName).AnyTimes()

	namespaceRegistry := shardContext.Resource.NamespaceCache
	namespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	stateMachineRegistry := hsm.NewRegistry()
	if err := workflow.RegisterStateMachine(stateMachineRegistry); err != nil {
		t.Fatalf("register workflow state machine: %v", err)
	}
	shardContext.SetStateMachineRegistry(stateMachineRegistry)

	eventsCache := events.NewHostLevelEventsCache(
		shardContext.Resource.ExecutionMgr,
		config,
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
		false,
	)
	shardContext.SetEventsCacheForTesting(eventsCache)

	execution := commonpb.WorkflowExecution{
		WorkflowId: activityCompletionBenchmarkWorkflowID,
		RunId:      tests.RunID,
	}
	mutableState := workflow.TestLocalMutableState(
		shardContext,
		eventsCache,
		tests.LocalNamespaceEntry,
		execution.GetWorkflowId(),
		execution.GetRunId(),
		log.NewNoopLogger(),
	)
	addWorkflowExecutionStartedEvent(
		mutableState,
		&execution,
		"activity-completion-benchmark-workflow-type",
		activityCompletionBenchmarkTaskQueue,
		payloads.EncodeString("benchmark input"),
		time.Minute,
		time.Minute,
		time.Minute,
		"benchmark-worker",
	)

	scheduledEventID := int64(1)
	startedEventID := int64(2)
	if completeActivity {
		workflowTask := addWorkflowTaskScheduledEvent(mutableState)
		workflowTaskStartedEvent := addWorkflowTaskStartedEvent(
			mutableState,
			workflowTask.ScheduledEventID,
			activityCompletionBenchmarkTaskQueue,
			"benchmark-worker",
		)
		if _, err := mutableState.AddWorkflowTaskCompletedEvent(
			workflowTask,
			&workflowservice.RespondWorkflowTaskCompletedRequest{Identity: "benchmark-worker"},
			historyi.WorkflowTaskCompletionLimits{
				MaxResetPoints:              primitives.DefaultHistoryMaxAutoResetPoints,
				MaxSearchAttributeValueSize: 2048,
			},
		); err != nil {
			t.Fatalf("complete benchmark workflow task: %v", err)
		}
		mutableState.FlushBufferedEvents()
		activityScheduledEvent, _ := addActivityTaskScheduledEvent(
			mutableState,
			workflowTaskStartedEvent.EventId,
			"activity-completion-benchmark-activity",
			"activity-completion-benchmark-activity-type",
			activityCompletionBenchmarkTaskQueue,
			payloads.EncodeString("benchmark activity input"),
			time.Minute,
			time.Minute,
			time.Minute,
			time.Minute,
		)
		activityStartedEvent := addActivityTaskStartedEvent(
			mutableState,
			activityScheduledEvent.EventId,
			"benchmark-worker",
		)
		scheduledEventID = activityScheduledEvent.EventId
		startedEventID = activityStartedEvent.EventId
	}
	workflowState := workflow.TestCloneToProto(context.Background(), mutableState)
	executionManager := shardContext.Resource.ExecutionMgr
	executionManager.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
			return &persistence.GetWorkflowExecutionResponse{
				State: proto.Clone(workflowState).(*persistencespb.WorkflowMutableState),
			}, nil
		},
	).AnyTimes()
	if omitRunID {
		executionManager.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(
			&persistence.GetCurrentExecutionResponse{RunID: tests.RunID},
			nil,
		).AnyTimes()
	}
	if completeActivity {
		executionManager.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
			tests.UpdateWorkflowExecutionResponse,
			nil,
		).AnyTimes()
		shardContext.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	}

	workflowCache := wcache.NewHostLevelCache(config, shardContext.GetLogger(), metrics.NoopMetricsHandler)
	queue := activityCompletionHandoffQueue{}
	engine := &historyEngineImpl{
		shardContext:               shardContext,
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(shardContext, workflowCache),
		logger:                     log.NewNoopLogger(),
		eventNotifier:              activityCompletionHandoffNotifier{},
		queueProcessors: map[tasks.Category]queues.Queue{
			tasks.CategoryTransfer:    queue,
			tasks.CategoryTimer:       queue,
			tasks.CategoryVisibility:  queue,
			tasks.CategoryArchival:    queue,
			tasks.CategoryMemoryTimer: queue,
			tasks.CategoryOutbound:    queue,
		},
	}
	shardContext.SetEngineForTesting(engine)

	shardController := shard.NewMockController(controller)
	shardController.EXPECT().GetShardByNamespaceWorkflow(
		tests.NamespaceID,
		activityCompletionBenchmarkWorkflowID,
	).Return(shardContext, nil).AnyTimes()

	taskToken := &tokenspb.Task{
		NamespaceId:      tests.NamespaceID.String(),
		WorkflowId:       activityCompletionBenchmarkWorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: scheduledEventID,
		StartedEventId:   startedEventID,
		Attempt:          1,
		ActivityId:       "activity-completion-benchmark-activity",
		WorkflowType:     "activity-completion-benchmark-workflow-type",
		ActivityType:     "activity-completion-benchmark-activity-type",
		Clock: &clockspb.VectorClock{
			ShardId:   1,
			Clock:     2,
			ClusterId: 3,
		},
		StartedTime: timestamppb.New(time.Unix(1_700_000_000, 0)),
	}
	if omitRunID {
		taskToken.RunId = ""
	}
	serializedTaskToken, err := tasktoken.NewSerializer().Serialize(taskToken)
	if err != nil {
		t.Fatalf("serialize benchmark task token: %v", err)
	}

	return &activityCompletionHandoffFixture{
		handler: &Handler{
			tokenSerializer: tasktoken.NewSerializer(),
			controller:      shardController,
			logger:          log.NewNoopLogger(),
		},
		workflowCache: workflowCache,
		workflowCacheKey: wcache.Key{
			WorkflowKey: definition.NewWorkflowKey(tests.NamespaceID.String(), execution.GetWorkflowId(), execution.GetRunId()),
			ArchetypeID: chasm.WorkflowArchetypeID,
			ShardUUID:   shardContext.GetOwner(),
		},
		request: &historyservice.RespondActivityTaskCompletedRequest{
			NamespaceId: tests.NamespaceID.String(),
			CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
				TaskToken: serializedTaskToken,
				Identity:  "benchmark-worker",
				Result:    payloads.EncodeString("benchmark activity result"),
			},
		},
	}
}

func TestRespondActivityTaskCompletedHandlerPreservesMissingRunIDResolution(t *testing.T) {
	fixture := newActivityCompletionHandoffFixture(t, true, false)

	_, err := fixture.handler.RespondActivityTaskCompleted(context.Background(), fixture.request)
	if err == nil {
		t.Fatal("RespondActivityTaskCompleted succeeded for an unknown activity")
	}
	if _, ok := err.(*serviceerror.NotFound); !ok {
		t.Fatalf("RespondActivityTaskCompleted error = %T (%v), want *serviceerror.NotFound", err, err)
	}
}

func BenchmarkRespondActivityTaskCompletedTokenHandoff(b *testing.B) {
	fixture := newActivityCompletionHandoffFixture(b, false, true)

	b.ReportAllocs()
	for b.Loop() {
		if response, err := fixture.handler.RespondActivityTaskCompleted(context.Background(), fixture.request); err != nil || response == nil {
			b.Fatalf("RespondActivityTaskCompleted = (%v, %v), want a response and nil error", response, err)
		}
		b.StopTimer()
		wcache.ClearMutableState(fixture.workflowCache, fixture.workflowCacheKey)
		b.StartTimer()
	}
}
