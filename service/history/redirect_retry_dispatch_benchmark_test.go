package history

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	redirectDispatchInitialBuildID = "dispatch-build-before-redirect"
	redirectDispatchTargetBuildID  = "dispatch-build-after-redirect"
)

type redirectRetryDispatchBenchmarkFixture struct {
	b *testing.B

	controller       *gomock.Controller
	shard            *shard.ContextTest
	timeSource       *clock.EventTimeSource
	workflowCache    wcache.Cache
	logger           log.Logger
	namespaceID      namespace.ID
	version          int64
	executionMgr     *persistence.MockExecutionManager
	matchingClient   *redirectRetryDispatchMatchingClient
	timerExecutor    *timerQueueActiveTaskExecutor
	transferExecutor *transferQueueActiveTaskExecutor

	persistedState  *persistencespb.WorkflowMutableState
	lastWorkflowKey *definition.WorkflowKey
	now             time.Time
}

type redirectRetryDispatchWork struct {
	transferTasks []*tasks.ActivityTask
	timerTasks    []*tasks.ActivityRetryTimerTask
}

// redirectRetryDispatchMatchingClient makes the benchmark consume every
// executor-produced matching request without introducing a network hop or mock
// reflection into one dispatch path more than the other.
type redirectRetryDispatchMatchingClient struct {
	matchingservice.MatchingServiceClient

	calls int
}

func (c *redirectRetryDispatchMatchingClient) AddActivityTask(
	_ context.Context,
	request *matchingservice.AddActivityTaskRequest,
	_ ...grpc.CallOption,
) (*matchingservice.AddActivityTaskResponse, error) {
	if request.GetVersionDirective().GetAssignedBuildId() != redirectDispatchTargetBuildID {
		return nil, fmt.Errorf("matching directive = %q, want %q", request.GetVersionDirective().GetAssignedBuildId(), redirectDispatchTargetBuildID)
	}
	c.calls++
	return redirectRetryDispatchAddActivityTaskResponse, nil
}

var redirectRetryDispatchAddActivityTaskResponse = &matchingservice.AddActivityTaskResponse{}

func newRedirectRetryDispatchBenchmarkFixture(b *testing.B) *redirectRetryDispatchBenchmarkFixture {
	b.Helper()

	fixture := &redirectRetryDispatchBenchmarkFixture{
		b:           b,
		now:         time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		namespaceID: tests.NamespaceID,
	}
	fixture.timeSource = clock.NewEventTimeSource().Update(fixture.now)
	fixture.controller = gomock.NewController(b)
	config := tests.NewDynamicConfig()
	fixture.shard = shard.NewTestContextWithTimeSource(
		fixture.controller,
		&persistencespb.ShardInfo{ShardId: 1, RangeId: 1},
		config,
		fixture.timeSource,
	)
	fixture.logger = fixture.shard.GetLogger()
	fixture.version = tests.GlobalNamespaceEntry.FailoverVersion(namespace.EmptyBusinessID)
	fixture.executionMgr = fixture.shard.Resource.ExecutionMgr
	fixture.matchingClient = &redirectRetryDispatchMatchingClient{}

	registry := hsm.NewRegistry()
	if err := workflow.RegisterStateMachine(registry); err != nil {
		b.Fatal(err)
	}
	fixture.shard.SetStateMachineRegistry(registry)
	fixture.shard.SetEventsCacheForTesting(events.NewHostLevelEventsCache(
		fixture.executionMgr,
		fixture.shard.GetConfig(),
		fixture.shard.GetMetricsHandler(),
		fixture.logger,
		false,
	))

	namespaceCache := fixture.shard.Resource.NamespaceCache
	namespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	namespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	clusterMetadata := fixture.shard.Resource.ClusterMetadata
	clusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	clusterMetadata.EXPECT().IsVersionFromSameCluster(tests.Version, tests.Version).Return(true).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	clusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(
		tests.GlobalNamespaceEntry.IsGlobalNamespace(),
		fixture.version,
	).Return(cluster.TestCurrentClusterName).AnyTimes()

	fixture.executionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
			if fixture.persistedState == nil {
				b.Fatal("dispatch attempted without a persisted mutable state")
			}
			return &persistence.GetWorkflowExecutionResponse{State: fixture.persistedState}, nil
		},
	).AnyTimes()

	fixture.workflowCache = wcache.NewHostLevelCache(
		fixture.shard.GetConfig(),
		fixture.logger,
		metrics.NoopMetricsHandler,
	)
	deleteManager := deletemanager.NewMockDeleteManager(fixture.controller)
	chasmEngine := chasm.NewMockEngine(fixture.controller)
	fixture.timerExecutor = newTimerQueueActiveTaskExecutor(
		fixture.shard,
		fixture.workflowCache,
		deleteManager,
		fixture.logger,
		metrics.NoopMetricsHandler,
		config,
		fixture.matchingClient,
		chasmEngine,
	).(*timerQueueActiveTaskExecutor)
	fixture.transferExecutor = &transferQueueActiveTaskExecutor{
		transferQueueTaskExecutorBase: newTransferQueueTaskExecutorBase(
			fixture.shard,
			fixture.workflowCache,
			fixture.logger,
			metrics.NoopMetricsHandler,
			fixture.shard.Resource.HistoryClient,
			fixture.matchingClient,
			fixture.shard.Resource.VisibilityManager,
			chasmEngine,
		),
	}

	b.Cleanup(func() {
		fixture.controller.Finish()
		fixture.shard.StopForTest()
	})
	return fixture
}

type redirectRetryDispatchSetup struct {
	mutableState   *workflow.MutableStateImpl
	retryEventIDs  map[int64]struct{}
	retryTimers    []*tasks.ActivityRetryTimerTask
	triggerEventID int64
	width          int
}

// prepareRetryLifecycle creates a normal pending retry and its physical timer
// before the measured redirect. This mirrors the state that already exists when
// a build-ID redirect arrives.
func (f *redirectRetryDispatchBenchmarkFixture) prepareRetryLifecycle(width int) redirectRetryDispatchSetup {
	f.b.Helper()
	if f.lastWorkflowKey != nil {
		f.clearMutableState(*f.lastWorkflowKey)
		f.lastWorkflowKey = nil
	}
	f.persistedState = nil
	f.timeSource.Update(f.now)
	f.matchingClient.calls = 0

	execution := &commonpb.WorkflowExecution{
		WorkflowId: fmt.Sprintf("redirect-dispatch-workflow-%d", width),
		RunId:      uuid.NewString(),
	}
	mutableState := workflow.TestGlobalMutableState(
		f.shard,
		f.shard.GetEventsCache(),
		f.logger,
		f.version,
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: f.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: "redirect-dispatch-workflow-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: "redirect-dispatch-workflow-task-queue"},
				WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
				WorkflowTaskTimeout: durationpb.New(time.Second),
			},
		},
	)
	if err != nil {
		f.b.Fatal(err)
	}
	workflowTask := addWorkflowTaskScheduledEvent(mutableState)
	workflowTaskStarted := addWorkflowTaskStartedEvent(
		mutableState,
		workflowTask.ScheduledEventID,
		"redirect-dispatch-workflow-task-queue",
		uuid.NewString(),
	)
	workflowTask.StartedEventID = workflowTaskStarted.GetEventId()
	workflowTaskCompleted, err := mutableState.AddWorkflowTaskCompletedEvent(
		workflowTask,
		&workflowservice.RespondWorkflowTaskCompletedRequest{Identity: "redirect-dispatch-worker"},
		defaultWorkflowTaskCompletionLimits,
	)
	if err != nil {
		f.b.Fatal(err)
	}
	mutableState.FlushBufferedEvents()
	mutableState.GetExecutionInfo().AssignedBuildId = redirectDispatchInitialBuildID

	retryEventIDs := make(map[int64]struct{}, width)
	for i := 0; i < width; i++ {
		scheduled, retry := addActivityTaskScheduledEventWithRetry(
			mutableState,
			workflowTaskCompleted.GetEventId(),
			fmt.Sprintf("redirect-dispatch-retry-%d", i),
			"redirect-dispatch-activity-type",
			"redirect-dispatch-activity-task-queue",
			nil,
			24*time.Hour,
			24*time.Hour,
			24*time.Hour,
			0,
			&commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(time.Hour),
				BackoffCoefficient: 1,
				MaximumAttempts:    2,
			},
		)
		retry.BuildIdInfo = &persistencespb.ActivityInfo_UseWorkflowBuildIdInfo_{
			UseWorkflowBuildIdInfo: &persistencespb.ActivityInfo_UseWorkflowBuildIdInfo{
				LastUsedBuildId: redirectDispatchInitialBuildID,
			},
		}
		_, err = mutableState.AddActivityTaskStartedEvent(
			retry,
			retry.GetScheduledEventId(),
			uuid.NewString(),
			"redirect-dispatch-worker",
			nil,
			nil,
			nil,
			"",
			nil,
		)
		if err != nil {
			f.b.Fatal(err)
		}
		retryState, err := mutableState.RetryActivity(retry, &failurepb.Failure{Message: "retryable failure"})
		if err != nil {
			f.b.Fatal(err)
		}
		if retryState != enumspb.RETRY_STATE_IN_PROGRESS {
			f.b.Fatalf("retry state = %v, want %v", retryState, enumspb.RETRY_STATE_IN_PROGRESS)
		}
		retryEventIDs[scheduled.GetEventId()] = struct{}{}
	}

	var retryTimers []*tasks.ActivityRetryTimerTask
	for _, task := range mutableState.PopTasks()[tasks.CategoryTimer] {
		if retryTimer, ok := task.(*tasks.ActivityRetryTimerTask); ok {
			if _, isRetry := retryEventIDs[retryTimer.EventID]; isRetry {
				retryTimer.TaskID = f.mustGenerateTaskID()
				retryTimers = append(retryTimers, retryTimer)
			}
		}
	}
	if len(retryTimers) != width {
		f.b.Fatalf("normal retry path generated %d retry timers, want %d", len(retryTimers), width)
	}

	trigger, _ := addActivityTaskScheduledEvent(
		mutableState,
		workflowTaskCompleted.GetEventId(),
		"redirect-dispatch-trigger",
		"redirect-dispatch-trigger-type",
		"redirect-dispatch-activity-task-queue",
		nil,
		24*time.Hour,
		24*time.Hour,
		24*time.Hour,
		0,
	)
	// The trigger's pre-existing physical tasks are not part of this lifecycle.
	mutableState.PopTasks()

	return redirectRetryDispatchSetup{
		mutableState:   mutableState,
		retryEventIDs:  retryEventIDs,
		retryTimers:    retryTimers,
		triggerEventID: trigger.GetEventId(),
		width:          width,
	}
}

// redirectAndPrepareDispatch runs the redirect transaction, snapshots the
// resulting authoritative state, and selects the resulting task route. It is
// intentionally in the measured interval with dispatch: this is the full
// redirect-to-Matching lifecycle rather than a redirect-only microbenchmark.
func (f *redirectRetryDispatchBenchmarkFixture) redirectAndPrepareDispatch(setup redirectRetryDispatchSetup) redirectRetryDispatchWork {
	if err := setup.mutableState.ApplyBuildIdRedirect(setup.triggerEventID, redirectDispatchTargetBuildID, 1); err != nil {
		f.b.Fatal(err)
	}

	var transferTasks []*tasks.ActivityTask
	for _, task := range setup.mutableState.PopTasks()[tasks.CategoryTransfer] {
		if activityTask, ok := task.(*tasks.ActivityTask); ok {
			if _, isRetry := setup.retryEventIDs[activityTask.ScheduledEventID]; isRetry {
				activityTask.TaskID = f.mustGenerateTaskID()
				transferTasks = append(transferTasks, activityTask)
			}
		}
	}

	f.persistedState = workflow.TestCloneToProto(context.Background(), setup.mutableState)
	f.timeSource.Update(f.now.Add(time.Hour))
	workflowKey := setup.mutableState.GetWorkflowKey()
	f.lastWorkflowKey = &workflowKey

	switch {
	case len(transferTasks) == setup.width:
		return redirectRetryDispatchWork{transferTasks: transferTasks}
	case len(transferTasks) == 0 && len(setup.retryTimers) == setup.width:
		return redirectRetryDispatchWork{timerTasks: setup.retryTimers}
	default:
		f.b.Fatalf("redirect produced %d transfer tasks and retained %d retry timers for width %d", len(transferTasks), len(setup.retryTimers), setup.width)
		return redirectRetryDispatchWork{}
	}
}

func (f *redirectRetryDispatchBenchmarkFixture) dispatch(work redirectRetryDispatchWork) error {
	for _, task := range work.transferTasks {
		if err := f.transferExecutor.processActivityTask(context.Background(), task); err != nil {
			return err
		}
	}
	for _, task := range work.timerTasks {
		if err := f.timerExecutor.executeActivityRetryTimerTask(context.Background(), task); err != nil {
			return err
		}
	}
	return nil
}

func (f *redirectRetryDispatchBenchmarkFixture) clearMutableState(workflowKey definition.WorkflowKey) {
	wcache.ClearMutableState(f.workflowCache, wcache.Key{
		WorkflowKey: workflowKey,
		ArchetypeID: chasm.WorkflowArchetypeID,
		ShardUUID:   f.shard.GetOwner(),
	})
}

func (f *redirectRetryDispatchBenchmarkFixture) mustGenerateTaskID() int64 {
	taskID, err := f.shard.GenerateTaskID()
	if err != nil {
		f.b.Fatal(err)
	}
	return taskID
}

// BenchmarkRedirectedFutureRetryLifecycle measures the full redirect-to-Matching
// lifecycle. Setup creates an ordinary future retry before timing; the timed
// interval applies the redirect, snapshots the resulting state, and dispatches
// every resulting task through its active executor and Matching. Baseline
// revisions take the redirect-generated ActivityTask route; the candidate takes
// the retained ActivityRetryTimerTask route.
func BenchmarkRedirectedFutureRetryLifecycle(b *testing.B) {
	for _, width := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("%d", width), func(b *testing.B) {
			fixture := newRedirectRetryDispatchBenchmarkFixture(b)
			b.ReportAllocs()

			var (
				err      error
				lastWork redirectRetryDispatchWork
			)
			for b.Loop() {
				b.StopTimer()
				setup := fixture.prepareRetryLifecycle(width)
				b.StartTimer()
				work := fixture.redirectAndPrepareDispatch(setup)
				err = fixture.dispatch(work)
				lastWork = work
				if err != nil {
					b.StopTimer()
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if got, want := fixture.matchingClient.calls, len(lastWork.transferTasks)+len(lastWork.timerTasks); got != want {
				b.Fatalf("matching dispatches = %d, want %d", got, want)
			}
		})
	}
}
