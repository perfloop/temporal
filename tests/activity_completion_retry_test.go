package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	historyi "go.temporal.io/server/service/history/interfaces"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	workflowLeaseAcquisitionsMetric = "workflow_lease_acquisitions_per_fault_injected_completion"
	historyDeliveriesMetric         = "history_completion_deliveries_per_fault_injected_completion"
	completionSuccessMetric         = "fault_injected_completion_success"
)

type activityCompletionRequestContextKey struct{}

type workflowLeaseCounter struct {
	acquisitions atomic.Int32
}

func (c *workflowLeaseCounter) reset() {
	c.acquisitions.Store(0)
}

type activityCompletionWorkflowCache struct {
	wcache.Cache
	counter *workflowLeaseCounter
}

func (c *activityCompletionWorkflowCache) GetOrCreateChasmExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	archetypeID chasm.ArchetypeID,
	lockPriority locks.Priority,
) (historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, error) {
	if ctx.Value(activityCompletionRequestContextKey{}) != nil {
		c.counter.acquisitions.Add(1)
	}
	return c.Cache.GetOrCreateChasmExecution(ctx, shardContext, namespaceID, execution, archetypeID, lockPriority)
}

// lostCompletionResponseInterceptor simulates a response that is lost after
// History has committed a successful activity completion. It sits outside the
// retryable History interceptor, so its error makes the normal History client
// issue the duplicate delivery.
type lostCompletionResponseInterceptor struct {
	deliveries atomic.Int32
	dropNext   atomic.Bool
}

func (i *lostCompletionResponseInterceptor) armResponseDrop() {
	i.deliveries.Store(0)
	i.dropNext.Store(true)
}

func (i *lostCompletionResponseInterceptor) Intercept(
	ctx context.Context,
	request any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	isActivityCompletion := info.FullMethod == historyservice.HistoryService_RespondActivityTaskCompleted_FullMethodName
	if isActivityCompletion {
		ctx = context.WithValue(ctx, activityCompletionRequestContextKey{}, struct{}{})
	}
	response, err := handler(ctx, request)
	if !isActivityCompletion {
		return response, err
	}

	i.deliveries.Add(1)
	if err == nil && i.dropNext.CompareAndSwap(true, false) {
		return nil, serviceerror.NewUnavailable("test lost the successful History completion response")
	}
	return response, err
}

type completionRetryScenario struct {
	t             testing.TB
	fault         *lostCompletionResponseInterceptor
	cluster       *testcore.TestCluster
	ctx           context.Context
	namespace     string
	workflowID    string
	taskQueueName string
	identity      string
	started       *workflowservice.StartWorkflowExecutionResponse
	activityTask  *workflowservice.PollActivityTaskQueueResponse
	leaseCounter  *workflowLeaseCounter
}

type completionRetryResult struct {
	workflowLeaseAcquisitions int32
	historyDeliveries         int32
	completionSuccess         int
}

func TestActivityCompletionLostResponseRetriesWorkflowLease(t *testing.T) {
	scenario := newCompletionRetryScenario(t)
	result := scenario.complete(true)
	scenario.verify(result, 2)
}

func TestActivityCompletionWithoutLostResponseCompletesNormally(t *testing.T) {
	scenario := newCompletionRetryScenario(t)
	result := scenario.complete(false)

	require.Equal(t, 1, result.completionSuccess, "a fault-free activity completion must return success")
	require.Equal(t, int32(1), result.workflowLeaseAcquisitions, "a fault-free activity completion must acquire one workflow lease")
	scenario.verify(result, 1)
}

func BenchmarkActivityCompletionLostResponseRetriesWorkflowLease(b *testing.B) {
	if b.N != 1 {
		b.Fatal("benchmark requires -benchtime=1x because each sample starts an isolated test cluster")
	}

	b.StopTimer()
	scenario := newCompletionRetryScenario(b)
	b.StartTimer()
	result := scenario.complete(true)
	b.StopTimer()
	scenario.verify(result, 2)

	b.ReportMetric(float64(result.workflowLeaseAcquisitions), workflowLeaseAcquisitionsMetric)
	b.ReportMetric(float64(result.historyDeliveries), historyDeliveriesMetric)
	b.ReportMetric(float64(result.completionSuccess), completionSuccessMetric)
}

func newCompletionRetryScenario(t testing.TB) *completionRetryScenario {
	t.Helper()

	fault := &lostCompletionResponseInterceptor{}
	leaseCounter := &workflowLeaseCounter{}
	cluster := newCompletionRetryTestCluster(t, fault, leaseCounter)
	frontendClient := cluster.FrontendClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	namespace := registerCompletionRetryTestNamespace(t, ctx, frontendClient)
	scenario := &completionRetryScenario{
		t:             t,
		fault:         fault,
		cluster:       cluster,
		ctx:           ctx,
		namespace:     namespace,
		workflowID:    testcore.RandomizeStr(t.Name() + "-workflow"),
		taskQueueName: testcore.RandomizeStr(t.Name() + "-task-queue"),
		identity:      "activity-completion-retry-test",
		leaseCounter:  leaseCounter,
	}

	frontendClient = cluster.FrontendClient()
	scenario.started = startCompletionRetryWorkflow(t, ctx, frontendClient, namespace, scenario.workflowID, scenario.taskQueueName, scenario.identity)
	scheduleCompletionRetryActivity(t, ctx, frontendClient, namespace, scenario.taskQueueName, scenario.identity)
	scenario.activityTask = pollCompletionRetryActivity(t, ctx, frontendClient, namespace, scenario.taskQueueName, scenario.identity)
	return scenario
}

func (s *completionRetryScenario) complete(injectResponseFault bool) completionRetryResult {
	s.t.Helper()

	s.leaseCounter.reset()
	if injectResponseFault {
		s.fault.armResponseDrop()
	}
	_, err := s.cluster.FrontendClient().RespondActivityTaskCompleted(s.ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: s.namespace,
		TaskToken: s.activityTask.GetTaskToken(),
		Identity:  s.identity,
	})
	completionSucceeded := err == nil
	if !completionSucceeded {
		var notFound *serviceerror.NotFound
		require.ErrorAs(s.t, err, &notFound)
	}

	return completionRetryResult{
		workflowLeaseAcquisitions: s.leaseCounter.acquisitions.Load(),
		historyDeliveries:         s.fault.deliveries.Load(),
		completionSuccess:         boolToInt(completionSucceeded),
	}
}

func (s *completionRetryScenario) verify(result completionRetryResult, expectedHistoryDeliveries int32) {
	s.t.Helper()

	require.False(s.t, s.fault.dropNext.Load(), "the injected response drop was not consumed")
	require.GreaterOrEqual(s.t, result.workflowLeaseAcquisitions, int32(1), "the accepted completion must acquire a workflow lease")
	require.Equal(s.t, expectedHistoryDeliveries, result.historyDeliveries, "the History client delivery count must match the injected response behavior")

	frontendClient := s.cluster.FrontendClient()
	events := getCompletionRetryHistory(s.t, s.ctx, frontendClient, s.namespace, s.workflowID, s.started.GetRunId())
	require.Equal(s.t, 1, countActivityCompletionEvents(events), "the retry must not persist a second ActivityTaskCompleted event")

	completeCompletionRetryWorkflow(s.t, s.ctx, frontendClient, s.namespace, s.taskQueueName, s.identity)
	events = getCompletionRetryHistory(s.t, s.ctx, frontendClient, s.namespace, s.workflowID, s.started.GetRunId())
	require.Equal(s.t, 1, countWorkflowCompletionEvents(events), "the workflow must complete normally after the activity completion")
}

func newCompletionRetryTestCluster(
	t testing.TB,
	fault *lostCompletionResponseInterceptor,
	leaseCounter *workflowLeaseCounter,
) *testcore.TestCluster {
	t.Helper()

	cluster, err := testcore.NewTestClusterFactory().NewCluster(t, &testcore.TestClusterConfig{
		Persistence: testcore.GetPersistenceTestDefaults(),
		HistoryConfig: testcore.HistoryConfig{
			NumHistoryShards: 4,
		},
		WorkerConfig:             testcore.WorkerConfig{DisableWorker: true},
		HistoryOuterInterceptors: []grpc.UnaryServerInterceptor{fault.Intercept},
		ServiceFxOptions: map[primitives.ServiceName][]fx.Option{
			primitives.HistoryService: {
				fx.Decorate(func(cache wcache.Cache) wcache.Cache {
					return &activityCompletionWorkflowCache{Cache: cache, counter: leaseCounter}
				}),
			},
		},
	}, log.NewTestLogger())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cluster.TearDownCluster())
	})
	return cluster
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func registerCompletionRetryTestNamespace(
	t testing.TB,
	ctx context.Context,
	frontendClient workflowservice.WorkflowServiceClient,
) string {
	t.Helper()

	namespace := testcore.RandomizeStr(t.Name() + "-namespace")
	_, err := frontendClient.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		response, err := frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
		return err == nil && response.GetNamespaceInfo().GetId() != ""
	}, 10*time.Second, 50*time.Millisecond)

	return namespace
}

func startCompletionRetryWorkflow(
	t testing.TB,
	ctx context.Context,
	frontendClient workflowservice.WorkflowServiceClient,
	namespace string,
	workflowID string,
	taskQueueName string,
	identity string,
) *workflowservice.StartWorkflowExecutionResponse {
	t.Helper()

	response, err := frontendClient.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  namespace,
		WorkflowId: workflowID,
		WorkflowType: &commonpb.WorkflowType{
			Name: "activity-completion-retry-workflow",
		},
		TaskQueue:           normalTaskQueue(taskQueueName),
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(time.Second),
		RequestId:           uuid.NewString(),
		Identity:            identity,
	})
	require.NoError(t, err)
	return response
}

func scheduleCompletionRetryActivity(
	t testing.TB,
	ctx context.Context,
	frontendClient workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueueName string,
	identity string,
) {
	t.Helper()

	workflowTask, err := frontendClient.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: normalTaskQueue(taskQueueName),
		Identity:  identity,
	})
	require.NoError(t, err)
	require.NotEmpty(t, workflowTask.GetTaskToken())

	_, err = frontendClient.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: namespace,
		TaskToken: workflowTask.GetTaskToken(),
		Identity:  identity,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "activity-to-complete",
					ActivityType:           &commonpb.ActivityType{Name: "activity-completion-retry-activity"},
					TaskQueue:              normalTaskQueue(taskQueueName),
					ScheduleToCloseTimeout: durationpb.New(time.Minute),
					ScheduleToStartTimeout: durationpb.New(time.Minute),
					StartToCloseTimeout:    durationpb.New(time.Minute),
				},
			},
		}},
	})
	require.NoError(t, err)
}

func pollCompletionRetryActivity(
	t testing.TB,
	ctx context.Context,
	frontendClient workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueueName string,
	identity string,
) *workflowservice.PollActivityTaskQueueResponse {
	t.Helper()

	response, err := frontendClient.PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: normalTaskQueue(taskQueueName),
		Identity:  identity,
	})
	require.NoError(t, err)
	require.NotEmpty(t, response.GetTaskToken())
	return response
}

func completeCompletionRetryWorkflow(
	t testing.TB,
	ctx context.Context,
	frontendClient workflowservice.WorkflowServiceClient,
	namespace string,
	taskQueueName string,
	identity string,
) {
	t.Helper()

	workflowTask, err := frontendClient.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: normalTaskQueue(taskQueueName),
		Identity:  identity,
	})
	require.NoError(t, err)
	require.NotEmpty(t, workflowTask.GetTaskToken())

	_, err = frontendClient.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: namespace,
		TaskToken: workflowTask.GetTaskToken(),
		Identity:  identity,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	require.NoError(t, err)
}

func getCompletionRetryHistory(
	t testing.TB,
	ctx context.Context,
	frontendClient workflowservice.WorkflowServiceClient,
	namespace string,
	workflowID string,
	runID string,
) []*historypb.HistoryEvent {
	t.Helper()

	response, err := frontendClient.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	require.NoError(t, err)
	return response.GetHistory().GetEvents()
}

func countActivityCompletionEvents(events []*historypb.HistoryEvent) int {
	count := 0
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			count++
		}
	}
	return count
}

func countWorkflowCompletionEvents(events []*historypb.HistoryEvent) int {
	count := 0
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
			count++
		}
	}
	return count
}

func normalTaskQueue(name string) *taskqueuepb.TaskQueue {
	return &taskqueuepb.TaskQueue{
		Name: name,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
}
