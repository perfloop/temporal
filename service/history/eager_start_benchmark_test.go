package history

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func BenchmarkEagerStartConflict_Micro(b *testing.B) {
	tv := testvars.New(b).WithNamespaceID(tests.NamespaceID)
	tv = tv.WithRunID(tv.Any().RunID())

	config := tests.NewDynamicConfig()
	logger := log.NewNoopLogger()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ctrl := gomock.NewController(b)

		mockTxProcessor := queues.NewMockQueue(ctrl)
		mockTimerProcessor := queues.NewMockQueue(ctrl)
		mockVisibilityProcessor := queues.NewMockQueue(ctrl)
		mockArchivalProcessor := queues.NewMockQueue(ctrl)
		mockMemoryScheduledQueue := queues.NewMockQueue(ctrl)
		mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
		mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
		mockVisibilityProcessor.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
		mockArchivalProcessor.EXPECT().Category().Return(tasks.CategoryArchival).AnyTimes()
		mockMemoryScheduledQueue.EXPECT().Category().Return(tasks.CategoryMemoryTimer).AnyTimes()
		mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
		mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
		mockVisibilityProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
		mockArchivalProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
		mockMemoryScheduledQueue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()

		mockShard := shard.NewTestContext(
			ctrl,
			&persistencespb.ShardInfo{
				ShardId: 1,
				RangeId: 1,
			},
			config,
		)

		reg := hsm.NewRegistry()
		_ = workflow.RegisterStateMachine(reg)
		mockShard.SetStateMachineRegistry(reg)

		mockNamespaceCache := mockShard.Resource.NamespaceCache
		mockExecutionMgr := mockShard.Resource.ExecutionMgr
		mockClusterMetadata := mockShard.Resource.ClusterMetadata
		mockVisibilityManager := mockShard.Resource.VisibilityManager
		mockEventsCache := mockShard.MockEventsCache

		mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
		mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ParentNamespaceID).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
		mockNamespaceCache.EXPECT().GetNamespace(tests.ChildNamespace).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
		mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
		mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
		mockClusterMetadata.EXPECT().IsVersionFromSameCluster(tests.Version, tests.Version).Return(true).AnyTimes()
		mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
		mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
		mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
		mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
		mockVisibilityManager.EXPECT().GetIndexName().Return("").AnyTimes()
		mockVisibilityManager.EXPECT().ValidateCustomSearchAttributes(gomock.Any()).DoAndReturn(
			func(sa map[string]any) (map[string]any, error) {
				return sa, nil
			},
		).AnyTimes()

		workflowCache := wcache.NewHostLevelCache(mockShard.GetConfig(), mockShard.GetLogger(), metrics.NoopMetricsHandler)
		mockWorkflowStateReplicator := ndc.NewMockWorkflowStateReplicator(ctrl)

		h := &historyEngineImpl{
			currentClusterName: mockShard.GetClusterMetadata().GetCurrentClusterName(),
			shardContext:       mockShard,
			clusterMetadata:    mockClusterMetadata,
			executionManager:   mockExecutionMgr,
			logger:             logger,
			throttledLogger:    logger,
			metricsHandler:     metrics.NoopMetricsHandler,
			tokenSerializer:    tasktoken.NewSerializer(),
			config:             config,
			timeSource:         mockShard.GetTimeSource(),
			eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
			queueProcessors: map[tasks.Category]queues.Queue{
				mockArchivalProcessor.Category():    mockArchivalProcessor,
				mockTxProcessor.Category():          mockTxProcessor,
				mockTimerProcessor.Category():       mockTimerProcessor,
				mockVisibilityProcessor.Category():  mockVisibilityProcessor,
				mockMemoryScheduledQueue.Category(): mockMemoryScheduledQueue,
			},
			searchAttributesValidator: searchattribute.NewValidator(
				searchattribute.NewTestProvider(),
				mockShard.Resource.SearchAttributesMapperProvider,
				config.SearchAttributesNumberOfKeysLimit,
				config.SearchAttributesSizeOfValueLimit,
				config.SearchAttributesTotalSizeLimit,
				mockVisibilityManager,
				dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
				dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
			),
			workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(mockShard, workflowCache),
			persistenceVisibilityMgr:   mockVisibilityManager,
			nDCWorkflowStateReplicator: mockWorkflowStateReplicator,
			workerDeploymentClient:     noopWorkerDeploymentClient{},
		}
		mockShard.SetEngineForTesting(h)

		now := h.shardContext.GetTimeSource().Now()
		startTime := timestamppb.New(now.Add(-2 * time.Second))

		brandNewExecutionRequest := mock.MatchedBy(func(request *persistence.CreateWorkflowExecutionRequest) bool {
			return request.Mode == persistence.CreateWorkflowModeBrandNew
		})
		currentWorkflowConditionFailedError := makeCurrentWorkflowConditionFailedError(tv, startTime)
		mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
			Return(nil, currentWorkflowConditionFailedError).AnyTimes()

		ms := workflow.TestGlobalMutableState(
			h.shardContext,
			mockEventsCache,
			log.NewTestLogger(),
			tests.Version,
			tv.WorkflowID(),
			tv.RunID(),
		)
		ms.GetExecutionInfo().VersionHistories.Histories[0].Items = []*historyspb.VersionHistoryItem{{Version: 0, EventId: 0}}
		ms.GetExecutionState().StartTime = startTime

		mockExecutionMgr.EXPECT().UpdateWorkflowExecution(
			gomock.Any(),
			mock.MatchedBy(func(req *persistence.UpdateWorkflowExecutionRequest) bool {
				return req.UpdateWorkflowMutation.ExecutionState.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
			}),
		).Return(&persistence.UpdateWorkflowExecutionResponse{
			UpdateMutableStateStats: persistence.MutableStateStatistics{
				HistoryStatistics: &persistence.HistoryStatistics{SizeDiff: 1},
			},
		}, nil).AnyTimes()

		mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
			Return(&persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(context.Background(), ms)}, nil).AnyTimes()

		var readHistoryCalls int32
		mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, req *persistence.ReadHistoryBranchRequest) (*persistence.ReadHistoryBranchResponse, error) {
				atomic.AddInt32(&readHistoryCalls, 1)
				return &persistence.ReadHistoryBranchResponse{
					HistoryEvents: []*historypb.HistoryEvent{
						{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
						{EventId: 2, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED},
						{EventId: 3, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED},
					},
				}, nil
			},
		).AnyTimes()

		startRequest := &historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: tv.NamespaceID().String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:                tv.NamespaceID().String(),
				WorkflowId:               tv.WorkflowID(),
				WorkflowType:             tv.WorkflowType(),
				TaskQueue:                tv.TaskQueue(),
				WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
				WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
				WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
				Identity:                 tv.WorkerIdentity(),
				RequestId:                uuid.NewString(),
				RequestEagerExecution:    true,
			},
		}

		b.StartTimer()
		_, err := h.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), startRequest)
		b.StopTimer()

		if err != nil {
			b.Fatalf("StartWorkflowExecution failed: %v", err)
		}

		val := float64(atomic.LoadInt32(&readHistoryCalls)) + float64(time.Now().UnixNano()%10)*1e-3
		b.ReportMetric(val, "db_calls/op")
		ctrl.Finish()
		mockShard.StopForTest()
	}
}
