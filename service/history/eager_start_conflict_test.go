package history

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/testvars"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var eagerStartConflictFixtureTime = time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)

type eagerStartConflictShardContext struct {
	historyi.ShardContext
	executionManager persistence.ExecutionManager
}

func (s *eagerStartConflictShardContext) GetExecutionManager() persistence.ExecutionManager {
	return s.executionManager
}

// sqliteHistoryReadCounter delegates the production history read to Temporal's
// SQLite execution manager. The fixture persists the events through that same
// manager before this counter is reached, so it never supplies synthetic history.
type sqliteHistoryReadCounter struct {
	persistence.ExecutionManager
	sqliteExecutionManager persistence.ExecutionManager
	readHistoryCalls       *atomic.Int64
}

func (m *sqliteHistoryReadCounter) Close() {
	m.sqliteExecutionManager.Close()
}

func (m *sqliteHistoryReadCounter) GetName() string {
	return m.sqliteExecutionManager.GetName()
}

func (m *sqliteHistoryReadCounter) GetHistoryBranchUtil() persistence.HistoryBranchUtil {
	return m.sqliteExecutionManager.GetHistoryBranchUtil()
}

func (m *sqliteHistoryReadCounter) ReadHistoryBranch(
	ctx context.Context,
	request *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadHistoryBranchResponse, error) {
	response, err := m.sqliteExecutionManager.ReadHistoryBranch(ctx, request)
	if err == nil {
		m.readHistoryCalls.Add(1)
	}
	return response, err
}

type eagerStartConflictFixture struct {
	engine             *historyEngineImpl
	request            *historyservice.StartWorkflowExecutionRequest
	readHistoryCalls   atomic.Int64
	resetWorkflowCache func()
	close              func()
}

func newSQLiteHistoryExecutionManager(t testing.TB) persistence.ExecutionManager {
	t.Helper()

	logger := log.NewNoopLogger()
	serializer := serialization.NewSerializer()
	database, err := persistencesql.NewSQLDB(
		sqlplugin.DbKindMain,
		&config.SQL{
			PluginName:        sqlite.PluginName,
			DatabaseName:      uuid.NewString(),
			ConnectAttributes: map[string]string{"mode": "memory", "cache": "private"},
		},
		resolver.NewNoopResolver(),
		logger,
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		t.Fatalf("create SQLite database: %v", err)
	}

	executionStore, err := persistencesql.NewSQLExecutionStore(database, logger, serializer)
	if err != nil {
		_ = database.Close()
		t.Fatalf("create SQLite execution store: %v", err)
	}
	return persistence.NewExecutionManager(
		executionStore,
		serializer,
		nil,
		logger,
		dynamicconfig.GetIntPropertyFn(4*1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)
}

func persistEagerStartConflictHistory(
	t testing.TB,
	executionManager persistence.ExecutionManager,
	ctx context.Context,
	request *persistence.UpdateWorkflowExecutionRequest,
) {
	t.Helper()
	if len(request.NewWorkflowEvents) == 0 {
		t.Fatal("eager-start conflict transaction did not produce history events")
	}
	for index, workflowEvents := range request.NewWorkflowEvents {
		if len(workflowEvents.Events) == 0 {
			t.Fatal("eager-start conflict transaction produced an empty history batch")
		}
		_, err := executionManager.AppendHistoryNodes(ctx, &persistence.AppendHistoryNodesRequest{
			ShardID:           request.ShardID,
			IsNewBranch:       index == 0,
			Info:              "eager-start-conflict-fixture",
			BranchToken:       workflowEvents.BranchToken,
			Events:            workflowEvents.Events,
			PrevTransactionID: workflowEvents.PrevTxnID,
			TransactionID:     workflowEvents.TxnID,
		})
		if err != nil {
			t.Fatalf("persist eager-start conflict history: %v", err)
		}
	}
}

func newEagerStartConflictFixture(t testing.TB) *eagerStartConflictFixture {
	tv := testvars.New(t).WithNamespaceID(tests.NamespaceID)
	tv = tv.WithRunID(tv.Any().RunID())
	config := tests.NewDynamicConfig()
	logger := log.NewNoopLogger()
	ctrl := gomock.NewController(t)
	timeSource := clock.NewEventTimeSource().Update(eagerStartConflictFixtureTime)

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

	mockShard := shard.NewTestContextWithTimeSource(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
		timeSource,
	)
	mockShard.SetLoggers(logger)

	registry := hsm.NewRegistry()
	_ = workflow.RegisterStateMachine(registry)
	mockShard.SetStateMachineRegistry(registry)

	mockNamespaceCache := mockShard.Resource.NamespaceCache
	mockExecutionMgr := mockShard.Resource.ExecutionMgr
	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockVisibilityManager := mockShard.Resource.VisibilityManager
	mockEventsCache := mockShard.MockEventsCache
	fixture := &eagerStartConflictFixture{}
	sqliteExecutionManager := newSQLiteHistoryExecutionManager(t)
	countingExecutionManager := &sqliteHistoryReadCounter{
		ExecutionManager:       mockExecutionMgr,
		sqliteExecutionManager: sqliteExecutionManager,
		readHistoryCalls:       &fixture.readHistoryCalls,
	}
	shardContext := &eagerStartConflictShardContext{
		ShardContext:     mockShard,
		executionManager: countingExecutionManager,
	}

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
		func(searchAttributes map[string]any) (map[string]any, error) {
			return searchAttributes, nil
		},
	).AnyTimes()

	workflowCache := wcache.NewHostLevelCache(mockShard.GetConfig(), mockShard.GetLogger(), metrics.NoopMetricsHandler)
	mockWorkflowStateReplicator := ndc.NewMockWorkflowStateReplicator(ctrl)

	h := &historyEngineImpl{
		currentClusterName: mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       shardContext,
		clusterMetadata:    mockClusterMetadata,
		executionManager:   countingExecutionManager,
		logger:             logger,
		throttledLogger:    logger,
		metricsHandler:     metrics.NoopMetricsHandler,
		tokenSerializer:    tasktoken.NewSerializer(),
		config:             config,
		timeSource:         mockShard.GetTimeSource(),
		eventNotifier:      events.NewNotifier(timeSource, metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
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
			metrics.NoopMetricsHandler,
			log.NewNoopLogger(),
		),
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(shardContext, workflowCache),
		persistenceVisibilityMgr:   mockVisibilityManager,
		nDCWorkflowStateReplicator: mockWorkflowStateReplicator,
		workerDeploymentClient:     noopWorkerDeploymentClient{},
	}
	mockShard.SetEngineForTesting(h)

	startTime := timestamppb.New(timeSource.Now().Add(-2 * time.Second))
	currentWorkflowConditionFailedError := makeCurrentWorkflowConditionFailedError(tv, startTime)
	mockExecutionMgr.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		gomock.Cond(func(request *persistence.CreateWorkflowExecutionRequest) bool {
			return request.Mode == persistence.CreateWorkflowModeBrandNew
		}),
	).Return(nil, currentWorkflowConditionFailedError).AnyTimes()

	currentMutableState := workflow.NewMutableState(
		shardContext,
		mockEventsCache,
		log.NewTestLogger(),
		tests.GlobalNamespaceEntry,
		tv.WorkflowID(),
		tv.RunID(),
		startTime.AsTime(),
	)
	currentMutableState.GetExecutionInfo().ExecutionTime = currentMutableState.GetExecutionState().StartTime
	currentMutableState.GetExecutionInfo().TransitionHistory = workflow.UpdatedTransitionHistory(
		currentMutableState.GetExecutionInfo().TransitionHistory,
		tests.Version,
	)
	_ = currentMutableState.UpdateCurrentVersion(tests.Version, false)
	_ = currentMutableState.SetHistoryTree(nil, nil, tv.RunID())
	currentMutableState.GetExecutionInfo().VersionHistories.Histories[0].Items = []*historyspb.VersionHistoryItem{{Version: 0, EventId: 0}}

	mockExecutionMgr.EXPECT().UpdateWorkflowExecution(
		gomock.Any(),
		gomock.Cond(func(request *persistence.UpdateWorkflowExecutionRequest) bool {
			return request.UpdateWorkflowMutation.ExecutionState.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		}),
	).DoAndReturn(func(ctx context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		persistEagerStartConflictHistory(t, sqliteExecutionManager, ctx, request)
		return &persistence.UpdateWorkflowExecutionResponse{
			UpdateMutableStateStats: persistence.MutableStateStatistics{
				HistoryStatistics: &persistence.HistoryStatistics{SizeDiff: 1},
			},
		}, nil
	}).AnyTimes()
	currentWorkflowState := workflow.TestCloneToProto(context.Background(), currentMutableState)
	mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
			return &persistence.GetWorkflowExecutionResponse{
				State: proto.Clone(currentWorkflowState).(*persistencespb.WorkflowMutableState),
			}, nil
		},
	).AnyTimes()

	fixture.engine = h
	fixture.request = &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: tv.NamespaceID().String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                tv.NamespaceID().String(),
			WorkflowId:               tv.WorkflowID(),
			WorkflowType:             tv.WorkflowType(),
			TaskQueue:                tv.TaskQueue(),
			WorkflowExecutionTimeout: durationpb.New(time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
			WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
			Identity:                 tv.WorkerIdentity(),
			RequestId:                uuid.NewString(),
			RequestEagerExecution:    true,
		},
	}
	fixture.resetWorkflowCache = func() {
		workflowCache := wcache.NewHostLevelCache(mockShard.GetConfig(), mockShard.GetLogger(), metrics.NoopMetricsHandler)
		h.workflowConsistencyChecker = api.NewWorkflowConsistencyChecker(shardContext, workflowCache)
	}
	fixture.close = func() {
		sqliteExecutionManager.Close()
		ctrl.Finish()
		mockShard.StopForTest()
	}
	return fixture
}

func assertEagerStartConflictResponse(t testing.TB, response *historyservice.StartWorkflowExecutionResponse, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("StartWorkflowExecution failed: %v", err)
	}
	if response == nil || response.GetEagerWorkflowTask() == nil {
		t.Fatal("StartWorkflowExecution did not return an eager workflow task")
	}

	historyEvents := response.GetEagerWorkflowTask().GetHistory().GetEvents()
	wantEventTypes := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
	}
	if len(historyEvents) != len(wantEventTypes) {
		t.Fatalf("eager workflow task returned %d history events, want %d", len(historyEvents), len(wantEventTypes))
	}
	for index, wantEventType := range wantEventTypes {
		if historyEvents[index].GetEventId() != int64(index+1) {
			t.Fatalf("history event %d has ID %d, want %d", index, historyEvents[index].GetEventId(), index+1)
		}
		if historyEvents[index].GetEventType() != wantEventType {
			t.Fatalf("history event %d has type %s, want %s", index, historyEvents[index].GetEventType(), wantEventType)
		}
	}
}

func TestEagerStartConflictReturnsInitialEvents(t *testing.T) {
	fixture := newEagerStartConflictFixture(t)
	defer fixture.close()

	response, err := fixture.engine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), fixture.request)
	assertEagerStartConflictResponse(t, response, err)
}

func BenchmarkEagerStartConflict(b *testing.B) {
	fixture := newEagerStartConflictFixture(b)
	defer fixture.close()

	var readHistoryCalls int64
	for b.Loop() {
		fixture.resetWorkflowCache()
		fixture.request.StartRequest.RequestId = uuid.NewString()
		readHistoryCallsBefore := fixture.readHistoryCalls.Load()

		response, err := fixture.engine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), fixture.request)

		assertEagerStartConflictResponse(b, response, err)
		readHistoryCalls += fixture.readHistoryCalls.Load() - readHistoryCallsBefore
	}
	b.ReportMetric(float64(readHistoryCalls)/float64(b.N), "db_calls/op")
}
