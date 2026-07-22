package history

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
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
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
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
	"google.golang.org/protobuf/types/known/durationpb"
)

type eagerStartConflictPersistence struct {
	executionManager persistence.ExecutionManager
	shardManager     persistence.ShardManager
	factory          *persistencesql.Factory
}

func (p *eagerStartConflictPersistence) Close() {
	p.executionManager.Close()
	p.shardManager.Close()
	p.factory.Close()
}

// storageHistoryReadCounter decorates the production ExecutionManager. It only
// counts a successful history response after the real networked PostgreSQL
// store has executed it; all other persistence operations use the same manager.
type storageHistoryReadCounter struct {
	persistence.ExecutionManager
	readHistoryCalls *atomic.Int64
}

func (m *storageHistoryReadCounter) ReadHistoryBranch(
	ctx context.Context,
	request *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadHistoryBranchResponse, error) {
	response, err := m.ExecutionManager.ReadHistoryBranch(ctx, request)
	if err == nil {
		m.readHistoryCalls.Add(1)
	}
	return response, err
}

type eagerStartConflictFixture struct {
	engine           *historyEngineImpl
	prepare          func(int) []*historyservice.StartWorkflowExecutionRequest
	readHistoryCalls atomic.Int64
	close            func()
}

func newEagerStartConflictPersistence(t testing.TB) *eagerStartConflictPersistence {
	t.Helper()

	address := os.Getenv("TEMPORAL_EAGER_START_CONFLICT_POSTGRES_ADDR")
	schemaPath := os.Getenv("TEMPORAL_EAGER_START_CONFLICT_POSTGRES_SCHEMA")
	if address == "" || schemaPath == "" {
		t.Fatal("eager-start conflict fixture requires TEMPORAL_EAGER_START_CONFLICT_POSTGRES_ADDR and TEMPORAL_EAGER_START_CONFLICT_POSTGRES_SCHEMA")
	}
	schemaPath, err := filepath.Abs(schemaPath)
	if err != nil {
		t.Fatalf("resolve PostgreSQL schema path: %v", err)
	}

	logger := log.NewNoopLogger()
	serializer := serialization.NewSerializer()
	cfg := config.SQL{
		User:              "temporal",
		ConnectAddr:       address,
		ConnectProtocol:   "tcp",
		PluginName:        postgresql.PluginName,
		DatabaseName:      "temporal_eager_start_conflict_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		ConnectAttributes: map[string]string{"sslmode": "disable"},
	}
	adminCfg := cfg
	adminCfg.DatabaseName = ""
	adminDB, err := persistencesql.NewSQLAdminDB(
		sqlplugin.DbKindUnknown,
		&adminCfg,
		resolver.NewNoopResolver(),
		logger,
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	if err := adminDB.CreateDatabase(cfg.DatabaseName); err != nil {
		_ = adminDB.Close()
		t.Fatalf("create PostgreSQL fixture database: %v", err)
	}
	if err := adminDB.Close(); err != nil {
		t.Fatalf("close PostgreSQL admin connection: %v", err)
	}

	schemaStatements, err := persistence.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		t.Fatalf("load PostgreSQL fixture schema: %v", err)
	}
	schemaDB, err := persistencesql.NewSQLAdminDB(
		sqlplugin.DbKindUnknown,
		&cfg,
		resolver.NewNoopResolver(),
		logger,
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		t.Fatalf("open PostgreSQL fixture database: %v", err)
	}
	for _, statement := range schemaStatements {
		if err := schemaDB.Exec(statement); err != nil {
			_ = schemaDB.Close()
			t.Fatalf("apply PostgreSQL fixture schema: %v", err)
		}
	}
	if err := schemaDB.Close(); err != nil {
		t.Fatalf("close PostgreSQL fixture database: %v", err)
	}

	factory := persistencesql.NewFactory(
		cfg,
		resolver.NewNoopResolver(),
		cluster.TestCurrentClusterName,
		logger,
		metrics.NoopMetricsHandler,
		serializer,
	)
	shardStore, err := factory.NewShardStore()
	if err != nil {
		factory.Close()
		t.Fatalf("create PostgreSQL shard store: %v", err)
	}
	executionStore, err := factory.NewExecutionStore()
	if err != nil {
		factory.Close()
		t.Fatalf("create PostgreSQL execution store: %v", err)
	}
	return &eagerStartConflictPersistence{
		executionManager: persistence.NewExecutionManager(
			executionStore,
			serializer,
			nil,
			logger,
			dynamicconfig.GetIntPropertyFn(4*1024*1024),
			dynamicconfig.GetBoolPropertyFn(false),
		),
		shardManager: persistence.NewShardManager(shardStore, serializer),
		factory:      factory,
	}
}

func newEagerStartConflictFixture(t testing.TB) *eagerStartConflictFixture {
	tv := testvars.New(t).WithNamespaceID(tests.NamespaceID)
	config := tests.NewDynamicConfig()
	config.WorkflowIdReuseMinimalInterval = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(0)
	logger := log.NewNoopLogger()
	ctrl := gomock.NewController(t)
	persistenceStore := newEagerStartConflictPersistence(t)
	fixture := &eagerStartConflictFixture{}
	countingExecutionManager := &storageHistoryReadCounter{
		ExecutionManager: persistenceStore.executionManager,
		readHistoryCalls: &fixture.readHistoryCalls,
	}

	shardInfo := &persistencespb.ShardInfo{
		ShardId: 1,
		RangeId: 1,
	}
	if _, err := persistenceStore.shardManager.GetOrCreateShard(context.Background(), &persistence.GetOrCreateShardRequest{
		ShardID:          shardInfo.ShardId,
		InitialShardInfo: shardInfo,
	}); err != nil {
		persistenceStore.Close()
		t.Fatalf("create PostgreSQL fixture shard: %v", err)
	}

	// StubContext installs the same actual execution manager used by the
	// history engine into ContextImpl, so Create/Update/Get/Read all cross the
	// production persistence boundary rather than a gomock expectation.
	h := &historyEngineImpl{}
	mockShard := shard.NewStubContext(
		ctrl,
		shard.ContextConfigOverrides{
			ShardInfo:        shardInfo,
			Config:           config,
			ExecutionManager: countingExecutionManager,
		},
		h,
	)
	mockShard.SetLoggers(logger)

	registry := hsm.NewRegistry()
	_ = workflow.RegisterStateMachine(registry)
	mockShard.SetStateMachineRegistry(registry)

	mockNamespaceCache := mockShard.Resource.NamespaceCache
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
		func(searchAttributes map[string]any) (map[string]any, error) {
			return searchAttributes, nil
		},
	).AnyTimes()

	workflowCache := wcache.NewHostLevelCache(mockShard.GetConfig(), mockShard.GetLogger(), metrics.NoopMetricsHandler)
	mockWorkflowStateReplicator := ndc.NewMockWorkflowStateReplicator(ctrl)
	archivalProcessor := mockArchivalProcessor(ctrl)
	txProcessor := mockTxProcessor(ctrl)
	timerProcessor := mockTimerProcessor(ctrl)
	visibilityProcessor := mockVisibilityProcessor(ctrl)
	memoryScheduledQueue := mockMemoryScheduledQueue(ctrl)

	*h = historyEngineImpl{
		currentClusterName: mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       mockShard,
		clusterMetadata:    mockClusterMetadata,
		executionManager:   countingExecutionManager,
		logger:             logger,
		throttledLogger:    logger,
		metricsHandler:     metrics.NoopMetricsHandler,
		tokenSerializer:    tasktoken.NewSerializer(),
		config:             config,
		timeSource:         mockShard.GetTimeSource(),
		eventNotifier:      events.NewNotifier(mockShard.GetTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			archivalProcessor.Category():    archivalProcessor,
			txProcessor.Category():          txProcessor,
			timerProcessor.Category():       timerProcessor,
			visibilityProcessor.Category():  visibilityProcessor,
			memoryScheduledQueue.Category(): memoryScheduledQueue,
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
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(mockShard, workflowCache),
		persistenceVisibilityMgr:   mockVisibilityManager,
		nDCWorkflowStateReplicator: mockWorkflowStateReplicator,
		workerDeploymentClient:     noopWorkerDeploymentClient{},
	}

	fixture.engine = h
	fixture.prepare = func(count int) []*historyservice.StartWorkflowExecutionRequest {
		requests := make([]*historyservice.StartWorkflowExecutionRequest, count)
		for index := range requests {
			workflowID := uuid.NewString()
			runID := uuid.NewString()
			currentMutableState := workflow.NewMutableState(
				mockShard,
				mockEventsCache,
				log.NewTestLogger(),
				tests.GlobalNamespaceEntry,
				workflowID,
				runID,
				mockShard.GetTimeSource().Now().Add(-2*time.Second),
			)
			currentMutableState.GetExecutionInfo().ExecutionTime = currentMutableState.GetExecutionState().StartTime
			currentMutableState.GetExecutionState().FirstExecutionRunId = runID
			currentMutableState.GetExecutionInfo().FirstExecutionRunId = runID
			currentMutableState.GetExecutionInfo().TransitionHistory = workflow.UpdatedTransitionHistory(
				currentMutableState.GetExecutionInfo().TransitionHistory,
				tests.Version,
			)
			if err := currentMutableState.UpdateCurrentVersion(tests.Version, false); err != nil {
				t.Fatalf("set current workflow version: %v", err)
			}
			if err := currentMutableState.SetHistoryTree(nil, nil, runID); err != nil {
				t.Fatalf("set current workflow history tree: %v", err)
			}
			snapshot, workflowEvents, err := currentMutableState.CloseTransactionAsSnapshot(
				context.Background(),
				historyi.TransactionPolicyActive,
			)
			if err != nil {
				t.Fatalf("close current workflow snapshot: %v", err)
			}
			if _, err := mockShard.CreateWorkflowExecution(context.Background(), &persistence.CreateWorkflowExecutionRequest{
				ShardID:             mockShard.GetShardID(),
				Mode:                persistence.CreateWorkflowModeBrandNew,
				NewWorkflowSnapshot: *snapshot,
				NewWorkflowEvents:   workflowEvents,
			}); err != nil {
				t.Fatalf("persist current workflow: %v", err)
			}

			requests[index] = &historyservice.StartWorkflowExecutionRequest{
				Attempt:     1,
				NamespaceId: tv.NamespaceID().String(),
				StartRequest: &workflowservice.StartWorkflowExecutionRequest{
					Namespace:                tv.NamespaceID().String(),
					WorkflowId:               workflowID,
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
		}
		return requests
	}
	fixture.close = func() {
		persistenceStore.Close()
		ctrl.Finish()
		mockShard.StopForTest()
	}
	return fixture
}

func mockTxProcessor(ctrl *gomock.Controller) *queues.MockQueue {
	queue := queues.NewMockQueue(ctrl)
	queue.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	queue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	return queue
}

func mockTimerProcessor(ctrl *gomock.Controller) *queues.MockQueue {
	queue := queues.NewMockQueue(ctrl)
	queue.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	queue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	return queue
}

func mockVisibilityProcessor(ctrl *gomock.Controller) *queues.MockQueue {
	queue := queues.NewMockQueue(ctrl)
	queue.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
	queue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	return queue
}

func mockArchivalProcessor(ctrl *gomock.Controller) *queues.MockQueue {
	queue := queues.NewMockQueue(ctrl)
	queue.EXPECT().Category().Return(tasks.CategoryArchival).AnyTimes()
	queue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	return queue
}

func mockMemoryScheduledQueue(ctrl *gomock.Controller) *queues.MockQueue {
	queue := queues.NewMockQueue(ctrl)
	queue.EXPECT().Category().Return(tasks.CategoryMemoryTimer).AnyTimes()
	queue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	return queue
}

func requireEagerStartConflictPostgres(t testing.TB) bool {
	t.Helper()
	if os.Getenv("TEMPORAL_EAGER_START_CONFLICT_POSTGRES_ADDR") == "" || os.Getenv("TEMPORAL_EAGER_START_CONFLICT_POSTGRES_SCHEMA") == "" {
		t.Skip("requires the PostgreSQL fixture started by scripts/perf/run-eager-start-conflict-postgres.sh")
		return false
	}
	return true
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
	if !requireEagerStartConflictPostgres(t) {
		return
	}
	fixture := newEagerStartConflictFixture(t)
	defer fixture.close()

	request := fixture.prepare(1)[0]
	response, err := fixture.engine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), request)
	assertEagerStartConflictResponse(t, response, err)
	if historyReads := fixture.readHistoryCalls.Load(); historyReads > 1 {
		t.Fatalf("eager conflict start made %d history reads, want at most 1", historyReads)
	}
}

func BenchmarkEagerStartConflict(b *testing.B) {
	if !requireEagerStartConflictPostgres(b) {
		return
	}
	fixture := newEagerStartConflictFixture(b)
	defer fixture.close()

	request := fixture.prepare(1)[0]
	for b.Loop() {
		request.StartRequest.RequestId = uuid.NewString()
		response, err := fixture.engine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), request)
		assertEagerStartConflictResponse(b, response, err)
	}
	b.ReportMetric(float64(fixture.readHistoryCalls.Load())/float64(b.N), "db_calls/op")
}
