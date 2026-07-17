package shard

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/startworkflow"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	starterContentionConcurrency      = 8
	starterContentionWarmupIterations = 50
)

type starterContentionFixture struct {
	shardContext       *ContextTest
	consistencyChecker api.WorkflowConsistencyChecker
}

type starterContentionEngine struct {
	historyi.Engine
}

func (starterContentionEngine) NotifyNewTasks(map[tasks.Category][]tasks.Task) {}

func (starterContentionEngine) NotifyChasmExecution(chasm.ExecutionKey, []byte) {}

func newStarterContentionFixture(tb testing.TB) *starterContentionFixture {
	tb.Helper()

	serializer := serialization.NewSerializer()
	factory := persistencesql.NewFactory(
		config.SQL{
			ConnectAddr:     "localhost",
			ConnectProtocol: "tcp",
			PluginName:      "sqlite",
			DatabaseName:    filepath.Join(tb.TempDir(), "starter-contention.db"),
			MaxConns:        starterContentionConcurrency,
			MaxIdleConns:    starterContentionConcurrency,
			ConnectAttributes: map[string]string{
				"busy_timeout": "5000",
				"journal_mode": "wal",
				"setup":        "true",
			},
		},
		resolver.NewNoopResolver(),
		"starter-contention",
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		serializer,
	)
	tb.Cleanup(factory.Close)

	shardStore, err := factory.NewShardStore()
	if err != nil {
		tb.Fatal(err)
	}
	executionStore, err := factory.NewExecutionStore()
	if err != nil {
		tb.Fatal(err)
	}
	shardManager := persistence.NewShardManager(shardStore, serializer)
	shardResponse, err := shardManager.GetOrCreateShard(context.Background(), &persistence.GetOrCreateShardRequest{
		ShardID: 1,
		InitialShardInfo: &persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
	})
	if err != nil {
		tb.Fatal(err)
	}
	executionManager := persistence.NewExecutionManager(
		executionStore,
		serializer,
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(4*1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)

	controller := gomock.NewController(tb)
	historyConfig := tests.NewDynamicConfig()
	historyConfig.ShardIOConcurrency = dynamicconfig.GetIntPropertyFn(starterContentionConcurrency)
	shardContext := NewTestContext(controller, shardResponse.ShardInfo, historyConfig)
	shardContext.executionManager = executionManager
	shardContext.SetLoggers(log.NewNoopLogger())
	// ContextTest fixes its I/O semaphore width at one, so mirror the configured
	// non-Cassandra shard concurrency used by this contention workload.
	shardContext.ioSemaphore = locks.NewPrioritySemaphore(starterContentionConcurrency)

	registry := hsm.NewRegistry()
	if err := workflow.RegisterStateMachine(registry); err != nil {
		tb.Fatal(err)
	}
	shardContext.SetStateMachineRegistry(registry)
	shardContext.SetEngineForTesting(starterContentionEngine{})

	shardContext.Resource.NamespaceCache.EXPECT().
		GetNamespaceByID(tests.NamespaceID).
		Return(tests.GlobalNamespaceEntry, nil).
		AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().
		GetClusterID().
		Return(tests.Version).
		AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().
		IsVersionFromSameCluster(tests.Version, tests.Version).
		Return(true).
		AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().
		IsGlobalNamespaceEnabled().
		Return(false).
		AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().
		GetCurrentClusterName().
		Return(cluster.TestCurrentClusterName).
		AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().
		ClusterNameForFailoverVersion(false, common.EmptyVersion).
		Return(cluster.TestCurrentClusterName).
		AnyTimes()
	shardContext.Resource.ClusterMetadata.EXPECT().
		ClusterNameForFailoverVersion(true, tests.Version).
		Return(cluster.TestCurrentClusterName).
		AnyTimes()
	shardContext.MockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	workflowCache := wcache.NewHostLevelCache(historyConfig, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	return &starterContentionFixture{
		shardContext:       shardContext,
		consistencyChecker: api.NewWorkflowConsistencyChecker(shardContext, workflowCache),
	}
}

func (f *starterContentionFixture) newStarter(workflowID string, requestID string) (*startworkflow.Starter, error) {
	return startworkflow.NewStarter(
		f.shardContext,
		f.consistencyChecker,
		tasktoken.NewSerializer(),
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: tests.NamespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:                tests.NamespaceID.String(),
				WorkflowId:               workflowID,
				WorkflowType:             &commonpb.WorkflowType{Name: "contention-benchmark"},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: "contention-benchmark"},
				WorkflowExecutionTimeout: durationpb.New(time.Minute),
				WorkflowRunTimeout:       durationpb.New(time.Minute),
				WorkflowTaskTimeout:      durationpb.New(time.Second),
				WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
				WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
				Identity:                 "contention-benchmark",
				RequestId:                requestID,
			},
		},
		nil,
		nil,
		func(context.Context, *namespace.Namespace, string, string, int64) error { return nil },
		api.NewWorkflowLeaseAndContext,
	)
}

func (f *starterContentionFixture) invokeContendedStarts(
	ctx context.Context,
	workflowID string,
) ([starterContentionConcurrency]startworkflow.StartOutcome, [starterContentionConcurrency]error) {
	var outcomes [starterContentionConcurrency]startworkflow.StartOutcome
	var errs [starterContentionConcurrency]error
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range starterContentionConcurrency {
		wg.Go(func() {
			<-start

			starter, err := f.newStarter(workflowID, fmt.Sprintf("%s-%d", workflowID, i))
			if err == nil {
				_, outcomes[i], err = starter.Invoke(ctx)
			}
			errs[i] = err
		})
	}
	close(start)
	wg.Wait()

	return outcomes, errs
}

func requireExpectedContendedStartOutcomes(
	tb testing.TB,
	outcomes [starterContentionConcurrency]startworkflow.StartOutcome,
	errs [starterContentionConcurrency]error,
) {
	tb.Helper()

	newCount := 0
	for i := range outcomes {
		if errs[i] != nil {
			tb.Fatalf("start %d failed: %v", i, errs[i])
		}
		switch outcomes[i] {
		case startworkflow.StartNew:
			newCount++
		case startworkflow.StartReused:
		default:
			tb.Fatalf("start %d returned outcome %v", i, outcomes[i])
		}
	}
	if newCount != 1 {
		tb.Fatalf("expected one new execution, got %d", newCount)
	}
}

func TestStarterInvokeSQLiteConditionalCreatePreservesDuplicateResults(t *testing.T) {
	fixture := newStarterContentionFixture(t)
	outcomes, errs := fixture.invokeContendedStarts(context.Background(), "correctness-workflow")
	requireExpectedContendedStartOutcomes(t, outcomes, errs)
}

func BenchmarkStarterInvokeSQLiteConditionalCreate(b *testing.B) {
	b.StopTimer()
	runtime.SetBlockProfileRate(1)
	defer runtime.SetBlockProfileRate(0)

	fixture := newStarterContentionFixture(b)
	var sequence uint64
	for range starterContentionWarmupIterations {
		workflowID := fmt.Sprintf("contention-warmup-%d", atomic.AddUint64(&sequence, 1))
		outcomes, errs := fixture.invokeContendedStarts(context.Background(), workflowID)
		requireExpectedContendedStartOutcomes(b, outcomes, errs)
	}
	b.ReportAllocs()
	b.ResetTimer()
	lockBlockCyclesBefore := blockProfileCyclesFor(
		"go.temporal.io/server/service/history/api/startworkflow.(*Starter).lockCurrentWorkflowExecution",
	)

	b.StartTimer()
	for b.Loop() {
		workflowID := fmt.Sprintf("contention-workflow-%d", atomic.AddUint64(&sequence, 1))
		outcomes, errs := fixture.invokeContendedStarts(context.Background(), workflowID)
		requireExpectedContendedStartOutcomes(b, outcomes, errs)
	}
	b.StopTimer()

	lockBlockCyclesAfter := blockProfileCyclesFor(
		"go.temporal.io/server/service/history/api/startworkflow.(*Starter).lockCurrentWorkflowExecution",
	)
	b.ReportMetric(
		float64(lockBlockCyclesAfter-lockBlockCyclesBefore)/float64(b.N),
		"current_lock_block_cycles/op",
	)
}

func blockProfileCyclesFor(function string) int64 {
	n, _ := runtime.BlockProfile(nil)
	records := make([]runtime.BlockProfileRecord, n)
	n, ok := runtime.BlockProfile(records)
	if !ok {
		return blockProfileCyclesFor(function)
	}

	var cycles int64
	for _, record := range records[:n] {
		frames := runtime.CallersFrames(record.Stack())
		for {
			frame, more := frames.Next()
			if frame.Function == function {
				cycles += record.Cycles
				break
			}
			if !more {
				break
			}
		}
	}
	return cycles
}
