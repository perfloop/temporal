package workflow

import (
	"fmt"
	"testing"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	redirectBenchmarkInitialBuildID = "build-before-redirect"
	redirectBenchmarkTargetBuildID  = "build-after-redirect"
)

type futureRetryRedirectFixture struct {
	mutableState          *MutableStateImpl
	startingTaskEventID   int64
	futureRetries         []*persistencespb.ActivityInfo
	retryTimerTasks       []*tasks.ActivityRetryTimerTask
	originalExecutionInfo *persistencespb.WorkflowExecutionInfo
	originalApproxSize    int
}

func newFutureRetryRedirectFixture(tb testing.TB, retryCount int) *futureRetryRedirectFixture {
	tb.Helper()

	controller := gomock.NewController(tb)
	timeSource := clock.NewEventTimeSource()
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource.Update(now)

	config := tests.NewDynamicConfig()
	config.EnableTransitionHistory = func(string) bool { return true }
	testShard := shard.NewTestContextWithTimeSource(
		controller,
		&persistencespb.ShardInfo{ShardId: 1, RangeId: 1},
		config,
		timeSource,
	)
	eventsCache := events.NewMockCache(controller)
	testShard.SetEventsCacheForTesting(eventsCache)
	testShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(
		tests.GlobalNamespaceEntry.IsGlobalNamespace(),
		tests.GlobalNamespaceEntry.FailoverVersion(tests.WorkflowID),
	).Return(cluster.TestCurrentClusterName).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	testShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()

	registry := hsm.NewRegistry()
	if err := RegisterStateMachine(registry); err != nil {
		tb.Fatalf("register workflow state machine: %v", err)
	}
	if err := callbacks.RegisterStateMachine(registry); err != nil {
		tb.Fatalf("register callback state machine: %v", err)
	}
	if err := nexusoperations.RegisterStateMachines(registry); err != nil {
		tb.Fatalf("register Nexus state machines: %v", err)
	}
	testShard.SetStateMachineRegistry(registry)

	tb.Cleanup(func() {
		controller.Finish()
		testShard.StopForTest()
	})

	mutableState := NewMutableState(
		testShard,
		eventsCache,
		testShard.GetLogger(),
		tests.GlobalNamespaceEntry,
		tests.WorkflowID,
		tests.RunID,
		now,
	)
	mutableState.executionInfo.ExecutionTime = mutableState.executionState.StartTime
	mutableState.executionInfo.AssignedBuildId = redirectBenchmarkInitialBuildID
	mutableState.executionInfo.LastCompletedWorkflowTaskStartedEventId = 1

	fixture := &futureRetryRedirectFixture{
		mutableState:        mutableState,
		startingTaskEventID: 1,
		futureRetries:       make([]*persistencespb.ActivityInfo, 0, retryCount),
	}

	mutableState.addPendingActivityInfo(&persistencespb.ActivityInfo{
		ScheduledEventId: fixture.startingTaskEventID,
		StartedEventId:   common.EmptyEventID,
		ActivityId:       "redirect-trigger",
		TaskQueue:        "redirect-benchmark-task-queue",
	})

	for i := 0; i < retryCount; i++ {
		activity := &persistencespb.ActivityInfo{
			Version:          1,
			ScheduledEventId: int64(i + 2),
			StartedEventId:   common.EmptyEventID,
			ScheduledTime:    timestamppb.New(now.Add(time.Hour)),
			ActivityId:       fmt.Sprintf("delayed-retry-%d", i),
			TaskQueue:        "redirect-benchmark-task-queue",
			HasRetryPolicy:   true,
			Attempt:          2,
			BuildIdInfo: &persistencespb.ActivityInfo_UseWorkflowBuildIdInfo_{
				UseWorkflowBuildIdInfo: &persistencespb.ActivityInfo_UseWorkflowBuildIdInfo{
					LastUsedBuildId: redirectBenchmarkInitialBuildID,
				},
			},
		}
		mutableState.addPendingActivityInfo(activity)
		if err := mutableState.taskGenerator.GenerateActivityRetryTasks(activity); err != nil {
			tb.Fatalf("generate retry timer for activity %d: %v", activity.ScheduledEventId, err)
		}
		fixture.futureRetries = append(fixture.futureRetries, activity)
	}

	for _, task := range mutableState.PopTasks()[tasks.CategoryTimer] {
		retryTask, ok := task.(*tasks.ActivityRetryTimerTask)
		if !ok {
			tb.Fatalf("generated timer task has type %T, want *ActivityRetryTimerTask", task)
		}
		fixture.retryTimerTasks = append(fixture.retryTimerTasks, retryTask)
	}
	if len(fixture.retryTimerTasks) != retryCount {
		tb.Fatalf("generated %d retry timer tasks, want %d", len(fixture.retryTimerTasks), retryCount)
	}

	fixture.originalExecutionInfo = proto.Clone(mutableState.executionInfo).(*persistencespb.WorkflowExecutionInfo)
	fixture.originalApproxSize = mutableState.approximateSize
	fixture.reset()
	return fixture
}

func (f *futureRetryRedirectFixture) reset() {
	f.mutableState.executionInfo = proto.Clone(f.originalExecutionInfo).(*persistencespb.WorkflowExecutionInfo)
	f.mutableState.approximateSize = f.originalApproxSize
	f.mutableState.updateActivityInfos = make(map[int64]*persistencespb.ActivityInfo)
	f.mutableState.syncActivityTasks = make(map[int64]struct{})
	f.mutableState.activityInfosUserDataUpdated = make(map[int64]struct{})
	f.mutableState.InsertTasks = make(map[tasks.Category][]tasks.Task)
	f.mutableState.BestEffortDeleteTasks = make(map[tasks.Category][]tasks.Key)
	f.mutableState.visibilityUpdated = false
	f.mutableState.executionStateUpdated = false
	f.mutableState.workflowTaskUpdated = false
	f.mutableState.reapplyEventsCandidate = nil

	for _, activity := range f.futureRetries {
		activity.Stamp = 0
		activity.RetryLastFailure = nil
	}
}

func TestFutureRetryTimerUsesRedirectedWorkflowBuildID(t *testing.T) {
	fixture := newFutureRetryRedirectFixture(t, 1)
	retry := fixture.futureRetries[0]
	retryTimer := fixture.retryTimerTasks[0]

	if err := fixture.mutableState.UpdateBuildIdAssignment(redirectBenchmarkTargetBuildID); err != nil {
		t.Fatalf("update workflow build ID: %v", err)
	}

	if retryTimer.EventID != retry.ScheduledEventId {
		t.Fatalf("retry timer event ID = %d, want %d", retryTimer.EventID, retry.ScheduledEventId)
	}
	if retryTimer.Attempt != retry.Attempt {
		t.Fatalf("retry timer attempt = %d, want %d", retryTimer.Attempt, retry.Attempt)
	}
	if retryTimer.Stamp != retry.Stamp {
		t.Fatalf("retry timer stamp = %d, want unchanged activity stamp %d", retryTimer.Stamp, retry.Stamp)
	}
	if got := MakeDirectiveForActivityTask(fixture.mutableState, retry).GetBuildId(); got != redirectBenchmarkTargetBuildID {
		t.Fatalf("retry task directive build ID = %q, want %q", got, redirectBenchmarkTargetBuildID)
	}
}

func BenchmarkApplyBuildIdRedirectFutureRetries(b *testing.B) {
	for _, retryCount := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("%d", retryCount), func(b *testing.B) {
			fixture := newFutureRetryRedirectFixture(b, retryCount)
			b.ReportAllocs()

			for b.Loop() {
				b.StopTimer()
				fixture.reset()
				b.StartTimer()

				err := fixture.mutableState.ApplyBuildIdRedirect(
					fixture.startingTaskEventID,
					redirectBenchmarkTargetBuildID,
					1,
				)

				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if got := fixture.mutableState.GetAssignedBuildId(); got != redirectBenchmarkTargetBuildID {
					b.Fatalf("assigned build ID = %q, want %q", got, redirectBenchmarkTargetBuildID)
				}
			}
		})
	}
}
