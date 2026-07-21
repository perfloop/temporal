package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	matcherDataFindMatchQueryOnlyRejectionTasks   = 1000
	matcherDataFindMatchQueryOnlyRejectionPollers = 1000
)

func newMatcherDataFindMatchFixture(tb testing.TB, queryOnlyPollers int) (*matcherData, int64) {
	tb.Helper()
	if queryOnlyPollers < 0 || queryOnlyPollers > matcherDataFindMatchQueryOnlyRejectionPollers {
		tb.Fatal("invalid query-only poller count")
	}

	now := time.Unix(0, 0)
	timeSource := clock.NewEventTimeSource().Update(now)
	config := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("benchmark-namespace-id", "benchmark-task-queue").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		NewConfig(dynamicconfig.NewNoopCollection()),
		"benchmark-namespace",
	)
	rateLimitManager := newRateLimitManager(&mockUserDataManager{}, config, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	rateLimitManager.Start()
	tb.Cleanup(rateLimitManager.Stop)

	data := newMatcherData(config, log.NewNoopLogger(), timeSource, true, rateLimitManager, func() {})
	for id := range matcherDataFindMatchQueryOnlyRejectionTasks {
		data.tasks.Add(newInternalTaskFromBacklog(&persistencespb.AllocatedTaskInfo{
			Data: &persistencespb.TaskInfo{
				CreateTime: timestamppb.New(now),
			},
			TaskId: int64(id + 1),
		}, nil))
	}
	data.pollers.heap = make([]*waitingPoller, matcherDataFindMatchQueryOnlyRejectionPollers)
	for i := range data.pollers.heap {
		data.pollers.heap[i] = &waitingPoller{
			queryOnly:    i < queryOnlyPollers,
			pollMetadata: &pollMetadata{},
		}
	}

	return &data, timeSource.Now().UnixNano()
}

func TestMatcherDataFindMatchQueryOnlyPollerRejection(t *testing.T) {
	data, now := newMatcherDataFindMatchFixture(t, matcherDataFindMatchQueryOnlyRejectionPollers)

	data.lock.Lock()
	defer data.lock.Unlock()

	task, poller, delay := data.findMatch(true, now)
	require.Nil(t, task)
	require.Nil(t, poller)
	require.Zero(t, delay)
}

func BenchmarkMatcherDataFindMatchQueryOnlyRejection1000x1000(b *testing.B) {
	data, now := newMatcherDataFindMatchFixture(b, matcherDataFindMatchQueryOnlyRejectionPollers)

	data.lock.Lock()
	defer data.lock.Unlock()

	b.ReportAllocs()
	var task *internalTask
	var poller *waitingPoller
	var delay time.Duration
	for b.Loop() {
		task, poller, delay = data.findMatch(true, now)
	}
	if task != nil || poller != nil || delay != 0 {
		b.Fatal("query-only pollers matched a regular backlog task")
	}
}

func BenchmarkMatcherDataFindMatchMixedPollerTail1000x1000(b *testing.B) {
	data, now := newMatcherDataFindMatchFixture(b, matcherDataFindMatchQueryOnlyRejectionPollers-1)

	data.lock.Lock()
	defer data.lock.Unlock()

	b.ReportAllocs()
	var task *internalTask
	var poller *waitingPoller
	var delay time.Duration
	for b.Loop() {
		task, poller, delay = data.findMatch(true, now)
	}
	if task == nil || poller == nil || delay != 0 {
		b.Fatal("regular poller at the end of the heap did not match a regular backlog task")
	}
}
