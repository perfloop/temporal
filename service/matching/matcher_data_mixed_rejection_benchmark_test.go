package matching

import (
	"fmt"
	"testing"
	"time"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/clock"
)

func matcherDataWithMixedRejectedPollers(tasks, pollers int) (*matcherData, *waitingPoller) {
	data := &matcherData{timeSource: clock.NewRealTimeSource()}
	for range tasks {
		data.tasks.Add(&internalTask{effectivePriority: effectivePriorityFactor * priorityKey(2)})
	}
	for i := range pollers - 1 {
		data.pollers.Add(&waitingPoller{queryOnly: true, startTime: time.Unix(int64(i), 0)})
	}

	normalPoller := &waitingPoller{
		startTime: time.Unix(int64(pollers), 0),
		pollMetadata: &pollMetadata{
			conditions: &matchingservice.PollConditions{MinPriority: 1},
		},
	}
	data.pollers.Add(normalPoller)
	return data, normalPoller
}

func assertMatcherDataMixedPollerRejection(b *testing.B, data *matcherData, normalPoller *waitingPoller) {
	b.Helper()

	queryOnlyPollers := 0
	normalPollers := 0
	for _, poller := range data.pollers.heap {
		if poller.queryOnly {
			queryOnlyPollers++
		}
		if poller == normalPoller {
			normalPollers++
		}
	}
	if normalPollers != 1 || normalPoller.queryOnly || normalPoller.minPriority() != 1 || queryOnlyPollers != data.pollers.Len()-1 {
		b.Fatal("want one min-priority normal poller and otherwise query-only pollers")
	}

	data.lock.Lock()
	task, poller := data.findMatch(false)
	data.lock.Unlock()
	if task != nil || poller != nil {
		b.Fatalf("findMatch() = (%v, %v), want no match", task, poller)
	}
}

func benchmarkMatcherDataMixedPollerRejectionFindMatch(b *testing.B, data *matcherData) {
	b.Helper()

	newOperation := func() matcherDataBenchmarkOperation {
		return matcherDataFindMatchOperation(data)
	}
	lockHoldNanos := matcherDataLockHoldNanos(data, 100, newOperation)
	matchLatencyP99Nanos := matcherDataMatchLatencyP99Nanos(data, 1000, newOperation)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		operation := newOperation()
		for pb.Next() {
			data.lock.Lock()
			operation.run()
			data.lock.Unlock()
		}
		operation.keepAlive()
	})
	b.ReportMetric(lockHoldNanos, "lock_hold_ns/op")
	b.ReportMetric(matchLatencyP99Nanos, "match_latency_p99_ns")
}

func benchmarkMatcherDataMixedPollerRejectionFindAndWakeMatches(b *testing.B, data *matcherData) {
	b.Helper()

	operation := matcherDataFindAndWakeMatchesOperation(data)
	data.lock.Lock()
	operation.run()
	data.lock.Unlock()
	operation.keepAlive()

	newOperation := func() matcherDataBenchmarkOperation {
		return matcherDataFindAndWakeMatchesOperation(data)
	}
	lockHoldNanos := matcherDataLockHoldNanos(data, 100, newOperation)
	matchLatencyP99Nanos := matcherDataMatchLatencyP99Nanos(data, 1000, newOperation)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		operation := newOperation()
		for pb.Next() {
			data.lock.Lock()
			operation.run()
			data.lock.Unlock()
		}
		operation.keepAlive()
	})
	b.ReportMetric(lockHoldNanos, "lock_hold_ns/op")
	b.ReportMetric(matchLatencyP99Nanos, "match_latency_p99_ns")
}

func BenchmarkMatcherDataMixedPollerRejection(b *testing.B) {
	for _, size := range []struct {
		tasks   int
		pollers int
	}{
		{tasks: 10, pollers: 10},
		{tasks: 10, pollers: 1000},
		{tasks: 1000, pollers: 10},
		{tasks: 1000, pollers: 1000},
	} {
		b.Run(fmt.Sprintf("FindMatch/NormalPollerMinPriority/tasks=%d/pollers=%d", size.tasks, size.pollers), func(b *testing.B) {
			data, normalPoller := matcherDataWithMixedRejectedPollers(size.tasks, size.pollers)
			assertMatcherDataMixedPollerRejection(b, data, normalPoller)
			benchmarkMatcherDataMixedPollerRejectionFindMatch(b, data)
		})
		b.Run(fmt.Sprintf("FindAndWakeMatches/NormalPollerMinPriority/tasks=%d/pollers=%d", size.tasks, size.pollers), func(b *testing.B) {
			data, normalPoller := matcherDataWithMixedRejectedPollers(size.tasks, size.pollers)
			assertMatcherDataMixedPollerRejection(b, data, normalPoller)
			benchmarkMatcherDataMixedPollerRejectionFindAndWakeMatches(b, data)
		})
	}
}
