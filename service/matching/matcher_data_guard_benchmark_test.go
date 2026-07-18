package matching

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"go.temporal.io/server/api/matchingservice/v1"
)

func matcherDataWithQueryOnlyQueryAtEnd(tasks, pollers int, rejectQuery bool) *matcherData {
	data := &matcherData{}
	for range tasks - 1 {
		data.tasks.Add(&internalTask{})
	}

	queryTask := &internalTask{query: &queryTaskInfo{}}
	if rejectQuery {
		queryTask.effectivePriority = effectivePriorityFactor * priorityKey(2)
	}
	data.tasks.Add(queryTask)

	for range pollers {
		poller := &waitingPoller{queryOnly: true}
		if rejectQuery {
			poller.pollMetadata = &pollMetadata{conditions: &matchingservice.PollConditions{MinPriority: 1}}
		}
		data.pollers.Add(poller)
	}
	return data
}

func matcherDataFindMatchOperation(data *matcherData) matcherDataBenchmarkOperation {
	var task *internalTask
	var poller *waitingPoller
	return matcherDataBenchmarkOperation{
		run: func() {
			task, poller = data.findMatch(false)
		},
		keepAlive: func() {
			runtime.KeepAlive(task)
			runtime.KeepAlive(poller)
		},
	}
}

func benchmarkMatcherDataFindMatch(b *testing.B, data *matcherData, wantMatch bool) {
	b.Helper()

	data.lock.Lock()
	task, poller := data.findMatch(false)
	data.lock.Unlock()
	if (task != nil && poller != nil) != wantMatch {
		b.Fatalf("findMatch() = (%v, %v), want match=%t", task, poller, wantMatch)
	}

	newOperation := func() matcherDataBenchmarkOperation {
		return matcherDataFindMatchOperation(data)
	}
	lockHoldNanos := matcherDataLockHoldNanos(data, 100, newOperation)
	matchLatencyP99Nanos := matcherDataMatchLatencyP99Nanos(data, 1000, newOperation)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var task *internalTask
		var poller *waitingPoller
		for pb.Next() {
			data.lock.Lock()
			task, poller = data.findMatch(false)
			data.lock.Unlock()
		}
		runtime.KeepAlive(task)
		runtime.KeepAlive(poller)
	})
	b.ReportMetric(lockHoldNanos, "lock_hold_ns/op")
	b.ReportMetric(matchLatencyP99Nanos, "match_latency_p99_ns")
}

func BenchmarkMatcherDataQueryOnlyCompatibility(b *testing.B) {
	for _, size := range []struct {
		tasks   int
		pollers int
	}{
		{tasks: 10, pollers: 10},
		{tasks: 10, pollers: 1000},
		{tasks: 1000, pollers: 10},
		{tasks: 1000, pollers: 1000},
	} {
		b.Run(fmt.Sprintf("QueryAtEnd/tasks=%d/pollers=%d", size.tasks, size.pollers), func(b *testing.B) {
			benchmarkMatcherDataFindMatch(b, matcherDataWithQueryOnlyQueryAtEnd(size.tasks, size.pollers, false), true)
		})
		b.Run(fmt.Sprintf("RejectedQueryAtEnd/tasks=%d/pollers=%d", size.tasks, size.pollers), func(b *testing.B) {
			benchmarkMatcherDataFindMatch(b, matcherDataWithQueryOnlyQueryAtEnd(size.tasks, size.pollers, true), false)
		})
	}
}

func matcherDataMixedPollerLifecycle(data *matcherData, task *internalTask, normalPoller, queryOnlyPoller *waitingPoller) (*internalTask, *waitingPoller) {
	data.tasks.Add(task)
	data.pollers.Add(normalPoller)
	data.pollers.Add(queryOnlyPoller)
	matchedTask, matchedPoller := data.findMatch(false)
	data.tasks.Remove(matchedTask)
	data.pollers.Remove(matchedPoller)
	data.pollers.Remove(queryOnlyPoller)
	return matchedTask, matchedPoller
}

func matcherDataMixedPollerLifecycleOperation(data *matcherData) matcherDataBenchmarkOperation {
	task := &internalTask{}
	normalPoller := &waitingPoller{startTime: time.Unix(0, 0)}
	queryOnlyPoller := &waitingPoller{queryOnly: true, startTime: time.Unix(1, 0)}
	var matchedTask *internalTask
	var matchedPoller *waitingPoller
	return matcherDataBenchmarkOperation{
		run: func() {
			matchedTask, matchedPoller = matcherDataMixedPollerLifecycle(data, task, normalPoller, queryOnlyPoller)
		},
		keepAlive: func() {
			runtime.KeepAlive(matchedTask)
			runtime.KeepAlive(matchedPoller)
		},
	}
}

func BenchmarkMatcherDataMixedPollerLifecycle(b *testing.B) {
	data := &matcherData{}
	data.lock.Lock()
	matchedTask, matchedPoller := matcherDataMixedPollerLifecycle(
		data,
		&internalTask{},
		&waitingPoller{startTime: time.Unix(0, 0)},
		&waitingPoller{queryOnly: true, startTime: time.Unix(1, 0)},
	)
	data.lock.Unlock()
	if matchedTask == nil || matchedPoller == nil || matchedPoller.queryOnly {
		b.Fatal("normal poller should match the normal task")
	}

	newOperation := func() matcherDataBenchmarkOperation {
		return matcherDataMixedPollerLifecycleOperation(data)
	}
	lockHoldNanos := matcherDataLockHoldNanos(data, 100, newOperation)
	matchLatencyP99Nanos := matcherDataMatchLatencyP99Nanos(data, 1000, newOperation)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		task := &internalTask{}
		normalPoller := &waitingPoller{startTime: time.Unix(0, 0)}
		queryOnlyPoller := &waitingPoller{queryOnly: true, startTime: time.Unix(1, 0)}
		for pb.Next() {
			data.lock.Lock()
			matcherDataMixedPollerLifecycle(data, task, normalPoller, queryOnlyPoller)
			data.lock.Unlock()
		}
	})
	b.ReportMetric(lockHoldNanos, "lock_hold_ns/op")
	b.ReportMetric(matchLatencyP99Nanos, "match_latency_p99_ns")
}
