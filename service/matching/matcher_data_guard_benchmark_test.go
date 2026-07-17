package matching

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
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

func matcherDataFindMatchLockHoldNanos(data *matcherData, samples int) float64 {
	var total time.Duration
	for range samples {
		data.lock.Lock()
		start := time.Now()
		task, poller := data.findMatch(false)
		total += time.Since(start)
		data.lock.Unlock()
		runtime.KeepAlive(task)
		runtime.KeepAlive(poller)
	}
	return float64(total) / float64(samples)
}

func matcherDataFindMatchLatencyP99Nanos(data *matcherData, samplesPerWorker int) float64 {
	workers := runtime.GOMAXPROCS(0)
	latencies := make([]time.Duration, workers*samplesPerWorker)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			var task *internalTask
			var poller *waitingPoller
			for i := range samplesPerWorker {
				started := time.Now()
				data.lock.Lock()
				task, poller = data.findMatch(false)
				data.lock.Unlock()
				latencies[worker*samplesPerWorker+i] = time.Since(started)
			}
			runtime.KeepAlive(task)
			runtime.KeepAlive(poller)
		}(worker)
	}
	close(start)
	wg.Wait()

	slices.Sort(latencies)
	return float64(latencies[(len(latencies)*99+99)/100-1])
}

func benchmarkMatcherDataFindMatch(b *testing.B, data *matcherData, wantMatch bool) {
	b.Helper()

	data.lock.Lock()
	task, poller := data.findMatch(false)
	data.lock.Unlock()
	if (task != nil && poller != nil) != wantMatch {
		b.Fatalf("findMatch() = (%v, %v), want match=%t", task, poller, wantMatch)
	}

	lockHoldNanos := matcherDataFindMatchLockHoldNanos(data, 100)
	matchLatencyP99Nanos := matcherDataFindMatchLatencyP99Nanos(data, 1000)
	b.ReportAllocs()
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

func matcherDataMixedPollerLifecycleLockHoldNanos(data *matcherData, samples int) float64 {
	task := &internalTask{}
	normalPoller := &waitingPoller{startTime: time.Unix(0, 0)}
	queryOnlyPoller := &waitingPoller{queryOnly: true, startTime: time.Unix(1, 0)}
	var total time.Duration
	for range samples {
		data.lock.Lock()
		start := time.Now()
		matcherDataMixedPollerLifecycle(data, task, normalPoller, queryOnlyPoller)
		total += time.Since(start)
		data.lock.Unlock()
	}
	return float64(total) / float64(samples)
}

func matcherDataMixedPollerLifecycleLatencyP99Nanos(data *matcherData, samplesPerWorker int) float64 {
	workers := runtime.GOMAXPROCS(0)
	latencies := make([]time.Duration, workers*samplesPerWorker)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			task := &internalTask{}
			normalPoller := &waitingPoller{startTime: time.Unix(0, 0)}
			queryOnlyPoller := &waitingPoller{queryOnly: true, startTime: time.Unix(1, 0)}
			<-start
			for i := range samplesPerWorker {
				started := time.Now()
				data.lock.Lock()
				matcherDataMixedPollerLifecycle(data, task, normalPoller, queryOnlyPoller)
				data.lock.Unlock()
				latencies[worker*samplesPerWorker+i] = time.Since(started)
			}
		}(worker)
	}
	close(start)
	wg.Wait()

	slices.Sort(latencies)
	return float64(latencies[(len(latencies)*99+99)/100-1])
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

	lockHoldNanos := matcherDataMixedPollerLifecycleLockHoldNanos(data, 100)
	matchLatencyP99Nanos := matcherDataMixedPollerLifecycleLatencyP99Nanos(data, 1000)
	b.ReportAllocs()
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
