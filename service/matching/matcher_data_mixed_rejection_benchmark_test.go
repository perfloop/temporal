package matching

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
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

	if data.pollers.heap[len(data.pollers.heap)-1] != normalPoller {
		b.Fatal("normal poller must follow the query-only heap prefix")
	}
	for _, poller := range data.pollers.heap[:len(data.pollers.heap)-1] {
		if !poller.queryOnly {
			b.Fatal("poller before normal poller must be query-only")
		}
	}

	data.lock.Lock()
	task, poller := data.findMatch(false)
	data.lock.Unlock()
	if task != nil || poller != nil {
		b.Fatalf("findMatch() = (%v, %v), want no match", task, poller)
	}
}

type mixedPollerRejectionOperation struct {
	run       func()
	keepAlive func()
}

func mixedPollerRejectionLockHoldNanos(
	data *matcherData,
	samples int,
	newOperation func() mixedPollerRejectionOperation,
) float64 {
	operation := newOperation()
	var total time.Duration
	for range samples {
		data.lock.Lock()
		started := time.Now()
		operation.run()
		total += time.Since(started)
		data.lock.Unlock()
	}
	operation.keepAlive()
	return float64(total) / float64(samples)
}

func mixedPollerRejectionLatencyP99Nanos(
	data *matcherData,
	samplesPerWorker int,
	newOperation func() mixedPollerRejectionOperation,
) float64 {
	workers := runtime.GOMAXPROCS(0)
	latencies := make([]time.Duration, workers*samplesPerWorker)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			operation := newOperation()
			<-start
			for i := range samplesPerWorker {
				started := time.Now()
				data.lock.Lock()
				operation.run()
				data.lock.Unlock()
				latencies[worker*samplesPerWorker+i] = time.Since(started)
			}
			operation.keepAlive()
		}(worker)
	}
	close(start)
	wg.Wait()
	slices.Sort(latencies)
	return float64(latencies[(len(latencies)*99+99)/100-1])
}

func mixedPollerRejectionFindMatchOperation(data *matcherData) mixedPollerRejectionOperation {
	var task *internalTask
	var poller *waitingPoller
	return mixedPollerRejectionOperation{
		run: func() {
			task, poller = data.findMatch(false)
		},
		keepAlive: func() {
			runtime.KeepAlive(task)
			runtime.KeepAlive(poller)
		},
	}
}

func mixedPollerRejectionFindAndWakeMatchesOperation(data *matcherData) mixedPollerRejectionOperation {
	var rateLimited bool
	return mixedPollerRejectionOperation{
		run: func() {
			rateLimited = data.findAndWakeMatches()
		},
		keepAlive: func() {
			if rateLimited {
				panic("findAndWakeMatches() unexpectedly rate limited")
			}
		},
	}
}

func benchmarkMatcherDataMixedPollerRejectionFindMatch(b *testing.B, data *matcherData) {
	b.Helper()

	newOperation := func() mixedPollerRejectionOperation {
		return mixedPollerRejectionFindMatchOperation(data)
	}
	lockHoldNanos := mixedPollerRejectionLockHoldNanos(data, 100, newOperation)
	matchLatencyP99Nanos := mixedPollerRejectionLatencyP99Nanos(data, 1000, newOperation)
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

	data.lock.Lock()
	rateLimited := data.findAndWakeMatches()
	data.lock.Unlock()
	if rateLimited {
		b.Fatal("findAndWakeMatches() unexpectedly rate limited")
	}

	newOperation := func() mixedPollerRejectionOperation {
		return mixedPollerRejectionFindAndWakeMatchesOperation(data)
	}
	lockHoldNanos := mixedPollerRejectionLockHoldNanos(data, 100, newOperation)
	matchLatencyP99Nanos := mixedPollerRejectionLatencyP99Nanos(data, 1000, newOperation)
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
