package matching

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/clock"
)

func matcherDataWithNoCompatibleQueryOnlyPollers(tasks, pollers int) *matcherData {
	data := &matcherData{timeSource: clock.NewRealTimeSource()}
	for range tasks {
		data.tasks.Add(&internalTask{})
	}
	for range pollers {
		data.pollers.Add(&waitingPoller{queryOnly: true})
	}
	return data
}

func TestMatcherDataFindMatchQueryOnlyPollers(t *testing.T) {
	data := &matcherData{}
	normalTask := &internalTask{}
	queryOnlyPoller := &waitingPoller{queryOnly: true}
	data.tasks.Add(normalTask)
	data.pollers.Add(queryOnlyPoller)
	data.pollers.Remove(queryOnlyPoller)

	normalPoller := &waitingPoller{}
	data.pollers.Add(normalPoller)
	if task, poller := data.findMatch(false); task != normalTask || poller != normalPoller {
		t.Fatalf("findMatch() = (%v, %v), want normal task matched to normal poller", task, poller)
	}
}

type matcherDataBenchmarkOperation struct {
	run       func()
	keepAlive func()
}

func matcherDataFindAndWakeMatchesOperation(data *matcherData) matcherDataBenchmarkOperation {
	return matcherDataBenchmarkOperation{
		run:       func() { data.findAndWakeMatches() },
		keepAlive: func() {},
	}
}

// matcherDataLockHoldNanos measures the time an operation holds matcherData.lock.
func matcherDataLockHoldNanos(data *matcherData, samples int, newOperation func() matcherDataBenchmarkOperation) float64 {
	operation := newOperation()
	var total time.Duration
	for range samples {
		data.lock.Lock()
		start := time.Now()
		operation.run()
		total += time.Since(start)
		data.lock.Unlock()
	}
	operation.keepAlive()
	return float64(total) / float64(samples)
}

// matcherDataMatchLatencyP99Nanos measures a caller's lock-wait plus operation latency.
func matcherDataMatchLatencyP99Nanos(data *matcherData, samplesPerWorker int, newOperation func() matcherDataBenchmarkOperation) float64 {
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

func BenchmarkMatcherDataFindAndWakeMatches(b *testing.B) {
	for _, size := range []struct {
		tasks   int
		pollers int
	}{
		{tasks: 10, pollers: 10},
		{tasks: 10, pollers: 1000},
		{tasks: 1000, pollers: 10},
		{tasks: 1000, pollers: 1000},
	} {
		b.Run(fmt.Sprintf("NoCompatibleQueryOnly/tasks=%d/pollers=%d", size.tasks, size.pollers), func(b *testing.B) {
			data := matcherDataWithNoCompatibleQueryOnlyPollers(size.tasks, size.pollers)
			newOperation := func() matcherDataBenchmarkOperation {
				return matcherDataFindAndWakeMatchesOperation(data)
			}
			lockHoldNanos := matcherDataLockHoldNanos(data, 100, newOperation)
			matchLatencyP99Nanos := matcherDataMatchLatencyP99Nanos(data, 1000, newOperation)
			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					data.lock.Lock()
					data.findAndWakeMatches()
					data.lock.Unlock()
				}
			})
			b.ReportMetric(lockHoldNanos, "lock_hold_ns/op")
			b.ReportMetric(matchLatencyP99Nanos, "match_latency_p99_ns")
		})
	}
}
