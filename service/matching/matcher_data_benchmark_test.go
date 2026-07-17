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
	data := matcherDataWithNoCompatibleQueryOnlyPollers(3, 3)

	if task, poller := data.findMatch(false); task != nil || poller != nil {
		t.Fatalf("findMatch() = (%v, %v), want no match", task, poller)
	}

	queryTask := &internalTask{query: &queryTaskInfo{}}
	data.tasks.Add(queryTask)
	if task, poller := data.findMatch(false); task != queryTask || poller == nil || !poller.queryOnly {
		t.Fatalf("findMatch() = (%v, %v), want query task matched to a query-only poller", task, poller)
	}

	data.tasks.Remove(queryTask)
	data.pollers.Remove(data.pollers.heap[0])
	normalPoller := &waitingPoller{}
	data.pollers.Add(normalPoller)
	if got, want := data.pollers.queryOnlyCount, len(data.pollers.heap)-1; got != want {
		t.Fatalf("queryOnlyCount = %d, want %d", got, want)
	}
	if task, poller := data.findMatch(false); task == nil || task.isQuery() || poller != normalPoller {
		t.Fatalf("findMatch() = (%v, %v), want normal task matched to normal poller", task, poller)
	}
}

// matcherDataLockHoldNanos measures the time findAndWakeMatches holds matcherData.lock.
func matcherDataLockHoldNanos(data *matcherData, samples int) float64 {
	var total time.Duration
	for range samples {
		data.lock.Lock()
		start := time.Now()
		data.findAndWakeMatches()
		total += time.Since(start)
		data.lock.Unlock()
	}
	return float64(total) / float64(samples)
}

// matcherDataMatchLatencyP99Nanos measures a caller's lock-wait plus matching latency.
func matcherDataMatchLatencyP99Nanos(data *matcherData, samplesPerWorker int) float64 {
	workers := runtime.GOMAXPROCS(0)
	latencies := make([]time.Duration, workers*samplesPerWorker)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := range samplesPerWorker {
				start := time.Now()
				data.lock.Lock()
				data.findAndWakeMatches()
				data.lock.Unlock()
				latencies[worker*samplesPerWorker+i] = time.Since(start)
			}
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
			lockHoldNanos := matcherDataLockHoldNanos(data, 100)
			matchLatencyP99Nanos := matcherDataMatchLatencyP99Nanos(data, 1000)
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
