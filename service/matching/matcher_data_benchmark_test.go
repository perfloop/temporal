package matching

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/api/matchingservice/v1"
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
	for _, tc := range []struct {
		name   string
		task   *internalTask
		poller *waitingPoller
	}{
		{
			name:   "query after normal task",
			task:   &internalTask{query: &queryTaskInfo{}, effectivePriority: 1},
			poller: &waitingPoller{queryOnly: true},
		},
		{
			name:   "priority poll forwarder after normal task",
			task:   newPollForwarderTask(pollForwarderPriority, priorityBacklogPollForwarder),
			poller: &waitingPoller{queryOnly: true, forwardCtx: context.Background()},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := &matcherData{}
			data.tasks.Add(&internalTask{})
			data.tasks.Add(&internalTask{})
			data.tasks.Add(tc.task)
			data.pollers.Add(tc.poller)

			if task, poller := data.findMatch(false); task != tc.task || poller != tc.poller {
				t.Fatalf("findMatch() = (%v, %v), want compatible task matched to query-only poller", task, poller)
			}
		})
	}

	t.Run("normal tasks remain unmatched", func(t *testing.T) {
		data := &matcherData{}
		data.tasks.Add(&internalTask{})
		data.tasks.Add(&internalTask{})
		data.pollers.Add(&waitingPoller{queryOnly: true})

		if task, poller := data.findMatch(false); task != nil || poller != nil {
			t.Fatalf("findMatch() = (%v, %v), want no match", task, poller)
		}
	})
}

func TestMatcherDataFindMatchMixedPollers(t *testing.T) {
	data := &matcherData{}
	rejectedTask := &internalTask{effectivePriority: effectivePriorityFactor * priorityKey(2)}
	matchedTask := &internalTask{effectivePriority: effectivePriorityFactor * priorityKey(1)}
	queryOnlyPoller := &waitingPoller{queryOnly: true}
	normalPoller := &waitingPoller{
		pollMetadata: &pollMetadata{
			conditions: &matchingservice.PollConditions{MinPriority: 1},
		},
	}
	data.tasks.Add(rejectedTask)
	data.tasks.Add(matchedTask)
	data.pollers.Add(queryOnlyPoller)
	data.pollers.Add(normalPoller)

	task, poller := data.findMatch(false)
	if task != matchedTask || poller != normalPoller {
		t.Fatalf("findMatch() = (%v, %v), want later normal task matched to normal poller", task, poller)
	}
}

type matcherDataBenchmarkOperation struct {
	run       func()
	keepAlive func()
}

func matcherDataFindAndWakeMatchesOperation(data *matcherData) matcherDataBenchmarkOperation {
	var rateLimited bool
	return matcherDataBenchmarkOperation{
		run: func() {
			rateLimited = data.findAndWakeMatches() || rateLimited
		},
		keepAlive: func() {
			runtime.KeepAlive(rateLimited)
		},
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
		})
	}
}
