package matching

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func matcherDataWithQueryOnlyPollers(pollers int) *matcherData {
	data := &matcherData{}
	for range pollers {
		data.pollers.Add(&waitingPoller{queryOnly: true})
	}
	return data
}

func matcherDataWithQueryOnlyPrefix(pollers int) (*matcherData, *waitingPoller) {
	data := &matcherData{}
	for i := range pollers - 1 {
		data.pollers.Add(&waitingPoller{queryOnly: true, startTime: time.Unix(int64(i), 0)})
	}
	normalPoller := &waitingPoller{startTime: time.Unix(int64(pollers), 0)}
	data.pollers.Add(normalPoller)
	return data, normalPoller
}

func BenchmarkMatcherDataFindMatchFastPaths(b *testing.B) {
	for _, pollers := range []int{10, 1000} {
		b.Run(fmt.Sprintf("QueryAtHead/pollers=%d", pollers), func(b *testing.B) {
			data := matcherDataWithQueryOnlyPollers(pollers)
			data.tasks.Add(&internalTask{query: &queryTaskInfo{}})
			benchmarkMatcherDataFindMatch(b, data, true)
		})

		b.Run(fmt.Sprintf("PollForwarderAtHead/pollers=%d", pollers), func(b *testing.B) {
			data := matcherDataWithQueryOnlyPollers(pollers)
			for _, poller := range data.pollers.heap {
				poller.forwardCtx = context.Background()
				poller.pollMetadata = &pollMetadata{}
			}
			data.tasks.Add(newPollForwarderTask(pollForwarderPriority, priorityBacklogPollForwarder))
			benchmarkMatcherDataFindMatch(b, data, true)
		})

		b.Run(fmt.Sprintf("NormalAfterQueryOnlyPrefix/pollers=%d", pollers), func(b *testing.B) {
			data, normalPoller := matcherDataWithQueryOnlyPrefix(pollers)
			if data.pollers.heap[len(data.pollers.heap)-1] != normalPoller {
				b.Fatal("normal poller must follow the query-only heap prefix")
			}
			data.tasks.Add(&internalTask{})
			benchmarkMatcherDataFindMatch(b, data, true)
		})
	}
}
