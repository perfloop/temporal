package workflow_test

import (
	"context"
	"math"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/service/history/workflow"
)

type bufferedEventCadenceShape struct {
	name              string
	initialSignals    int
	additionalCommits int
	payloadBytes      int
}

type bufferedEventCadence struct {
	shape    bufferedEventCadenceShape
	state    *workflow.MutableStateImpl
	payloads []*commonpb.Payloads
}

// BenchmarkBufferSizeAcceptableActiveSignalCadence runs one active signal
// cadence at each buffered-event count and payload shape in one benchmark
// operation. State setup is outside the timer; every buffered insertion,
// transaction close, and cache-reuse boundary is timed.
func BenchmarkBufferSizeAcceptableActiveSignalCadence(b *testing.B) {
	shapes := []bufferedEventCadenceShape{
		{name: "1x256B", initialSignals: 1, additionalCommits: 2, payloadBytes: 256},
		{name: "25x1KiB", initialSignals: 20, additionalCommits: 5, payloadBytes: 1024},
		{name: "50x4KiB", initialSignals: 45, additionalCommits: 5, payloadBytes: 4 * 1024},
		{name: "100x4KiB", initialSignals: 95, additionalCommits: 5, payloadBytes: 4 * 1024},
	}
	h := newBufferedEventSizeHarness(b, 100, math.MaxInt)
	ctx := headers.SetPrincipal(context.Background(), &commonpb.Principal{Type: "user", Name: "benchmark-principal"})

	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		cadences := make([]bufferedEventCadence, 0, len(shapes))
		for shapeIndex, shape := range shapes {
			state, _ := h.newActiveState(b)
			payloads := make([]*commonpb.Payloads, shape.initialSignals+shape.additionalCommits)
			for i := range payloads {
				payloads[i] = bufferedSignalPayload(shape.payloadBytes, byte(shapeIndex+i+1))
			}
			cadences = append(cadences, bufferedEventCadence{shape: shape, state: state, payloads: payloads})
		}
		b.StartTimer()

		for _, cadence := range cadences {
			runBufferedEventCadence(b, h, ctx, cadence)
		}

		b.StopTimer()
		for _, cadence := range cadences {
			if got := len(cadence.state.CloneToProto().GetBufferedEvents()); got != cadence.shape.initialSignals+cadence.shape.additionalCommits {
				b.Fatalf("%s retained %d buffered events, want %d", cadence.shape.name, got, cadence.shape.initialSignals+cadence.shape.additionalCommits)
			}
		}
	}
}

func runBufferedEventCadence(
	b *testing.B,
	h *bufferedEventSizeHarness,
	ctx context.Context,
	cadence bufferedEventCadence,
) {
	b.Helper()

	for i := 0; i < cadence.shape.initialSignals; i++ {
		addBufferedSizeSignal(b, cadence.state, cadence.payloads[i])
	}
	mutation := closeBufferedSizeTransaction(b, cadence.state, ctx)
	if got := len(mutation.NewBufferedEvents); got != cadence.shape.initialSignals {
		b.Fatalf("%s first commit produced %d buffered events, want %d", cadence.shape.name, got, cadence.shape.initialSignals)
	}

	for i := 0; i < cadence.shape.additionalCommits; i++ {
		startBufferedSizeTransaction(b, h, cadence.state)
		addBufferedSizeSignal(b, cadence.state, cadence.payloads[cadence.shape.initialSignals+i])
		mutation = closeBufferedSizeTransaction(b, cadence.state, ctx)
		if got := len(mutation.NewBufferedEvents); got != 1 {
			b.Fatalf("%s incremental commit produced %d buffered events, want one", cadence.shape.name, got)
		}
	}
}
