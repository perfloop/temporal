package workflow

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/events"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func BenchmarkBufferedEventBatchReload1x256B(b *testing.B) {
	benchmarkBufferedEventBatchReload(b, 1, 256)
}

func BenchmarkBufferedEventBatchReload25x1KiB(b *testing.B) {
	benchmarkBufferedEventBatchReload(b, 25, 1024)
}

func BenchmarkBufferedEventBatchReload50x4KiB(b *testing.B) {
	benchmarkBufferedEventBatchReload(b, 50, 4096)
}

func BenchmarkBufferedEventBatchCloneOutput1x256B(b *testing.B) {
	benchmarkBufferedEventBatchCloneOutput(b, 1, 256)
}

func BenchmarkBufferedEventBatchCloneOutput25x1KiB(b *testing.B) {
	benchmarkBufferedEventBatchCloneOutput(b, 25, 1024)
}

func BenchmarkBufferedEventBatchCloneOutput50x4KiB(b *testing.B) {
	benchmarkBufferedEventBatchCloneOutput(b, 50, 4096)
}

func benchmarkBufferedEventBatchReload(b *testing.B, count int, payloadSize int) {
	limits := &bufferedSizeLimits{maxEvents: 1000, maxBytes: math.MaxInt}
	env := newBufferedSizeTestEnvironment(b, limits)
	encoded := bufferedEventBatchSerializedRecord(b, env, count, payloadSize)
	eventsCache := events.NewMockCache(env.controller)
	eventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	var result *MutableStateImpl
	for b.Loop() {
		b.StopTimer()
		record := &persistencespb.WorkflowMutableState{}
		require.NoError(b, proto.Unmarshal(encoded, record))
		b.StartTimer()

		mutableState, err := NewMutableStateFromDB(
			env.shard,
			eventsCache,
			log.NewNoopLogger(),
			env.namespace,
			record,
			1,
		)
		if err != nil {
			b.Fatal(err)
		}
		result = mutableState
	}
	if got := result.hBuilder.NumBufferedEvents(); got != count {
		b.Fatalf("reloaded buffered event count: got %d, want %d", got, count)
	}
}

func benchmarkBufferedEventBatchCloneOutput(b *testing.B, count int, payloadSize int) {
	limits := &bufferedSizeLimits{maxEvents: 1000, maxBytes: math.MaxInt}
	env := newBufferedSizeTestEnvironment(b, limits)
	mutableState := bufferedEventBatchMutableState(b, env, count, payloadSize)

	var result *persistencespb.WorkflowMutableState
	for b.Loop() {
		result = mutableState.CloneToProto()
	}
	if got := len(result.BufferedEvents); got != count {
		b.Fatalf("cloned buffered event count: got %d, want %d", got, count)
	}
}

func bufferedEventBatchSerializedRecord(
	tb testing.TB,
	env *bufferedSizeTestEnvironment,
	count int,
	payloadSize int,
) []byte {
	tb.Helper()

	encoded, err := proto.Marshal(bufferedEventBatchMutableState(tb, env, count, payloadSize).CloneToProto())
	require.NoError(tb, err)
	return encoded
}

func bufferedEventBatchMutableState(
	tb testing.TB,
	env *bufferedSizeTestEnvironment,
	count int,
	payloadSize int,
) *MutableStateImpl {
	tb.Helper()

	ctx := headers.SetPrincipal(context.Background(), &commonpb.Principal{Type: "user", Name: "destination-benchmark"})
	mutableState := env.newMutableState(tb)
	startBufferedSizeWorkflow(tb, mutableState, env.namespace)
	for index := range count {
		addBufferedSizeSignal(tb, mutableState, newBufferedSizeSignal(index, payloadSize))
		mutation := closeBufferedSizeMutation(tb, mutableState, ctx)
		if len(mutation.NewBufferedEvents) != 1 {
			tb.Fatalf("signal %d did not produce one buffered event", index)
		}
		if index+1 < count {
			_, err := mutableState.StartTransaction(env.namespace)
			require.NoError(tb, err)
		}
	}
	return mutableState
}
