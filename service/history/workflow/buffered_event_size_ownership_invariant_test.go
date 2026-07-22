package workflow

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/headers"
	"google.golang.org/protobuf/proto"
)

func TestBufferedEventSizeOwnershipInvariant(t *testing.T) {
	limits := &bufferedSizeLimits{maxEvents: 1000, maxBytes: math.MaxInt}
	env := newBufferedSizeTestEnvironment(t, limits)
	ctx := headers.SetPrincipal(context.Background(), &commonpb.Principal{Type: "user", Name: "ownership-invariant"})
	mutableState := bufferedEventBatchMutableState(t, env, 1, 256)
	record := mutableState.CloneToProto()
	reloaded := env.newMutableStateFromDB(t, record)

	// NewMutableStateFromDB takes ownership of the buffered-event slice, so discarding
	// the source record's slice cannot detach the reloaded state's retained prefix.
	record.BufferedEvents = nil
	assertBufferedEventSizeDecisionMatchesOutput(t, reloaded, limits)

	_, err := reloaded.StartTransaction(env.namespace)
	require.NoError(t, err)

	externalRecord := reloaded.CloneToProto()
	mutateBufferedEventPayload(t, externalRecord.BufferedEvents[0], "clone-output")
	assertBufferedEventSizeDecisionMatchesOutput(t, reloaded, limits)

	var exposedEventCaptured bool
	require.True(t, reloaded.HasAnyBufferedEvent(func(event *historypb.HistoryEvent) bool {
		mutateBufferedEventPayload(t, event, "filter")
		exposedEventCaptured = true
		return true
	}))
	require.True(t, exposedEventCaptured)
	assertBufferedEventSizeDecisionMatchesOutput(t, reloaded, limits)

	addBufferedSizeSignal(t, reloaded, newBufferedSizeSignal(99, 128))
	closeBufferedSizeMutation(t, reloaded, ctx)
	assertBufferedEventSizeDecisionMatchesOutput(t, reloaded, limits)
}

func mutateBufferedEventPayload(t testing.TB, event *historypb.HistoryEvent, suffix string) {
	t.Helper()

	payloads := event.GetWorkflowExecutionSignaledEventAttributes().GetInput().GetPayloads()
	require.NotEmpty(t, payloads)
	payloads[0].Data = append(payloads[0].Data, []byte(suffix)...)
}

func assertBufferedEventSizeDecisionMatchesOutput(
	t *testing.T,
	mutableState *MutableStateImpl,
	limits *bufferedSizeLimits,
) {
	t.Helper()

	record := mutableState.CloneToProto()
	wantSize := 0
	for _, event := range record.BufferedEvents {
		wantSize += proto.Size(event)
	}
	assertBufferedSize(t, mutableState, wantSize)

	limits.maxBytes = wantSize
	require.True(t, mutableState.BufferSizeAcceptable())
	limits.maxBytes = wantSize - 1
	require.False(t, mutableState.BufferSizeAcceptable())
	limits.maxBytes = math.MaxInt
}
