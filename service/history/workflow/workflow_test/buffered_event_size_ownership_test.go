package workflow_test

import (
	"context"
	"math"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/headers"
	"google.golang.org/protobuf/proto"
)

func TestBufferedEventSizeOwnershipLifecycle(t *testing.T) {
	h := newBufferedEventSizeHarness(t, 100, math.MaxInt)
	mutableState, _ := h.newActiveState(t)

	firstInput := bufferedSignalPayload(1024, 3)
	firstExpectedInput := proto.Clone(firstInput).(*commonpb.Payloads)
	first := addBufferedSizeSignal(t, mutableState, firstInput)
	firstSize := proto.Size(first)
	assertBufferedSizeLimit(t, h, mutableState, firstSize)

	// The event must retain the input it accepted, not a caller-owned pointer
	// that can grow across the configured byte limit before the commit.
	firstInput.Payloads[0].Data = make([]byte, 2*firstSize)
	if got := first.GetWorkflowExecutionSignaledEventAttributes().GetInput(); !proto.Equal(got, firstExpectedInput) {
		t.Fatal("caller mutation before Finish changed the retained buffered signal input")
	}
	assertBufferedSizeLimit(t, h, mutableState, firstSize)

	const firstPrincipalName = "first-buffered-size-principal"
	firstPrincipal := &commonpb.Principal{Type: "user", Name: firstPrincipalName}
	firstMutation := closeBufferedSizeTransaction(t, mutableState, headers.SetPrincipal(context.Background(), firstPrincipal))
	if len(firstMutation.NewBufferedEvents) != 1 || firstMutation.NewBufferedEvents[0].GetPrincipal().GetName() != firstPrincipalName {
		t.Fatal("first active signal commit did not retain its principal-stamped buffered event")
	}
	firstPrincipal.Name = "caller-mutated-principal"

	sourceRecord := mutableState.CloneToProto()
	firstCommittedSize := bufferedEventsSize(sourceRecord.GetBufferedEvents())
	loaded := h.loadActiveState(t, sourceRecord)

	// Mutate both ownership boundaries after the transaction restart: the
	// original caller input and the persisted source record. Neither may alter
	// the reloaded cache or its byte-limit decision.
	firstInput.Payloads[0].Data = make([]byte, 3*firstSize)
	sourcePayload := sourceRecord.GetBufferedEvents()[0].GetWorkflowExecutionSignaledEventAttributes().GetInput().GetPayloads()[0]
	sourcePayload.Data = append(sourcePayload.Data, 4)

	postReload := loaded.CloneToProto()
	if got := len(postReload.GetBufferedEvents()); got != 1 {
		t.Fatalf("reload should retain one buffered signal after source mutation, got %d", got)
	}
	firstOutput := postReload.GetBufferedEvents()[0]
	if got := firstOutput.GetWorkflowExecutionSignaledEventAttributes().GetInput(); !proto.Equal(got, firstExpectedInput) {
		t.Fatal("caller or persisted-record mutation changed the reloaded buffered signal input")
	}
	if got := firstOutput.GetPrincipal().GetName(); got != firstPrincipalName {
		t.Fatalf("reload retained principal %q, want %q", got, firstPrincipalName)
	}
	assertBufferedSizeLimit(t, h, loaded, firstCommittedSize)

	secondInput := bufferedSignalPayload(1024, 5)
	secondExpectedInput := proto.Clone(secondInput).(*commonpb.Payloads)
	second := addBufferedSizeSignal(t, loaded, secondInput)
	secondInput.Payloads[0].Data = make([]byte, 2*proto.Size(second))
	assertBufferedSizeLimit(t, h, loaded, firstCommittedSize+proto.Size(second))

	secondPrincipal := &commonpb.Principal{Type: "user", Name: "second-buffered-size-principal"}
	secondMutation := closeBufferedSizeTransaction(t, loaded, headers.SetPrincipal(context.Background(), secondPrincipal))
	if len(secondMutation.NewBufferedEvents) != 1 || secondMutation.NewBufferedEvents[0].GetPrincipal().GetName() != secondPrincipal.GetName() {
		t.Fatal("second active signal commit did not retain its principal-stamped buffered event")
	}

	postCommit := loaded.CloneToProto()
	if got := len(postCommit.GetBufferedEvents()); got != 2 {
		t.Fatalf("two active commits should retain two buffered signals, got %d", got)
	}
	firstOutput = postCommit.GetBufferedEvents()[0]
	if got := firstOutput.GetWorkflowExecutionSignaledEventAttributes().GetInput(); !proto.Equal(got, firstExpectedInput) {
		t.Fatal("second commit changed the first buffered signal payload")
	}
	if got := firstOutput.GetPrincipal().GetName(); got != firstPrincipalName {
		t.Fatalf("second commit changed first signal principal to %q, want %q", got, firstPrincipalName)
	}
	secondOutput := postCommit.GetBufferedEvents()[1]
	if got := secondOutput.GetWorkflowExecutionSignaledEventAttributes().GetInput(); !proto.Equal(got, secondExpectedInput) {
		t.Fatal("second caller mutation changed the retained tail signal payload")
	}
	if got := secondOutput.GetPrincipal().GetName(); got != secondPrincipal.GetName() {
		t.Fatalf("tail signal principal %q, want %q", got, secondPrincipal.GetName())
	}
	assertBufferedSizeLimit(t, h, loaded, bufferedEventsSize(postCommit.GetBufferedEvents()))
}
