package workflow

import (
	"errors"
	"testing"

	commandpb "go.temporal.io/api/command/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/rpc"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

const (
	taskCompletionGapEmptyCommandCount = 8192
	taskCompletionGapBufferLimit       = 40 * 1024 * 1024
)

func TestTaskCompletionPaginationGapRejectsLargeEmptyPage(t *testing.T) {
	workflowContext, buffer, finalRequest := newTaskCompletionPaginationGapInput(t)
	workflowContext.taskCompletionBuffer = buffer

	_, err := workflowContext.GetMergedTaskCompletionPages(
		benchmarkTaskCompletionSchedID,
		benchmarkTaskCompletionAttempt,
		finalRequest,
	)
	var bufferLost *serviceerror.WorkflowTaskCompletionBufferLost
	if !errors.As(err, &bufferLost) {
		t.Fatalf("GetMergedTaskCompletionPages error = %v, want WorkflowTaskCompletionBufferLost", err)
	}
}

func BenchmarkTaskCompletionPaginationGap(b *testing.B) {
	gapContext, gapBuffer, gapFinalRequest := newTaskCompletionPaginationGapInput(b)
	validCase := taskCompletionPaginationBenchmarkCase{
		pageCount:         4,
		commandsPerPage:   16,
		finalCommandCount: 16,
	}
	validContext, validBuffer, validFinalRequest, expectedNames := newTaskCompletionPaginationBenchmarkInput(validCase)

	for b.Loop() {
		// GetMergedTaskCompletionPages clears this pointer on every rejection. The
		// buffered page itself is immutable after its real admission below.
		gapContext.taskCompletionBuffer = gapBuffer
		_, err := gapContext.GetMergedTaskCompletionPages(
			benchmarkTaskCompletionSchedID,
			benchmarkTaskCompletionAttempt,
			gapFinalRequest,
		)
		if err == nil {
			b.Fatal("GetMergedTaskCompletionPages accepted a page sequence with page 0 missing")
		}

		// Pair the malformed rejection with the production merge plus its caller's
		// final-page append. A reservation before gap validation makes this combined
		// allocation guard regress while this append consumes the optimized result.
		validContext.taskCompletionBuffer = validBuffer
		merged, err := validContext.GetMergedTaskCompletionPages(
			benchmarkTaskCompletionSchedID,
			benchmarkTaskCompletionAttempt,
			validFinalRequest,
		)
		if err != nil {
			b.Fatalf("GetMergedTaskCompletionPages returned error: %v", err)
		}
		commands := append(merged, validFinalRequest.Commands...)
		if len(commands) != len(expectedNames) ||
			commands[0].GetRecordMarkerCommandAttributes().GetMarkerName() != expectedNames[0] ||
			commands[len(commands)-1].GetRecordMarkerCommandAttributes().GetMarkerName() != expectedNames[len(expectedNames)-1] {
			b.Fatal("merged commands did not preserve the buffered-to-final order")
		}
	}
}

func newTaskCompletionPaginationGapInput(t testing.TB) (
	*ContextImpl,
	*TaskCompletionBuffer,
	*workflowservice.RespondWorkflowTaskCompletedRequest,
) {
	t.Helper()

	config := tests.NewDynamicConfig()
	config.WorkflowTaskCompletionBufferSizeLimit = func(string) int { return taskCompletionGapBufferLimit }
	workflowContext := NewContext(
		config,
		tests.WorkflowKey,
		chasm.WorkflowArchetypeID,
		log.NewNoopLogger(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)

	controller := gomock.NewController(t)
	mutableState := historyi.NewMockMutableState(controller)
	mutableState.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry).AnyTimes()
	mutableState.EXPECT().GetStartedWorkflowTask().Return(&historyi.WorkflowTaskInfo{}).AnyTimes()
	workflowContext.MutableState = mutableState

	emptyCommands := make([]*commandpb.Command, taskCompletionGapEmptyCommandCount)
	for i := range emptyCommands {
		emptyCommands[i] = &commandpb.Command{}
	}
	intermediateRequest := &workflowservice.RespondWorkflowTaskCompletedRequest{
		IntermediatePage: true,
		PageNumber:       1,
		Commands:         emptyCommands,
	}
	if requestBytes := proto.Size(intermediateRequest); requestBytes > rpc.MaxHTTPAPIRequestBytes {
		t.Fatalf("intermediate request size = %d, exceeds gRPC request limit %d", requestBytes, rpc.MaxHTTPAPIRequestBytes)
	}
	if err := workflowContext.AppendTaskCompletionPage(
		benchmarkTaskCompletionSchedID,
		benchmarkTaskCompletionAttempt,
		intermediateRequest,
	); err != nil {
		t.Fatalf("AppendTaskCompletionPage returned error: %v", err)
	}
	if workflowContext.taskCompletionBuffer.totalSize != 0 {
		t.Fatalf("buffered empty commands have byte size %d, want 0", workflowContext.taskCompletionBuffer.totalSize)
	}

	buffer := workflowContext.taskCompletionBuffer
	// AppendTaskCompletionPage needs MutableState, but the merge benchmark only
	// needs the accepted buffer and uses the zero workflow-task identity.
	workflowContext.MutableState = nil
	workflowContext.taskCompletionBuffer = nil
	return workflowContext, buffer, finalPage(2, nil)
}
