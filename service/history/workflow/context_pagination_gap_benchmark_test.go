package workflow

import (
	"testing"

	commandpb "go.temporal.io/api/command/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/rpc"
	"google.golang.org/protobuf/proto"
)

const taskCompletionGapEmptyCommandCount = 8192

func BenchmarkTaskCompletionPaginationGap(b *testing.B) {
	workflowContext, buffer, finalRequest := newTaskCompletionPaginationGapInput(b)

	for b.Loop() {
		// GetMergedTaskCompletionPages clears this pointer on every rejection. The
		// buffered page itself is immutable after its real admission below.
		workflowContext.taskCompletionBuffer = buffer
		_, err := workflowContext.GetMergedTaskCompletionPages(
			benchmarkTaskCompletionSchedID,
			benchmarkTaskCompletionAttempt,
			finalRequest,
		)
		if err == nil {
			b.Fatal("GetMergedTaskCompletionPages accepted a page sequence with page 0 missing")
		}
	}
}

func newTaskCompletionPaginationGapInput(t testing.TB) (
	*ContextImpl,
	*TaskCompletionBuffer,
	*workflowservice.RespondWorkflowTaskCompletedRequest,
) {
	t.Helper()

	workflowContext := newTaskCompletionPaginationBenchmarkContext(t)

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
