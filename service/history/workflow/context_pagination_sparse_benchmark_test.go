package workflow

import (
	"strconv"
	"testing"

	commandpb "go.temporal.io/api/command/v1"
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
	taskCompletionSparsePageCount             = 256
	taskCompletionOutOfRangeEmptyCommandCount = 8192
	taskCompletionBenchmarkBufferLimit        = 40 * 1024 * 1024
)

func BenchmarkTaskCompletionPaginationSparseFinalPage(b *testing.B) {
	workflowContext, buffer, finalRequest := newTaskCompletionPaginationSparseInput(b)

	for b.Loop() {
		workflowContext.taskCompletionBuffer = buffer
		merged, err := workflowContext.GetMergedTaskCompletionPages(
			benchmarkTaskCompletionSchedID,
			benchmarkTaskCompletionAttempt,
			finalRequest,
		)
		if err != nil {
			b.Fatalf("GetMergedTaskCompletionPages returned error: %v", err)
		}
		commands := append(merged, finalRequest.Commands...)
		if len(commands) != taskCompletionSparsePageCount ||
			commands[0].GetRecordMarkerCommandAttributes().GetMarkerName() != "sparse-0" ||
			commands[len(commands)-1].GetRecordMarkerCommandAttributes().GetMarkerName() != "sparse-255" {
			b.Fatal("merged commands did not preserve the buffered-to-final order")
		}
	}
}

func BenchmarkTaskCompletionPaginationOutOfRangePage(b *testing.B) {
	workflowContext, buffer, finalRequest := newTaskCompletionPaginationOutOfRangeInput(b)

	for b.Loop() {
		workflowContext.taskCompletionBuffer = buffer
		merged, err := workflowContext.GetMergedTaskCompletionPages(
			benchmarkTaskCompletionSchedID,
			benchmarkTaskCompletionAttempt,
			finalRequest,
		)
		if err != nil {
			b.Fatalf("GetMergedTaskCompletionPages returned error: %v", err)
		}
		commands := append(merged, finalRequest.Commands...)
		if len(commands) != len(finalRequest.Commands)+1 ||
			commands[0].GetRecordMarkerCommandAttributes().GetMarkerName() != "in-range" ||
			commands[len(commands)-1].GetRecordMarkerCommandAttributes().GetMarkerName() != "final-15" {
			b.Fatal("merged commands included an out-of-range buffered page")
		}
	}
}

func newTaskCompletionPaginationSparseInput(t testing.TB) (
	*ContextImpl,
	*TaskCompletionBuffer,
	*workflowservice.RespondWorkflowTaskCompletedRequest,
) {
	t.Helper()
	workflowContext := newTaskCompletionPaginationBenchmarkContext(t)
	for page := range taskCompletionSparsePageCount {
		request := &workflowservice.RespondWorkflowTaskCompletedRequest{
			IntermediatePage: true,
			PageNumber:       int32(page),
			Commands:         []*commandpb.Command{benchmarkTaskCompletionCommand("sparse-" + strconv.Itoa(page))},
		}
		if err := workflowContext.AppendTaskCompletionPage(
			benchmarkTaskCompletionSchedID,
			benchmarkTaskCompletionAttempt,
			request,
		); err != nil {
			t.Fatalf("AppendTaskCompletionPage returned error: %v", err)
		}
	}

	buffer := workflowContext.taskCompletionBuffer
	workflowContext.MutableState = nil
	workflowContext.taskCompletionBuffer = nil
	return workflowContext, buffer, &workflowservice.RespondWorkflowTaskCompletedRequest{
		PageNumber: taskCompletionSparsePageCount,
	}
}

func newTaskCompletionPaginationOutOfRangeInput(t testing.TB) (
	*ContextImpl,
	*TaskCompletionBuffer,
	*workflowservice.RespondWorkflowTaskCompletedRequest,
) {
	t.Helper()
	workflowContext := newTaskCompletionPaginationBenchmarkContext(t)

	emptyCommands := make([]*commandpb.Command, taskCompletionOutOfRangeEmptyCommandCount)
	for i := range emptyCommands {
		emptyCommands[i] = &commandpb.Command{}
	}
	outOfRangeRequest := &workflowservice.RespondWorkflowTaskCompletedRequest{
		IntermediatePage: true,
		PageNumber:       maxWorkflowTaskCompletionPages - 1,
		Commands:         emptyCommands,
	}
	if requestBytes := proto.Size(outOfRangeRequest); requestBytes > rpc.MaxHTTPAPIRequestBytes {
		t.Fatalf("out-of-range request size = %d, exceeds gRPC request limit %d", requestBytes, rpc.MaxHTTPAPIRequestBytes)
	}
	if err := workflowContext.AppendTaskCompletionPage(
		benchmarkTaskCompletionSchedID,
		benchmarkTaskCompletionAttempt,
		outOfRangeRequest,
	); err != nil {
		t.Fatalf("AppendTaskCompletionPage returned error: %v", err)
	}
	if err := workflowContext.AppendTaskCompletionPage(
		benchmarkTaskCompletionSchedID,
		benchmarkTaskCompletionAttempt,
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			IntermediatePage: true,
			PageNumber:       0,
			Commands:         []*commandpb.Command{benchmarkTaskCompletionCommand("in-range")},
		},
	); err != nil {
		t.Fatalf("AppendTaskCompletionPage returned error: %v", err)
	}

	finalCommands := make([]*commandpb.Command, 16)
	for i := range finalCommands {
		finalCommands[i] = benchmarkTaskCompletionCommand("final-" + strconv.Itoa(i))
	}
	buffer := workflowContext.taskCompletionBuffer
	workflowContext.MutableState = nil
	workflowContext.taskCompletionBuffer = nil
	return workflowContext, buffer, &workflowservice.RespondWorkflowTaskCompletedRequest{
		PageNumber: 1,
		Commands:   finalCommands,
	}
}

func newTaskCompletionPaginationBenchmarkContext(t testing.TB) *ContextImpl {
	t.Helper()

	config := tests.NewDynamicConfig()
	config.WorkflowTaskCompletionBufferSizeLimit = func(string) int { return taskCompletionBenchmarkBufferLimit }
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
	return workflowContext
}
