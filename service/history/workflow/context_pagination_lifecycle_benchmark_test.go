package workflow

import (
	"strconv"
	"testing"

	commandpb "go.temporal.io/api/command/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/rpc"
	"google.golang.org/protobuf/proto"
)

type taskCompletionPaginationLifecycleBenchmarkCase struct {
	name              string
	pageCount         int
	commandsPerPage   int
	finalCommandCount int
	pageZeroLast      bool
}

func TestTaskCompletionPaginationLifecycleOutOfOrderPages(t *testing.T) {
	workflowContext := newTaskCompletionPaginationBenchmarkContext(t)

	for _, request := range []*workflowservice.RespondWorkflowTaskCompletedRequest{
		{
			IntermediatePage: true,
			PageNumber:       1,
			Commands:         []*commandpb.Command{benchmarkTaskCompletionCommand("second")},
		},
		{
			IntermediatePage: true,
			PageNumber:       0,
			Commands:         []*commandpb.Command{benchmarkTaskCompletionCommand("first")},
		},
	} {
		if err := workflowContext.AppendTaskCompletionPage(
			benchmarkTaskCompletionSchedID,
			benchmarkTaskCompletionAttempt,
			request,
		); err != nil {
			t.Fatalf("AppendTaskCompletionPage returned error: %v", err)
		}
	}

	merged, err := workflowContext.GetMergedTaskCompletionPages(
		benchmarkTaskCompletionSchedID,
		benchmarkTaskCompletionAttempt,
		&workflowservice.RespondWorkflowTaskCompletedRequest{PageNumber: 2},
	)
	if err != nil {
		t.Fatalf("GetMergedTaskCompletionPages returned error: %v", err)
	}
	if len(merged) != 2 ||
		merged[0].GetRecordMarkerCommandAttributes().GetMarkerName() != "first" ||
		merged[1].GetRecordMarkerCommandAttributes().GetMarkerName() != "second" {
		t.Fatal("merged commands did not preserve out-of-order page admission order")
	}
}

func TestTaskCompletionPaginationLifecycleLowerFinalPage(t *testing.T) {
	workflowContext := newTaskCompletionPaginationBenchmarkContext(t)

	for page, markerName := range []string{"first", "second"} {
		if err := workflowContext.AppendTaskCompletionPage(
			benchmarkTaskCompletionSchedID,
			benchmarkTaskCompletionAttempt,
			&workflowservice.RespondWorkflowTaskCompletedRequest{
				IntermediatePage: true,
				PageNumber:       int32(page),
				Commands:         []*commandpb.Command{benchmarkTaskCompletionCommand(markerName)},
			},
		); err != nil {
			t.Fatalf("AppendTaskCompletionPage returned error: %v", err)
		}
	}

	merged, err := workflowContext.GetMergedTaskCompletionPages(
		benchmarkTaskCompletionSchedID,
		benchmarkTaskCompletionAttempt,
		&workflowservice.RespondWorkflowTaskCompletedRequest{PageNumber: 1},
	)
	if err != nil {
		t.Fatalf("GetMergedTaskCompletionPages returned error: %v", err)
	}
	if len(merged) != 1 || merged[0].GetRecordMarkerCommandAttributes().GetMarkerName() != "first" {
		t.Fatal("lower final page did not merge only its requested prefix")
	}
}

func BenchmarkTaskCompletionPaginationLifecycle(b *testing.B) {
	for _, testCase := range []taskCompletionPaginationLifecycleBenchmarkCase{
		{
			name:              "ordered_four_pages_multi_command",
			pageCount:         4,
			commandsPerPage:   16,
			finalCommandCount: 16,
		},
		{
			name:            "ordered_sparse_256_pages",
			pageCount:       taskCompletionSparsePageCount,
			commandsPerPage: 1,
		},
		{
			name:            "sparse_256_pages_page_zero_last",
			pageCount:       taskCompletionSparsePageCount,
			commandsPerPage: 1,
			pageZeroLast:    true,
		},
		{
			name:            "sparse_1023_pages_page_zero_last",
			pageCount:       int(maxWorkflowTaskCompletionPages) - 1,
			commandsPerPage: 1,
			pageZeroLast:    true,
		},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			workflowContext, intermediatePages, finalPage, expectedCommandCount := newTaskCompletionPaginationLifecycleInput(b, testCase)

			for b.Loop() {
				for _, intermediatePage := range intermediatePages {
					if err := workflowContext.AppendTaskCompletionPage(
						benchmarkTaskCompletionSchedID,
						benchmarkTaskCompletionAttempt,
						intermediatePage,
					); err != nil {
						b.Fatalf("AppendTaskCompletionPage returned error: %v", err)
					}
				}
				merged, err := workflowContext.GetMergedTaskCompletionPages(
					benchmarkTaskCompletionSchedID,
					benchmarkTaskCompletionAttempt,
					finalPage,
				)
				if err != nil {
					b.Fatalf("GetMergedTaskCompletionPages returned error: %v", err)
				}
				commands := append(merged, finalPage.Commands...)
				if len(commands) != expectedCommandCount {
					b.Fatalf("merged command count = %d, want %d", len(commands), expectedCommandCount)
				}
			}
		})
	}
}

func BenchmarkTaskCompletionPaginationOutOfRangeNearRequestLimit(b *testing.B) {
	workflowContext := newTaskCompletionPaginationBenchmarkContext(b)
	emptyCommands := make([]*commandpb.Command, rpc.MaxHTTPAPIRequestBytes/2-1024)
	emptyCommand := &commandpb.Command{}
	for i := range emptyCommands {
		emptyCommands[i] = emptyCommand
	}
	outOfRangePage := &workflowservice.RespondWorkflowTaskCompletedRequest{
		IntermediatePage: true,
		PageNumber:       maxWorkflowTaskCompletionPages - 1,
		Commands:         emptyCommands,
	}
	if requestBytes := proto.Size(outOfRangePage); requestBytes > rpc.MaxHTTPAPIRequestBytes || requestBytes < rpc.MaxHTTPAPIRequestBytes-4096 {
		b.Fatalf("out-of-range request size = %d, want within 4096 bytes of request limit %d", requestBytes, rpc.MaxHTTPAPIRequestBytes)
	}
	if err := workflowContext.AppendTaskCompletionPage(
		benchmarkTaskCompletionSchedID,
		benchmarkTaskCompletionAttempt,
		outOfRangePage,
	); err != nil {
		b.Fatalf("AppendTaskCompletionPage returned error: %v", err)
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
		b.Fatalf("AppendTaskCompletionPage returned error: %v", err)
	}
	buffer := workflowContext.taskCompletionBuffer
	workflowContext.MutableState = nil
	workflowContext.taskCompletionBuffer = nil
	finalPage := &workflowservice.RespondWorkflowTaskCompletedRequest{
		PageNumber: 1,
		Commands:   []*commandpb.Command{benchmarkTaskCompletionCommand("final")},
	}

	for b.Loop() {
		workflowContext.taskCompletionBuffer = buffer
		merged, err := workflowContext.GetMergedTaskCompletionPages(
			benchmarkTaskCompletionSchedID,
			benchmarkTaskCompletionAttempt,
			finalPage,
		)
		if err != nil {
			b.Fatalf("GetMergedTaskCompletionPages returned error: %v", err)
		}
		commands := append(merged, finalPage.Commands...)
		if len(commands) != 2 {
			b.Fatalf("merged command count = %d, want 2", len(commands))
		}
	}
}

func newTaskCompletionPaginationLifecycleInput(
	t testing.TB,
	testCase taskCompletionPaginationLifecycleBenchmarkCase,
) (
	*ContextImpl,
	[]*workflowservice.RespondWorkflowTaskCompletedRequest,
	*workflowservice.RespondWorkflowTaskCompletedRequest,
	int,
) {
	t.Helper()
	workflowContext := newTaskCompletionPaginationBenchmarkContext(t)
	pages := make([]*workflowservice.RespondWorkflowTaskCompletedRequest, testCase.pageCount)
	for page := range pages {
		commands := make([]*commandpb.Command, testCase.commandsPerPage)
		for command := range commands {
			commands[command] = benchmarkTaskCompletionCommand("lifecycle-" + strconv.Itoa(page*testCase.commandsPerPage+command))
		}
		pages[page] = &workflowservice.RespondWorkflowTaskCompletedRequest{
			IntermediatePage: true,
			PageNumber:       int32(page),
			Commands:         commands,
		}
	}
	if testCase.pageZeroLast {
		pages = append(pages[1:], pages[0])
	}

	finalCommands := make([]*commandpb.Command, testCase.finalCommandCount)
	for command := range finalCommands {
		finalCommands[command] = benchmarkTaskCompletionCommand("final-" + strconv.Itoa(command))
	}
	return workflowContext, pages, &workflowservice.RespondWorkflowTaskCompletedRequest{
		PageNumber: int32(testCase.pageCount),
		Commands:   finalCommands,
	}, testCase.pageCount*testCase.commandsPerPage + testCase.finalCommandCount
}
