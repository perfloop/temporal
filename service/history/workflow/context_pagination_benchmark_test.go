package workflow

import (
	"strconv"
	"testing"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/metrics"
)

type taskCompletionPaginationBenchmarkCase struct {
	name              string
	pageCount         int
	commandsPerPage   int
	finalCommandCount int
}

func TestTaskCompletionPaginationFinalPagePreservesCommandOrder(t *testing.T) {
	testCase := taskCompletionPaginationBenchmarkCase{
		pageCount:         3,
		commandsPerPage:   2,
		finalCommandCount: 2,
	}
	workflowContext, buffer, finalPage, expectedNames := newTaskCompletionPaginationBenchmarkInput(testCase)
	workflowContext.taskCompletionBuffer = buffer

	merged, err := workflowContext.GetMergedTaskCompletionPages(benchmarkTaskCompletionSchedID, benchmarkTaskCompletionAttempt, finalPage)
	if err != nil {
		t.Fatalf("GetMergedTaskCompletionPages returned error: %v", err)
	}
	commands := append(merged, finalPage.Commands...)
	if len(commands) != len(expectedNames) {
		t.Fatalf("merged command count = %d, want %d", len(commands), len(expectedNames))
	}
	for i, command := range commands {
		if got := command.GetRecordMarkerCommandAttributes().GetMarkerName(); got != expectedNames[i] {
			t.Errorf("command %d marker = %q, want %q", i, got, expectedNames[i])
		}
	}
	if workflowContext.taskCompletionBuffer != nil {
		t.Fatal("GetMergedTaskCompletionPages did not clear the page buffer")
	}
}

func BenchmarkTaskCompletionPaginationFinalPage(b *testing.B) {
	for _, testCase := range []taskCompletionPaginationBenchmarkCase{
		{
			name:              "four_pages_multi_command",
			pageCount:         4,
			commandsPerPage:   16,
			finalCommandCount: 16,
		},
		{
			name:              "eight_pages_multi_command",
			pageCount:         8,
			commandsPerPage:   16,
			finalCommandCount: 32,
		},
		{
			name:              "four_pages_single_command",
			pageCount:         4,
			commandsPerPage:   1,
			finalCommandCount: 16,
		},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			workflowContext, buffer, finalPage, expectedNames := newTaskCompletionPaginationBenchmarkInput(testCase)
			expectedCommandCount := len(expectedNames)

			// Verify the fixed setup once outside the timed loop. Each iteration below
			// reuses these immutable buffered pages, matching the final request after
			// its intermediate pages have already been accepted.
			workflowContext.taskCompletionBuffer = buffer
			merged, err := workflowContext.GetMergedTaskCompletionPages(benchmarkTaskCompletionSchedID, benchmarkTaskCompletionAttempt, finalPage)
			if err != nil {
				b.Fatalf("GetMergedTaskCompletionPages returned error: %v", err)
			}
			commands := append(merged, finalPage.Commands...)
			if len(commands) != expectedCommandCount ||
				commands[0].GetRecordMarkerCommandAttributes().GetMarkerName() != expectedNames[0] ||
				commands[len(commands)-1].GetRecordMarkerCommandAttributes().GetMarkerName() != expectedNames[len(expectedNames)-1] {
				b.Fatal("merged commands did not preserve the buffered-to-final order")
			}

			for b.Loop() {
				// GetMergedTaskCompletionPages clears only this context pointer; the
				// buffer's pages remain immutable after intermediate-page acceptance.
				workflowContext.taskCompletionBuffer = buffer
				merged, err = workflowContext.GetMergedTaskCompletionPages(benchmarkTaskCompletionSchedID, benchmarkTaskCompletionAttempt, finalPage)
				if err != nil {
					b.Fatalf("GetMergedTaskCompletionPages returned error: %v", err)
				}
				commands = append(merged, finalPage.Commands...)
				if len(commands) != expectedCommandCount ||
					commands[0].GetRecordMarkerCommandAttributes().GetMarkerName() != expectedNames[0] ||
					commands[len(commands)-1].GetRecordMarkerCommandAttributes().GetMarkerName() != expectedNames[len(expectedNames)-1] {
					b.Fatal("merged commands did not preserve the buffered-to-final order")
				}
			}
		})
	}
}

const (
	benchmarkTaskCompletionSchedID = int64(99)
	benchmarkTaskCompletionAttempt = int32(1)
)

func newTaskCompletionPaginationBenchmarkInput(testCase taskCompletionPaginationBenchmarkCase) (
	*ContextImpl,
	*TaskCompletionBuffer,
	*workflowservice.RespondWorkflowTaskCompletedRequest,
	[]string,
) {
	pages := make(map[int32][]*commandpb.Command, testCase.pageCount)
	expectedNames := make([]string, 0, testCase.pageCount*testCase.commandsPerPage+testCase.finalCommandCount)
	var totalSize int64
	commandNumber := 0
	for page := range testCase.pageCount {
		commands := make([]*commandpb.Command, testCase.commandsPerPage)
		for command := range commands {
			markerName := "buffered-" + strconv.Itoa(commandNumber)
			commands[command] = benchmarkTaskCompletionCommand(markerName)
			expectedNames = append(expectedNames, markerName)
			commandNumber++
		}
		pages[int32(page)] = commands
		totalSize += taskCompletionPageBytes(commands)
	}

	finalCommands := make([]*commandpb.Command, testCase.finalCommandCount)
	for command := range finalCommands {
		markerName := "final-" + strconv.Itoa(commandNumber)
		finalCommands[command] = benchmarkTaskCompletionCommand(markerName)
		expectedNames = append(expectedNames, markerName)
		commandNumber++
	}

	return &ContextImpl{metricsHandler: metrics.NoopMetricsHandler}, &TaskCompletionBuffer{
			pages:     pages,
			totalSize: totalSize,
			identity: workflowTaskIdentity{
				schedID: benchmarkTaskCompletionSchedID,
				attempt: benchmarkTaskCompletionAttempt,
			},
		}, &workflowservice.RespondWorkflowTaskCompletedRequest{
			PageNumber: int32(testCase.pageCount),
			Commands:   finalCommands,
		}, expectedNames
}

func benchmarkTaskCompletionCommand(markerName string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
		Attributes: &commandpb.Command_RecordMarkerCommandAttributes{
			RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
				MarkerName: markerName,
			},
		},
	}
}
