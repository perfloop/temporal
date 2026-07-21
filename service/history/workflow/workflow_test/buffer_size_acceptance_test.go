package workflow_test

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	historyservice "go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const benchmarkBufferedSignalCount = 100

func TestBufferSizeAcceptableActiveCommitBoundaries(t *testing.T) {
	input := bufferSizeSignalInputs()[2]

	t.Run("byte_limit_accepts_exact_total", func(t *testing.T) {
		maximumBytes := math.MaxInt
		mutableState := newBufferSizeActiveWorkflow(t, bufferSizeConfig(func() int { return math.MaxInt }, func() int { return maximumBytes }))

		totalSize := 0
		for i := 0; i < 3; i++ {
			event := addBufferedSignal(t, mutableState, input, "byte-exact-"+strconv.Itoa(i))
			totalSize += proto.Size(event)
			maximumBytes = totalSize
			if !mutableState.BufferSizeAcceptable() {
				t.Fatalf("exact byte total %d should be accepted", totalSize)
			}
			if workflowEvents := closeBufferedSignalTransaction(t, mutableState); hasWorkflowTaskFailedEvent(workflowEvents) {
				t.Fatal("exact byte total unexpectedly forced the workflow task closed")
			}
		}
	})

	t.Run("byte_limit_rejects_one_byte_over", func(t *testing.T) {
		maximumBytes := math.MaxInt
		mutableState := newBufferSizeActiveWorkflow(t, bufferSizeConfig(func() int { return math.MaxInt }, func() int { return maximumBytes }))

		totalSize := 0
		for i := 0; i < 2; i++ {
			event := addBufferedSignal(t, mutableState, input, "byte-prefix-"+strconv.Itoa(i))
			totalSize += proto.Size(event)
			maximumBytes = totalSize
			if workflowEvents := closeBufferedSignalTransaction(t, mutableState); hasWorkflowTaskFailedEvent(workflowEvents) {
				t.Fatal("prefix under the byte limit unexpectedly forced the workflow task closed")
			}
		}

		event := addBufferedSignal(t, mutableState, input, "byte-over")
		totalSize += proto.Size(event)
		maximumBytes = totalSize - 1
		if mutableState.BufferSizeAcceptable() {
			t.Fatal("one byte over the buffered-event limit should be rejected")
		}
		if workflowEvents := closeBufferedSignalTransaction(t, mutableState); !hasWorkflowTaskFailedEvent(workflowEvents) {
			t.Fatal("one byte over the buffered-event limit did not force the workflow task closed")
		}
	})

	t.Run("count_limit_accepts_boundary_and_rejects_next", func(t *testing.T) {
		maximumEvents := math.MaxInt
		mutableState := newBufferSizeActiveWorkflow(t, bufferSizeConfig(func() int { return maximumEvents }, func() int { return math.MaxInt }))

		for i := 1; i <= 2; i++ {
			addBufferedSignal(t, mutableState, input, "count-boundary-"+strconv.Itoa(i))
			maximumEvents = i
			if !mutableState.BufferSizeAcceptable() {
				t.Fatalf("exact buffered-event count %d should be accepted", i)
			}
			if workflowEvents := closeBufferedSignalTransaction(t, mutableState); hasWorkflowTaskFailedEvent(workflowEvents) {
				t.Fatalf("exact buffered-event count %d unexpectedly forced the workflow task closed", i)
			}
		}

		addBufferedSignal(t, mutableState, input, "count-over")
		if mutableState.BufferSizeAcceptable() {
			t.Fatal("one event over the buffered-event limit should be rejected")
		}
		if workflowEvents := closeBufferedSignalTransaction(t, mutableState); !hasWorkflowTaskFailedEvent(workflowEvents) {
			t.Fatal("one event over the buffered-event limit did not force the workflow task closed")
		}
	})
}

func BenchmarkBufferSizeAcceptableActiveCommitCadence(b *testing.B) {
	inputs := bufferSizeSignalInputs()
	requestIDs := make([]string, benchmarkBufferedSignalCount)
	for i := range requestIDs {
		requestIDs[i] = "benchmark-request-" + strconv.Itoa(i)
	}
	config := bufferSizeConfig(
		func() int { return benchmarkBufferedSignalCount },
		func() int { return 2 * 1024 * 1024 },
	)

	for b.Loop() {
		mutableState := newBufferSizeActiveWorkflow(b, config)
		successfulCommits := 0
		for i := range benchmarkBufferedSignalCount {
			event := addBufferedSignal(b, mutableState, inputs[i%len(inputs)], requestIDs[i])
			if event.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				b.Fatalf("unexpected event type: %v", event.GetEventType())
			}
			workflowEvents := closeBufferedSignalTransaction(b, mutableState)
			if hasWorkflowTaskFailedEvent(workflowEvents) {
				b.Fatal("configured buffered-event boundary should accept the active commit")
			}
			successfulCommits++
		}
		if successfulCommits != benchmarkBufferedSignalCount {
			b.Fatalf("completed %d signal commits, want %d", successfulCommits, benchmarkBufferedSignalCount)
		}
	}
}

type bufferSizeSignalInput struct {
	input  *commonpb.Payloads
	header *commonpb.Header
}

func bufferSizeSignalInputs() []bufferSizeSignalInput {
	return []bufferSizeSignalInput{
		newBufferSizeSignalInput(128, 1, 1),
		newBufferSizeSignalInput(1024, 2, 2),
		newBufferSizeSignalInput(4096, 4, 4),
		newBufferSizeSignalInput(4096, 8, 8),
	}
}

func newBufferSizeSignalInput(payloadBytes, payloadCount, headerCount int) bufferSizeSignalInput {
	payloads := make([]*commonpb.Payload, payloadCount)
	for i := range payloads {
		payloads[i] = &commonpb.Payload{
			Metadata: map[string][]byte{
				"encoding": []byte("binary/plain"),
				"index":    []byte(strconv.Itoa(i)),
			},
			Data: bufferSizeInputBytes(payloadBytes, i),
		}
	}
	headerFields := make(map[string]*commonpb.Payload, headerCount)
	for i := 0; i < headerCount; i++ {
		headerFields["header-"+strconv.Itoa(i)] = &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte("binary/plain")},
			Data:     bufferSizeInputBytes(payloadBytes/4, payloadCount+i),
		}
	}
	return bufferSizeSignalInput{
		input:  &commonpb.Payloads{Payloads: payloads},
		header: &commonpb.Header{Fields: headerFields},
	}
}

func bufferSizeInputBytes(size, seed int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(seed + i)
	}
	return data
}

func bufferSizeConfig(maximumEvents func() int, maximumBytes func() int) *configs.Config {
	config := tests.NewDynamicConfig()
	config.MaximumBufferedEventsBatch = maximumEvents
	config.MaximumBufferedEventsSizeInBytes = maximumBytes
	return config
}

func newBufferSizeActiveWorkflow(tb testing.TB, config *configs.Config) *workflow.MutableStateImpl {
	tb.Helper()
	controller := gomock.NewController(tb)
	shardContext := shard.NewTestContext(controller, &persistencespb.ShardInfo{}, config)
	registry := hsm.NewRegistry()
	if err := workflow.RegisterStateMachine(registry); err != nil {
		tb.Fatalf("register workflow state machine: %v", err)
	}
	shardContext.SetStateMachineRegistry(registry)

	namespaceEntry := tests.LocalNamespaceEntry
	namespaceCache := shardContext.Resource.NamespaceCache
	namespaceCache.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()

	clusterMetadata := shardContext.Resource.ClusterMetadata
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(namespaceEntry.IsGlobalNamespace(), namespaceEntry.FailoverVersion(tests.WorkflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()

	executionManager := shardContext.Resource.ExecutionMgr
	executionManager.EXPECT().GetHistoryBranchUtil().Return(persistence.NewHistoryBranchUtil(serialization.NewSerializer())).AnyTimes()

	eventsCache := events.NewMockCache(controller)
	eventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	mutableState := workflow.NewMutableState(
		shardContext,
		eventsCache,
		log.NewNoopLogger(),
		namespaceEntry,
		tests.WorkflowID,
		tests.RunID,
		time.Time{},
	)
	mutableState.GetExecutionInfo().NamespaceId = namespaceEntry.ID().String()
	mutableState.GetExecutionInfo().VersionHistories.Histories[0].Items = []*historyspb.VersionHistoryItem{{Version: 0, EventId: 1}}

	startBufferSizeWorkflow(tb, mutableState, namespaceEntry)
	if workflowEvents := closeBufferedSignalTransaction(tb, mutableState); hasWorkflowTaskFailedEvent(workflowEvents) {
		tb.Fatal("initial active workflow task unexpectedly failed")
	}
	return mutableState
}

func startBufferSizeWorkflow(tb testing.TB, mutableState *workflow.MutableStateImpl, namespaceEntry *namespace.Namespace) {
	tb.Helper()
	if _, err := mutableState.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{
			WorkflowId: mutableState.GetWorkflowKey().WorkflowID,
			RunId:      mutableState.GetWorkflowKey().RunID,
		},
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: namespaceEntry.ID().String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: "buffer-size-benchmark-workflow"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: "buffer-size-benchmark-queue"},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(time.Second),
			},
		},
	); err != nil {
		tb.Fatalf("start workflow: %v", err)
	}

	workflowTask, err := mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	if err != nil {
		tb.Fatalf("schedule workflow task: %v", err)
	}
	if _, _, err := mutableState.AddWorkflowTaskStartedEvent(
		workflowTask.ScheduledEventID,
		workflowTask.RequestID,
		workflowTask.TaskQueue,
		"",
		nil,
		nil,
		nil,
		false,
		nil,
		0,
	); err != nil {
		tb.Fatalf("start workflow task: %v", err)
	}
}

func addBufferedSignal(tb testing.TB, mutableState *workflow.MutableStateImpl, input bufferSizeSignalInput, requestID string) *historypb.HistoryEvent {
	tb.Helper()
	event, err := mutableState.AddWorkflowExecutionSignaled(
		"buffer-size-signal",
		input.input,
		"buffer-size-benchmark",
		input.header,
		requestID,
		nil,
	)
	if err != nil {
		tb.Fatalf("add workflow signal: %v", err)
	}
	return event
}

func closeBufferedSignalTransaction(tb testing.TB, mutableState *workflow.MutableStateImpl) []*persistence.WorkflowEvents {
	tb.Helper()
	mutation, workflowEvents, err := mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	if err != nil {
		tb.Fatalf("close active workflow transaction: %v", err)
	}
	if mutation == nil {
		tb.Fatal("close active workflow transaction returned no mutation")
	}
	return workflowEvents
}

func hasWorkflowTaskFailedEvent(workflowEvents []*persistence.WorkflowEvents) bool {
	for _, batch := range workflowEvents {
		for _, event := range batch.Events {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
				return true
			}
		}
	}
	return false
}
