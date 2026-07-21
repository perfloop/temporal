package getworkflowexecutionhistory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	historyapi "go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	historytests "go.temporal.io/server/service/history/tests"
)

const (
	terminalPageWorkflowID = "terminal-page-workflow"
	terminalPageRunID      = "terminal-page-run"
)

type terminalPageExecutionManager struct {
	persistence.ExecutionManager
	eventsByFirstEventID map[int64][]*historypb.HistoryEvent
}

func (m *terminalPageExecutionManager) ReadHistoryBranch(
	_ context.Context,
	request *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadHistoryBranchResponse, error) {
	events := m.eventsByFirstEventID[request.MinEventID]
	return &persistence.ReadHistoryBranchResponse{
		HistoryEvents: events,
		NextPageToken: nil,
		Size:          len(events),
	}, nil
}

type terminalPageShardContext struct {
	historyi.ShardContext
	config           *configs.Config
	executionManager persistence.ExecutionManager
	logger           log.Logger
}

func (s *terminalPageShardContext) GetConfig() *configs.Config {
	return s.config
}

func (s *terminalPageShardContext) GetExecutionManager() persistence.ExecutionManager {
	return s.executionManager
}

func (s *terminalPageShardContext) GetLogger() log.Logger {
	return s.logger
}

func (s *terminalPageShardContext) GetSearchAttributesProvider() searchattribute.Provider {
	return searchattribute.NewTestProvider()
}

func (s *terminalPageShardContext) GetSearchAttributesMapperProvider() searchattribute.MapperProvider {
	return searchattribute.NewTestMapperProvider(nil)
}

type terminalPageWorkflowContext struct {
	historyi.WorkflowContext
	mutableState historyi.MutableState
}

func (c *terminalPageWorkflowContext) LoadMutableState(
	_ context.Context,
	_ historyi.ShardContext,
) (historyi.MutableState, error) {
	return c.mutableState, nil
}

type terminalPageMutableState struct {
	historyi.MutableState
	branchToken    []byte
	executionInfo  *persistencespb.WorkflowExecutionInfo
	executionState *persistencespb.WorkflowExecutionState
	nextEventID    int64
	transientTasks *historyspb.TransientWorkflowTaskInfo
}

func (s *terminalPageMutableState) GetCurrentBranchToken() ([]byte, error) {
	return s.branchToken, nil
}

func (s *terminalPageMutableState) GetExecutionInfo() *persistencespb.WorkflowExecutionInfo {
	return s.executionInfo
}

func (s *terminalPageMutableState) GetExecutionState() *persistencespb.WorkflowExecutionState {
	return s.executionState
}

func (s *terminalPageMutableState) GetWorkflowStateStatus() (
	enumsspb.WorkflowExecutionState,
	enumspb.WorkflowExecutionStatus,
) {
	return enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
}

func (s *terminalPageMutableState) GetLastFirstEventIDTxnID() (int64, int64) {
	return 1, 1
}

func (s *terminalPageMutableState) GetNextEventID() int64 {
	return s.nextEventID
}

func (s *terminalPageMutableState) GetLastCompletedWorkflowTaskStartedEventId() int64 {
	return 0
}

func (s *terminalPageMutableState) IsStickyTaskQueueSet() bool {
	return false
}

func (s *terminalPageMutableState) GetAssignedBuildId() string {
	return ""
}

func (s *terminalPageMutableState) GetInheritedBuildId() string {
	return ""
}

func (s *terminalPageMutableState) GetPendingWorkflowTask() *historyi.WorkflowTaskInfo {
	return &historyi.WorkflowTaskInfo{}
}

func (s *terminalPageMutableState) GetTransientWorkflowTaskInfo(
	_ *historyi.WorkflowTaskInfo,
	_ string,
) *historyspb.TransientWorkflowTaskInfo {
	return s.transientTasks
}

type terminalPageConsistencyChecker struct {
	historyapi.WorkflowConsistencyChecker
	workflowLease historyapi.WorkflowLease
	leaseCalls    int64
}

func (c *terminalPageConsistencyChecker) GetWorkflowLeaseWithConsistencyCheck(
	_ context.Context,
	_ *clockspb.VectorClock,
	_ historyapi.MutableStateConsistencyPredicate,
	_ definition.WorkflowKey,
	_ locks.Priority,
) (historyapi.WorkflowLease, error) {
	c.leaseCalls++
	return c.workflowLease, nil
}

type terminalPageVisibilityManager struct {
	manager.VisibilityManager
}

func (terminalPageVisibilityManager) GetIndexName() string {
	return ""
}

type terminalPageFixture struct {
	ctx                context.Context
	request            *historyservice.GetWorkflowExecutionHistoryRequest
	shardContext       *terminalPageShardContext
	consistencyChecker *terminalPageConsistencyChecker
	visibilityManager  manager.VisibilityManager
}

func newTerminalPageFixture(
	testingTB testing.TB,
	nextEventID int64,
	transientTasks *historyspb.TransientWorkflowTaskInfo,
	eventsByFirstEventID map[int64][]*historypb.HistoryEvent,
) *terminalPageFixture {
	testingTB.Helper()

	branchToken := []byte("terminal-page-branch")
	continuationToken, err := historyapi.SerializeHistoryToken(&tokenspb.HistoryContinuation{
		RunId:             terminalPageRunID,
		FirstEventId:      1,
		NextEventId:       5,
		PersistenceToken:  []byte("persistence-page"),
		BranchToken:       branchToken,
		IsWorkflowRunning: true,
	})
	require.NoError(testingTB, err)

	config := historytests.NewDynamicConfig()
	config.SendTransientOrSpeculativeWorkflowTaskEvents = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	mutableState := &terminalPageMutableState{
		branchToken: branchToken,
		executionInfo: &persistencespb.WorkflowExecutionInfo{
			WorkflowId:       terminalPageWorkflowID,
			WorkflowTypeName: "terminal-page-workflow-type",
			TaskQueue:        "terminal-page-task-queue",
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: nextEventID - 1, Version: 1},
						},
					},
				},
			},
		},
		executionState: &persistencespb.WorkflowExecutionState{RunId: terminalPageRunID},
		nextEventID:    nextEventID,
		transientTasks: transientTasks,
	}
	workflowContext := &terminalPageWorkflowContext{mutableState: mutableState}
	workflowLease := historyapi.NewWorkflowLease(workflowContext, func(error) {}, mutableState)
	consistencyChecker := &terminalPageConsistencyChecker{workflowLease: workflowLease}

	return &terminalPageFixture{
		ctx: headers.SetVersionsForTests(
			context.Background(),
			"1.0.0",
			headers.ClientNameGoSDK,
			headers.SupportedServerVersions,
			headers.AllFeatures,
		),
		request: &historyservice.GetWorkflowExecutionHistoryRequest{
			NamespaceId: historytests.NamespaceID.String(),
			Request: &workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:       historytests.Namespace.String(),
				Execution:       &commonpb.WorkflowExecution{WorkflowId: terminalPageWorkflowID, RunId: terminalPageRunID},
				MaximumPageSize: 10,
				NextPageToken:   continuationToken,
			},
		},
		shardContext: &terminalPageShardContext{
			config: config,
			executionManager: &terminalPageExecutionManager{
				eventsByFirstEventID: eventsByFirstEventID,
			},
			logger: log.NewNoopLogger(),
		},
		consistencyChecker: consistencyChecker,
		visibilityManager:  terminalPageVisibilityManager{},
	}
}

func (f *terminalPageFixture) invoke() (*historyservice.GetWorkflowExecutionHistoryResponseWithRaw, error) {
	return Invoke(
		f.ctx,
		f.shardContext,
		f.consistencyChecker,
		headers.NewDefaultVersionChecker(),
		nil,
		f.request,
		f.visibilityManager,
	)
}

func (f *terminalPageFixture) resetLeaseCalls() {
	f.consistencyChecker.leaseCalls = 0
}

func (f *terminalPageFixture) leaseCalls() int64 {
	return f.consistencyChecker.leaseCalls
}

func transientWorkflowTask(startEventID int64) *historyspb.TransientWorkflowTaskInfo {
	return &historyspb.TransientWorkflowTaskInfo{
		HistorySuffix: []*historypb.HistoryEvent{
			{EventId: startEventID, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED},
			{EventId: startEventID + 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED},
		},
	}
}

func historyEvent(eventID int64) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{EventId: eventID, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED}
}

func TestInvokeTerminalPageFreshSnapshotPreservesResponse(t *testing.T) {
	testCases := []struct {
		name                 string
		nextEventID          int64
		transientTasks       *historyspb.TransientWorkflowTaskInfo
		eventsByFirstEventID map[int64][]*historypb.HistoryEvent
		wantEventIDs         []int64
		wantLeaseCalls       int64
	}{
		{
			name:           "transient workflow task",
			nextEventID:    5,
			transientTasks: transientWorkflowTask(5),
			eventsByFirstEventID: map[int64][]*historypb.HistoryEvent{
				1: {historyEvent(4)},
			},
			wantEventIDs: []int64{4, 5, 6},
		},
		{
			name:        "known empty snapshot",
			nextEventID: 5,
			eventsByFirstEventID: map[int64][]*historypb.HistoryEvent{
				1: {historyEvent(4)},
			},
			wantEventIDs: []int64{4},
		},
		{
			name:           "invalid transient workflow task",
			nextEventID:    5,
			transientTasks: transientWorkflowTask(6),
			eventsByFirstEventID: map[int64][]*historypb.HistoryEvent{
				1: {historyEvent(4)},
			},
			wantEventIDs: []int64{4},
		},
		{
			name:           "gap before fresh transient workflow task",
			nextEventID:    6,
			transientTasks: transientWorkflowTask(6),
			eventsByFirstEventID: map[int64][]*historypb.HistoryEvent{
				1: {historyEvent(4)},
				5: {historyEvent(5)},
			},
			wantEventIDs:   []int64{4, 5, 6, 7},
			wantLeaseCalls: 1,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newTerminalPageFixture(
				t,
				testCase.nextEventID,
				testCase.transientTasks,
				testCase.eventsByFirstEventID,
			)

			response, err := fixture.invoke()
			require.NoError(t, err)
			require.NotNil(t, response.Response)
			require.NotNil(t, response.Response.History)

			gotEventIDs := make([]int64, 0, len(response.Response.History.Events))
			for _, event := range response.Response.History.Events {
				gotEventIDs = append(gotEventIDs, event.GetEventId())
			}
			require.Equal(t, testCase.wantEventIDs, gotEventIDs)
			if testCase.wantLeaseCalls != 0 {
				require.Equal(t, testCase.wantLeaseCalls, fixture.leaseCalls())
			}
		})
	}
}

func BenchmarkInvokeTerminalPageFreshTransientSnapshot(b *testing.B) {
	fixture := newTerminalPageFixture(
		b,
		5,
		transientWorkflowTask(5),
		map[int64][]*historypb.HistoryEvent{
			1: {historyEvent(4)},
		},
	)
	fixture.resetLeaseCalls()

	b.ResetTimer()
	for b.Loop() {
		response, err := fixture.invoke()
		if err != nil {
			b.Fatal(err)
		}
		if got := response.GetResponse().GetHistory().GetEvents(); len(got) != 3 || got[2].GetEventId() != 6 {
			b.Fatalf("unexpected history response: %v", got)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(fixture.leaseCalls())/float64(b.N), "lease_calls/op")
}
