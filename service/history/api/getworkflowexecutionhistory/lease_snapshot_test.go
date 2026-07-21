package getworkflowexecutionhistory

import (
	"context"
	"errors"
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
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/searchattribute"
	historyapi "go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	historytests "go.temporal.io/server/service/history/tests"
)

type heldLeaseExecutionManager struct {
	persistence.ExecutionManager
	eventsByFirstEventID map[int64][]*historypb.HistoryEvent
	payloadSerializer    serialization.Serializer
}

func (m *heldLeaseExecutionManager) ReadHistoryBranch(
	_ context.Context,
	request *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadHistoryBranchResponse, error) {
	events := m.eventsByFirstEventID[request.MinEventID]
	return &persistence.ReadHistoryBranchResponse{
		HistoryEvents: events,
		Size:          len(events),
	}, nil
}

func (m *heldLeaseExecutionManager) ReadRawHistoryBranch(
	_ context.Context,
	request *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadRawHistoryBranchResponse, error) {
	events := m.eventsByFirstEventID[request.MinEventID]
	blob, err := m.payloadSerializer.SerializeEvents(events)
	if err != nil {
		return nil, err
	}
	return &persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{blob},
		NodeIDs:           []int64{request.MinEventID},
		Size:              len(events),
	}, nil
}

type heldLeaseShardContext struct {
	historyi.ShardContext
	config            *configs.Config
	executionManager  persistence.ExecutionManager
	payloadSerializer serialization.Serializer
	logger            log.Logger
}

func (s *heldLeaseShardContext) GetConfig() *configs.Config {
	return s.config
}

func (s *heldLeaseShardContext) GetExecutionManager() persistence.ExecutionManager {
	return s.executionManager
}

func (s *heldLeaseShardContext) GetPayloadSerializer() serialization.Serializer {
	return s.payloadSerializer
}

func (s *heldLeaseShardContext) GetShardID() int32 {
	return 1
}

func (s *heldLeaseShardContext) GetLogger() log.Logger {
	return s.logger
}

func (s *heldLeaseShardContext) GetSearchAttributesProvider() searchattribute.Provider {
	return searchattribute.NewTestProvider()
}

func (s *heldLeaseShardContext) GetSearchAttributesMapperProvider() searchattribute.MapperProvider {
	return searchattribute.NewTestMapperProvider(nil)
}

type heldLeaseMutableState struct {
	*terminalPageMutableState
	workflowState  enumsspb.WorkflowExecutionState
	workflowStatus enumspb.WorkflowExecutionStatus
}

func (s *heldLeaseMutableState) GetWorkflowStateStatus() (
	enumsspb.WorkflowExecutionState,
	enumspb.WorkflowExecutionStatus,
) {
	return s.workflowState, s.workflowStatus
}

type heldLeaseConsistencyChecker struct {
	historyapi.WorkflowConsistencyChecker
	workflowLeases []historyapi.WorkflowLease
	leaseCalls     int64
	releaseCalls   int64
}

func (c *heldLeaseConsistencyChecker) GetWorkflowLeaseWithConsistencyCheck(
	_ context.Context,
	_ *clockspb.VectorClock,
	_ historyapi.MutableStateConsistencyPredicate,
	_ definition.WorkflowKey,
	_ locks.Priority,
) (historyapi.WorkflowLease, error) {
	leaseIndex := int(c.leaseCalls)
	if leaseIndex >= len(c.workflowLeases) {
		leaseIndex = len(c.workflowLeases) - 1
	}
	if leaseIndex > 0 && c.releaseCalls == 0 {
		return nil, errors.New("second mutable state requested before the first lease was released")
	}
	c.leaseCalls++
	return c.workflowLeases[leaseIndex], nil
}

type heldLeaseFixture struct {
	ctx                context.Context
	request            *historyservice.GetWorkflowExecutionHistoryRequest
	shardContext       *heldLeaseShardContext
	consistencyChecker *heldLeaseConsistencyChecker
	useRawHistory      bool
}

func newHeldLeaseMutableState(
	nextEventID int64,
	transientTasks *historyspb.TransientWorkflowTaskInfo,
	workflowState enumsspb.WorkflowExecutionState,
	workflowStatus enumspb.WorkflowExecutionStatus,
) *heldLeaseMutableState {
	branchToken := []byte("terminal-page-branch")
	return &heldLeaseMutableState{
		terminalPageMutableState: &terminalPageMutableState{
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
		},
		workflowState:  workflowState,
		workflowStatus: workflowStatus,
	}
}

func newHeldLeaseFixture(
	testingTB testing.TB,
	useRawHistory bool,
	eventsByFirstEventID map[int64][]*historypb.HistoryEvent,
	mutableStates ...*heldLeaseMutableState,
) *heldLeaseFixture {
	testingTB.Helper()
	require.NotEmpty(testingTB, mutableStates)

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

	payloadSerializer := serialization.NewSerializer()
	config := historytests.NewDynamicConfig()
	config.SendTransientOrSpeculativeWorkflowTaskEvents = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	config.SendRawWorkflowHistory = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(useRawHistory)

	consistencyChecker := &heldLeaseConsistencyChecker{}
	for _, mutableState := range mutableStates {
		workflowContext := &terminalPageWorkflowContext{mutableState: mutableState}
		workflowLease := historyapi.NewWorkflowLease(workflowContext, func(error) {
			consistencyChecker.releaseCalls++
		}, mutableState)
		consistencyChecker.workflowLeases = append(consistencyChecker.workflowLeases, workflowLease)
	}

	return &heldLeaseFixture{
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
		shardContext: &heldLeaseShardContext{
			config: config,
			executionManager: &heldLeaseExecutionManager{
				eventsByFirstEventID: eventsByFirstEventID,
				payloadSerializer:    payloadSerializer,
			},
			payloadSerializer: payloadSerializer,
			logger:            log.NewNoopLogger(),
		},
		consistencyChecker: consistencyChecker,
		useRawHistory:      useRawHistory,
	}
}

func (f *heldLeaseFixture) invoke() (*historyservice.GetWorkflowExecutionHistoryResponseWithRaw, error) {
	return Invoke(
		f.ctx,
		f.shardContext,
		f.consistencyChecker,
		headers.NewDefaultVersionChecker(),
		nil,
		f.request,
		terminalPageVisibilityManager{},
	)
}

func (f *heldLeaseFixture) responseEventIDs(
	t *testing.T,
	response *historyservice.GetWorkflowExecutionHistoryResponseWithRaw,
) []int64 {
	t.Helper()
	var events []*historypb.HistoryEvent
	if f.useRawHistory {
		for _, blob := range response.GetResponse().GetRawHistory() {
			blobEvents, err := f.shardContext.GetPayloadSerializer().DeserializeEvents(blob)
			require.NoError(t, err)
			events = append(events, blobEvents...)
		}
	} else {
		events = response.GetResponse().GetHistory().GetEvents()
	}

	eventIDs := make([]int64, 0, len(events))
	for _, event := range events {
		eventIDs = append(eventIDs, event.GetEventId())
	}
	return eventIDs
}

func TestInvokeTerminalPageHeldSnapshotConsistency(t *testing.T) {
	testCases := []struct {
		name                       string
		eventsByFirstEventID       map[int64][]*historypb.HistoryEvent
		mutableStates              []*heldLeaseMutableState
		wantEventIDs               []int64
		wantEventIDsAfterRelease   []int64
		wantCompletedAfterRelease  bool
		wantLeaseCallsAfterRelease int64
	}{
		{
			name: "fresh transient suffix",
			eventsByFirstEventID: map[int64][]*historypb.HistoryEvent{
				1: {historyEvent(4)},
			},
			mutableStates: []*heldLeaseMutableState{
				newHeldLeaseMutableState(5, transientWorkflowTask(5), enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			},
			wantEventIDs: []int64{4, 5, 6},
		},
		{
			name: "known empty snapshot",
			eventsByFirstEventID: map[int64][]*historypb.HistoryEvent{
				1: {historyEvent(4)},
			},
			mutableStates: []*heldLeaseMutableState{
				newHeldLeaseMutableState(5, nil, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			},
			wantEventIDs: []int64{4},
		},
		{
			name: "suffix would be created after the freshness snapshot",
			eventsByFirstEventID: map[int64][]*historypb.HistoryEvent{
				1: {historyEvent(4)},
			},
			mutableStates: []*heldLeaseMutableState{
				newHeldLeaseMutableState(5, nil, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
				newHeldLeaseMutableState(5, transientWorkflowTask(5), enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
			},
			wantEventIDs:               []int64{4},
			wantEventIDsAfterRelease:   []int64{4, 5, 6},
			wantLeaseCallsAfterRelease: 2,
		},
		{
			name: "workflow would complete after the freshness snapshot",
			eventsByFirstEventID: map[int64][]*historypb.HistoryEvent{
				1: {historyEvent(4)},
			},
			mutableStates: []*heldLeaseMutableState{
				newHeldLeaseMutableState(5, nil, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
				newHeldLeaseMutableState(5, nil, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED),
			},
			wantEventIDs:               []int64{4},
			wantEventIDsAfterRelease:   []int64{4},
			wantCompletedAfterRelease:  true,
			wantLeaseCallsAfterRelease: 3,
		},
	}

	for _, useRawHistory := range []bool{false, true} {
		representation := "decoded history"
		if useRawHistory {
			representation = "raw history"
		}
		t.Run(representation, func(t *testing.T) {
			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					// The first state models the terminal freshness read. Additional states are available only
					// after the prior lease is released; a subsequent terminal query observes that update,
					// whereas the first response stays linearized at its held snapshot.
					fixture := newHeldLeaseFixture(
						t,
						useRawHistory,
						testCase.eventsByFirstEventID,
						testCase.mutableStates...,
					)

					response, err := fixture.invoke()
					require.NoError(t, err)
					require.NotNil(t, response.Response)
					require.Equal(t, testCase.wantEventIDs, fixture.responseEventIDs(t, response))
					if testCase.wantEventIDsAfterRelease == nil {
						require.Equal(t, int64(1), fixture.consistencyChecker.leaseCalls)
						require.Equal(t, int64(1), fixture.consistencyChecker.releaseCalls)
						return
					}

					// A later terminal query cannot observe the second state until Invoke released the held
					// first lease. The checker rejects a second lease before that release.
					require.Equal(t, int64(1), fixture.consistencyChecker.releaseCalls)
					responseAfterRelease, err := fixture.invoke()
					require.NoError(t, err)
					require.Equal(t, testCase.wantEventIDsAfterRelease, fixture.responseEventIDs(t, responseAfterRelease))
					if testCase.wantCompletedAfterRelease {
						require.Empty(t, responseAfterRelease.GetResponse().GetNextPageToken())
					}
					require.Equal(t, testCase.wantLeaseCallsAfterRelease, fixture.consistencyChecker.leaseCalls)
					require.Equal(t, testCase.wantLeaseCallsAfterRelease, fixture.consistencyChecker.releaseCalls)
				})
			}
		})
	}
}
