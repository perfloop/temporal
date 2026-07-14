package startworkflow

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
	historyapi "go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

const historyReaderShardID int32 = 1

const historyReaderEventCount = 1025

var errDuplicatePageRequest = errors.New("duplicate history page request")

type historyReaderState struct {
	continuationToken     []byte
	duplicatePageRequests int
	databaseReadRequests  int
}

type countingHistoryStore struct {
	persistence.ExecutionStore
	state *historyReaderState
}

type duplicateStoppingExecutionManager struct {
	persistence.ExecutionManager
	state *historyReaderState
}

func TestStarterGetWorkflowHistorySinglePage(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	executionManager := persistence.NewMockExecutionManager(ctrl)
	starter := newHistoryReaderStarter(ctrl, executionManager)

	executionManager.EXPECT().ReadHistoryBranch(gomock.Any(), historyReadRequest(nil)).Return(
		&persistence.ReadHistoryBranchResponse{
			HistoryEvents: []*historypb.HistoryEvent{{EventId: 1}},
		},
		nil,
	)

	events, err := starter.getWorkflowHistory(context.Background(), historyReaderMutableState())
	if err != nil {
		t.Fatalf("getWorkflowHistory returned error: %v", err)
	}
	if len(events) != 1 || events[0].GetEventId() != 1 {
		t.Fatalf("getWorkflowHistory returned events: %#v", events)
	}
}

type duplicatePageRequestControl struct {
	starter               *Starter
	duplicatePageRequests int
}

func newDuplicatePageRequestControl(tb testing.TB) (*duplicatePageRequestControl, *gomock.Controller) {
	tb.Helper()

	ctrl := gomock.NewController(tb)
	executionManager := persistence.NewMockExecutionManager(ctrl)
	control := &duplicatePageRequestControl{starter: newHistoryReaderStarter(ctrl, executionManager)}
	continuationToken := []byte("next-page")
	calls := 0

	executionManager.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.ReadHistoryBranchRequest) (*persistence.ReadHistoryBranchResponse, error) {
			calls++
			switch calls {
			case 1:
				if len(request.NextPageToken) != 0 {
					tb.Fatalf("first request token = %q, want empty", request.NextPageToken)
				}
				return &persistence.ReadHistoryBranchResponse{
					HistoryEvents: []*historypb.HistoryEvent{{EventId: 1}},
					NextPageToken: continuationToken,
				}, nil
			case 2:
				if len(request.NextPageToken) == 0 {
					control.duplicatePageRequests++
					return nil, errDuplicatePageRequest
				}
				if !bytes.Equal(request.NextPageToken, continuationToken) {
					tb.Fatalf("second request token = %q, want %q", request.NextPageToken, continuationToken)
				}
				return &persistence.ReadHistoryBranchResponse{
					HistoryEvents: []*historypb.HistoryEvent{{EventId: 2}},
				}, nil
			default:
				tb.Fatalf("ReadHistoryBranch calls = %d, want 2", calls)
				return nil, nil
			}
		},
	).AnyTimes()
	return control, ctrl
}

func (c *duplicatePageRequestControl) getWorkflowHistory() ([]*historypb.HistoryEvent, error) {
	return c.starter.getWorkflowHistory(context.Background(), historyReaderMutableState())
}

func TestStarterGetWorkflowHistoryPropagatesNextPageToken(t *testing.T) {
	t.Parallel()

	control, ctrl := newDuplicatePageRequestControl(t)
	t.Cleanup(ctrl.Finish)
	events, err := control.getWorkflowHistory()
	if errors.Is(err, errDuplicatePageRequest) {
		t.Fatalf("duplicate_page_requests/op = %d, want 0", control.duplicatePageRequests)
	}
	if err != nil {
		t.Fatalf("getWorkflowHistory returned error: %v", err)
	}
	if control.duplicatePageRequests != 0 || len(events) != 2 || events[0].GetEventId() != 1 || events[1].GetEventId() != 2 {
		t.Fatalf("duplicate_page_requests/op = %d, events = %#v", control.duplicatePageRequests, events)
	}
}

func TestStarterRespondToRetriedRequestReadsSecondHistoryPage(t *testing.T) {
	t.Parallel()

	starter, state, mutableState := newSQLiteHistoryReader(t)
	state.reset()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	workflowCache := wcache.NewMockCache(ctrl)
	consistencyChecker := historyapi.NewMockWorkflowConsistencyChecker(ctrl)
	workflowContext := historyi.NewMockWorkflowContext(ctrl)
	workflowMutableState := historyi.NewMockMutableState(ctrl)
	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "namespace-id", Name: "namespace"},
		nil,
		"active",
	)
	consistencyChecker.EXPECT().GetWorkflowCache().Return(workflowCache)
	workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		starter.shardContext,
		namespaceEntry.ID(),
		gomock.Any(),
		gomock.Any(),
	).Return(workflowContext, func(error) {}, nil)
	workflowContext.EXPECT().LoadMutableState(gomock.Any(), starter.shardContext).Return(workflowMutableState, nil)
	workflowMutableState.EXPECT().GetCurrentBranchToken().Return(mutableState.branchToken, nil)
	workflowMutableState.EXPECT().GetStartedWorkflowTask().Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID: 2,
		StartedEventID:   3,
		Attempt:          1,
	})
	workflowMutableState.EXPECT().GetFirstRunID(gomock.Any()).Return("first-run", nil)
	workflowMutableState.EXPECT().GetNextEventID().Return(int64(historyReaderEventCount + 2))

	errAfterHistory := errors.New("response construction reached")
	shardContext := starter.shardContext.(*historyi.MockShardContext)
	shardContext.EXPECT().NewVectorClock().Return(nil, errAfterHistory)
	starter.workflowConsistencyChecker = consistencyChecker
	starter.namespace = namespaceEntry
	starter.request = &historyservice.StartWorkflowExecutionRequest{
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			RequestEagerExecution: true,
			WorkflowId:            "workflow",
		},
	}

	_, err := starter.respondToRetriedRequest(context.Background(), "run", "first-run")
	if !errors.Is(err, errAfterHistory) {
		t.Fatalf("respondToRetriedRequest returned error: %v", err)
	}
	if state.duplicatePageRequests != 0 || state.databaseReadRequests != 2 {
		t.Fatalf("paged retry read state = %#v", state)
	}
}

func BenchmarkStarterGetWorkflowHistoryDuplicatePageRequests(b *testing.B) {
	starter, state, mutableState := newSQLiteHistoryReader(b)

	b.ResetTimer()
	for range b.N {
		state.reset()
		events, err := starter.getWorkflowHistory(context.Background(), mutableState)
		if errors.Is(err, errDuplicatePageRequest) {
			if state.duplicatePageRequests != 1 || state.databaseReadRequests != 2 {
				b.Fatalf("duplicate read state = %#v", state)
			}
			continue
		}
		if err != nil {
			b.Fatalf("getWorkflowHistory returned error: %v", err)
		}
		if state.duplicatePageRequests != 0 || state.databaseReadRequests != 2 {
			b.Fatalf("paged read state = %#v", state)
		}
		if len(events) != historyReaderEventCount {
			b.Fatalf("getWorkflowHistory returned %d events, want %d", len(events), historyReaderEventCount)
		}
		if events[0].GetEventId() != 1 || events[len(events)-1].GetEventId() != historyReaderEventCount {
			b.Fatalf("getWorkflowHistory returned unexpected event IDs: first=%d last=%d", events[0].GetEventId(), events[len(events)-1].GetEventId())
		}
	}
}

func newHistoryReaderStarter(ctrl *gomock.Controller, executionManager persistence.ExecutionManager) *Starter {
	shardContext := historyi.NewMockShardContext(ctrl)
	shardContext.EXPECT().GetExecutionManager().Return(executionManager).AnyTimes()
	shardContext.EXPECT().GetShardID().Return(historyReaderShardID).AnyTimes()
	return &Starter{shardContext: shardContext}
}

func historyReaderMutableState() *mutableStateInfo {
	return &mutableStateInfo{
		branchToken: []byte("branch"),
		lastEventID: 3,
	}
}

func historyReadRequest(nextPageToken []byte) *persistence.ReadHistoryBranchRequest {
	return &persistence.ReadHistoryBranchRequest{
		ShardID:       historyReaderShardID,
		BranchToken:   []byte("branch"),
		MinEventID:    1,
		MaxEventID:    3,
		PageSize:      1024,
		NextPageToken: nextPageToken,
	}
}

func newSQLiteHistoryReader(tb testing.TB) (*Starter, *historyReaderState, *mutableStateInfo) {
	tb.Helper()

	serializer := serialization.NewSerializer()
	factory := sql.NewFactory(
		config.SQL{
			PluginName:        "sqlite",
			DatabaseName:      "startworkflow-history-reader",
			ConnectAttributes: map[string]string{"mode": "memory", "cache": "private"},
		},
		resolver.NewNoopResolver(),
		"history-reader",
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		serializer,
	)
	tb.Cleanup(factory.Close)

	store, err := factory.NewExecutionStore()
	if err != nil {
		tb.Fatalf("NewExecutionStore failed: %v", err)
	}
	state := &historyReaderState{}
	executionManager := persistence.NewExecutionManager(
		&countingHistoryStore{ExecutionStore: store, state: state},
		serializer,
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)

	branchToken, err := executionManager.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		nil,
		nil,
		0,
		0,
		0,
	)
	if err != nil {
		tb.Fatalf("NewHistoryBranch failed: %v", err)
	}
	for eventID := int64(1); eventID <= historyReaderEventCount; eventID++ {
		_, err = executionManager.AppendHistoryNodes(context.Background(), &persistence.AppendHistoryNodesRequest{
			ShardID:       historyReaderShardID,
			IsNewBranch:   eventID == 1,
			BranchToken:   branchToken,
			Events:        []*historypb.HistoryEvent{{EventId: eventID, Version: 1}},
			TransactionID: eventID,
		})
		if err != nil {
			tb.Fatalf("AppendHistoryNodes(%d) failed: %v", eventID, err)
		}
	}

	ctrl := gomock.NewController(tb)
	tb.Cleanup(ctrl.Finish)
	starter := newHistoryReaderStarter(ctrl, &duplicateStoppingExecutionManager{
		ExecutionManager: executionManager,
		state:            state,
	})
	return starter, state, &mutableStateInfo{
		branchToken: branchToken,
		lastEventID: historyReaderEventCount + 1,
	}
}

func (s *historyReaderState) reset() {
	s.continuationToken = nil
	s.duplicatePageRequests = 0
	s.databaseReadRequests = 0
}

func (s *countingHistoryStore) ReadHistoryBranch(
	ctx context.Context,
	request *persistence.InternalReadHistoryBranchRequest,
) (*persistence.InternalReadHistoryBranchResponse, error) {
	s.state.databaseReadRequests++
	return s.ExecutionStore.ReadHistoryBranch(ctx, request)
}

func (m *duplicateStoppingExecutionManager) ReadHistoryBranch(
	ctx context.Context,
	request *persistence.ReadHistoryBranchRequest,
) (*persistence.ReadHistoryBranchResponse, error) {
	response, err := m.ExecutionManager.ReadHistoryBranch(ctx, request)
	if err != nil {
		return nil, err
	}

	if len(request.NextPageToken) == 0 {
		if m.state.continuationToken != nil {
			m.state.duplicatePageRequests++
			return nil, errDuplicatePageRequest
		}
		if len(response.NextPageToken) == 0 {
			return nil, errors.New("first history page has no continuation token")
		}
		m.state.continuationToken = response.NextPageToken
		return response, nil
	}
	if !bytes.Equal(request.NextPageToken, m.state.continuationToken) {
		return nil, errors.New("unexpected history continuation token")
	}
	if len(response.NextPageToken) != 0 {
		return nil, errors.New("continuation history page was not terminal")
	}
	return response, nil
}
