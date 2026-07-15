package startworkflow

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func TestStarterGetWorkflowHistorySinglePage(t *testing.T) {
	event := &historypb.HistoryEvent{EventId: 1}
	requests := 0
	starter := newHistoryStarter(t, func(request *persistence.ReadHistoryBranchRequest) (*persistence.ReadHistoryBranchResponse, error) {
		requests++
		if !historyRequestMatches(request) || len(request.NextPageToken) != 0 {
			return nil, errors.New("unexpected single-page history request")
		}
		return &persistence.ReadHistoryBranchResponse{HistoryEvents: []*historypb.HistoryEvent{event}}, nil
	})

	events, err := starter.getWorkflowHistory(context.Background(), historyMutableStateInfo())

	require.NoError(t, err)
	require.Equal(t, []*historypb.HistoryEvent{event}, events)
	require.Equal(t, 1, requests)
}

func TestStarterGetWorkflowHistoryPropagatesNextPageToken(t *testing.T) {
	nextPageToken := []byte("next-page")
	duplicatePageRequest := errors.New("duplicate page request")
	firstEvent := &historypb.HistoryEvent{EventId: 1}
	secondEvent := &historypb.HistoryEvent{EventId: 2}
	emptyTokenRequests := 0
	duplicatePageRequests := 0
	starter := newHistoryStarter(t, func(request *persistence.ReadHistoryBranchRequest) (*persistence.ReadHistoryBranchResponse, error) {
		if !historyRequestMatches(request) {
			return nil, errors.New("unexpected history request")
		}

		switch {
		case len(request.NextPageToken) == 0:
			emptyTokenRequests++
			if emptyTokenRequests == 1 {
				return &persistence.ReadHistoryBranchResponse{
					HistoryEvents: []*historypb.HistoryEvent{firstEvent},
					NextPageToken: nextPageToken,
				}, nil
			}
			duplicatePageRequests++
			return nil, duplicatePageRequest
		case bytes.Equal(request.NextPageToken, nextPageToken):
			return &persistence.ReadHistoryBranchResponse{
				HistoryEvents: []*historypb.HistoryEvent{secondEvent},
			}, nil
		default:
			return nil, errors.New("unexpected history page token")
		}
	})

	events, err := starter.getWorkflowHistory(context.Background(), historyMutableStateInfo())

	require.NoError(t, err)
	require.Len(t, events, 2)
	require.EqualValues(t, 1, events[0].GetEventId())
	require.EqualValues(t, 2, events[1].GetEventId())
	require.Zero(t, duplicatePageRequests)
	require.Equal(t, 1, emptyTokenRequests)
}

func BenchmarkStarterGetWorkflowHistoryDuplicatePageRequests(b *testing.B) {
	nextPageToken := []byte("next-page")
	duplicatePageRequest := errors.New("duplicate page request")
	firstEvent := &historypb.HistoryEvent{EventId: 1}
	secondEvent := &historypb.HistoryEvent{EventId: 2}
	type operationState struct {
		firstPageRead         bool
		duplicatePageRequests int
	}

	var state *operationState
	starter := newHistoryStarter(b, func(request *persistence.ReadHistoryBranchRequest) (*persistence.ReadHistoryBranchResponse, error) {
		if !historyRequestMatches(request) {
			return nil, errors.New("unexpected history request")
		}

		switch {
		case len(request.NextPageToken) == 0:
			if !state.firstPageRead {
				state.firstPageRead = true
				return &persistence.ReadHistoryBranchResponse{
					HistoryEvents: []*historypb.HistoryEvent{firstEvent},
					NextPageToken: nextPageToken,
				}, nil
			}
			state.duplicatePageRequests++
			return nil, duplicatePageRequest
		case bytes.Equal(request.NextPageToken, nextPageToken):
			return &persistence.ReadHistoryBranchResponse{
				HistoryEvents: []*historypb.HistoryEvent{secondEvent},
			}, nil
		default:
			return nil, errors.New("unexpected history page token")
		}
	})

	duplicates := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state = &operationState{}
		events, err := starter.getWorkflowHistory(context.Background(), historyMutableStateInfo())
		switch {
		case errors.Is(err, duplicatePageRequest):
			if state.duplicatePageRequests != 1 {
				b.Fatalf("duplicate page requests = %d, want 1", state.duplicatePageRequests)
			}
		case err != nil:
			b.Fatalf("get workflow history: %v", err)
		default:
			if state.duplicatePageRequests != 0 {
				b.Fatalf("duplicate page requests = %d, want 0", state.duplicatePageRequests)
			}
			if len(events) != 2 || events[0].GetEventId() != 1 || events[1].GetEventId() != 2 {
				b.Fatalf("history events = %v, want event IDs [1 2]", events)
			}
		}
		duplicates += state.duplicatePageRequests
	}
	b.StopTimer()
	b.ReportMetric(float64(duplicates)/float64(b.N), "duplicate_page_requests/op")
}

func newHistoryStarter(
	t testing.TB,
	readHistoryBranch func(*persistence.ReadHistoryBranchRequest) (*persistence.ReadHistoryBranchResponse, error),
) *Starter {
	t.Helper()
	controller := gomock.NewController(t)
	shardContext := historyi.NewMockShardContext(controller)
	executionManager := persistence.NewMockExecutionManager(controller)
	shardContext.EXPECT().GetExecutionManager().Return(executionManager).AnyTimes()
	shardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	executionManager.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.ReadHistoryBranchRequest) (*persistence.ReadHistoryBranchResponse, error) {
			return readHistoryBranch(request)
		},
	).AnyTimes()
	return &Starter{shardContext: shardContext}
}

func historyMutableStateInfo() *mutableStateInfo {
	return &mutableStateInfo{
		branchToken: []byte("branch"),
		lastEventID: 3,
	}
}

func historyRequestMatches(request *persistence.ReadHistoryBranchRequest) bool {
	return request.ShardID == 1 &&
		bytes.Equal(request.BranchToken, []byte("branch")) &&
		request.MinEventID == 1 &&
		request.MaxEventID == 3 &&
		request.PageSize == 1024
}
