package startworkflow

import (
	"bytes"
	"context"
	"errors"
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func BenchmarkStarterGetWorkflowHistoryDuplicatePageRequestControl(b *testing.B) {
	duplicatePageRequests := 0
	for range b.N {
		ctrl := gomock.NewController(b)
		executionManager := persistence.NewMockExecutionManager(ctrl)
		shardContext := historyi.NewMockShardContext(ctrl)
		shardContext.EXPECT().GetExecutionManager().Return(executionManager).AnyTimes()
		shardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
		starter := &Starter{shardContext: shardContext}
		continuationToken := []byte("next-page")
		errDuplicatePageRequest := errors.New("duplicate history page request")
		calls := 0

		executionManager.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, request *persistence.ReadHistoryBranchRequest) (*persistence.ReadHistoryBranchResponse, error) {
				calls++
				switch calls {
				case 1:
					if len(request.NextPageToken) != 0 {
						b.Fatalf("first request token = %q, want empty", request.NextPageToken)
					}
					return &persistence.ReadHistoryBranchResponse{
						HistoryEvents: []*historypb.HistoryEvent{{EventId: 1}},
						NextPageToken: continuationToken,
					}, nil
				case 2:
					if len(request.NextPageToken) == 0 {
						return nil, errDuplicatePageRequest
					}
					if !bytes.Equal(request.NextPageToken, continuationToken) {
						b.Fatalf("second request token = %q, want %q", request.NextPageToken, continuationToken)
					}
					return &persistence.ReadHistoryBranchResponse{
						HistoryEvents: []*historypb.HistoryEvent{{EventId: 2}},
					}, nil
				default:
					b.Fatalf("ReadHistoryBranch calls = %d, want 2", calls)
					return nil, nil
				}
			},
		).AnyTimes()

		events, err := starter.getWorkflowHistory(context.Background(), &mutableStateInfo{
			branchToken: []byte("branch"),
			lastEventID: 3,
		})
		ctrl.Finish()
		if errors.Is(err, errDuplicatePageRequest) {
			duplicatePageRequests++
			continue
		}
		if err != nil {
			b.Fatalf("getWorkflowHistory returned error: %v", err)
		}
		if len(events) != 2 || events[0].GetEventId() != 1 || events[1].GetEventId() != 2 {
			b.Fatalf("getWorkflowHistory returned events: %#v", events)
		}
	}
	b.ReportMetric(float64(duplicatePageRequests)/float64(b.N), "duplicate_page_requests/op")
}
