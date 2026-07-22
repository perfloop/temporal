package startworkflow

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/persistence"
)

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
