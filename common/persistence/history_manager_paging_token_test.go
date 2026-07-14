package persistence_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.uber.org/mock/gomock"
)

func TestHistoryManagerReadHistoryBranchReverseTraversesAncestorRanges(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	store := mock.NewMockExecutionStore(ctrl)
	serializer := serialization.NewSerializer()
	branchUtil := p.NewHistoryBranchUtil(serializer)
	store.EXPECT().GetHistoryBranchUtil().AnyTimes().Return(branchUtil)

	ancestorBranchID := uuid.NewString()
	currentBranchID := uuid.NewString()
	branchToken, err := branchUtil.NewHistoryBranch(
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		&currentBranchID,
		[]*persistencespb.HistoryBranchRange{{
			BranchId:    ancestorBranchID,
			BeginNodeId: 1,
			EndNodeId:   4,
		}},
		0,
		0,
		0,
	)
	require.NoError(t, err)

	currentEvents, err := serializer.SerializeEvents([]*historypb.HistoryEvent{{EventId: 4, Version: 1}})
	require.NoError(t, err)
	ancestorEvents, err := serializer.SerializeEvents([]*historypb.HistoryEvent{
		{EventId: 1, Version: 1},
		{EventId: 2, Version: 1},
		{EventId: 3, Version: 1},
	})
	require.NoError(t, err)

	var readBranchIDs []string
	store.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *p.InternalReadHistoryBranchRequest) (*p.InternalReadHistoryBranchResponse, error) {
			readBranchIDs = append(readBranchIDs, request.BranchID)
			switch request.BranchID {
			case currentBranchID:
				return &p.InternalReadHistoryBranchResponse{Nodes: []p.InternalHistoryNode{{
					NodeID:            4,
					Events:            currentEvents,
					PrevTransactionID: 3,
					TransactionID:     4,
				}}}, nil
			case ancestorBranchID:
				return &p.InternalReadHistoryBranchResponse{Nodes: []p.InternalHistoryNode{{
					NodeID:        1,
					Events:        ancestorEvents,
					TransactionID: 3,
				}}}, nil
			default:
				t.Fatalf("unexpected branch ID: %q", request.BranchID)
				return nil, nil
			}
		},
	).Times(2)
	executionManager := p.NewExecutionManager(
		store,
		serializer,
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)

	request := &p.ReadHistoryBranchReverseRequest{
		ShardID:     1,
		BranchToken: branchToken,
		MaxEventID:  5,
		PageSize:    1,
	}
	response, err := executionManager.ReadHistoryBranchReverse(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, response.HistoryEvents, 1)
	require.EqualValues(t, 4, response.HistoryEvents[0].GetEventId())
	require.NotEmpty(t, response.NextPageToken)

	request.NextPageToken = response.NextPageToken
	response, err = executionManager.ReadHistoryBranchReverse(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, response.HistoryEvents, 3)
	require.EqualValues(t, 3, response.HistoryEvents[0].GetEventId())
	require.EqualValues(t, 2, response.HistoryEvents[1].GetEventId())
	require.EqualValues(t, 1, response.HistoryEvents[2].GetEventId())
	require.Empty(t, response.NextPageToken)
	require.Equal(t, []string{currentBranchID, ancestorBranchID}, readBranchIDs)
}

func TestHistoryManagerInvalidHistoryPagingTokenReturnsError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	store := mock.NewMockExecutionStore(ctrl)
	serializer := serialization.NewSerializer()
	branchUtil := p.NewHistoryBranchUtil(serializer)
	store.EXPECT().GetHistoryBranchUtil().AnyTimes().Return(branchUtil)
	executionManager := p.NewExecutionManager(
		store,
		serializer,
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)
	branchToken, err := branchUtil.NewHistoryBranch(
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
	require.NoError(t, err)

	for name, nextPageToken := range map[string][]byte{
		"malformed":    []byte("{"),
		"out of range": []byte(`{"CurrentRangeIndex":2147483647,"FinalRangeIndex":2147483647}`),
	} {
		t.Run(name, func(t *testing.T) {
			var readErr error
			require.NotPanics(t, func() {
				_, readErr = executionManager.ReadHistoryBranch(context.Background(), &p.ReadHistoryBranchRequest{
					ShardID:       1,
					BranchToken:   branchToken,
					MinEventID:    1,
					MaxEventID:    3,
					PageSize:      1,
					NextPageToken: nextPageToken,
				})
			})
			require.Error(t, readErr)
		})
	}
}
