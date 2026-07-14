package persistence_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.uber.org/mock/gomock"
)

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
