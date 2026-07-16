package persistence

import (
	"context"
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
)

// instrumentedPageSizeAwareRawHistoryStore exposes the response work done at
// the ExecutionManager-to-store boundary for the filtered continuation shape.
type instrumentedPageSizeAwareRawHistoryStore struct {
	*pageSizeAwareRawHistoryStore
	calls        int
	fetchedBytes int
}

func (s *instrumentedPageSizeAwareRawHistoryStore) ReadHistoryBranch(
	ctx context.Context,
	request *InternalReadHistoryBranchRequest,
) (*InternalReadHistoryBranchResponse, error) {
	response, err := s.pageSizeAwareRawHistoryStore.ReadHistoryBranch(ctx, request)
	if err != nil {
		return nil, err
	}

	s.calls++
	for _, node := range response.Nodes {
		s.fetchedBytes += len(node.Events.Data)
	}
	return response, nil
}

func newInstrumentedPageSizeAwareRawHistoryExecutionManager(
	b *testing.B,
	nodes []InternalHistoryNode,
	firstPageSize int,
) (ExecutionManager, []byte, *instrumentedPageSizeAwareRawHistoryStore) {
	b.Helper()

	serializer := serialization.NewSerializer()
	historyBranchUtil := NewHistoryBranchUtil(serializer)
	branchID := "instrumented-branch-id"
	branchToken, err := historyBranchUtil.NewHistoryBranch(
		"namespace-id",
		"workflow-id",
		"run-id",
		"tree-id",
		&branchID,
		nil,
		0,
		0,
		0,
	)
	if err != nil {
		b.Fatal(err)
	}

	store := &instrumentedPageSizeAwareRawHistoryStore{
		pageSizeAwareRawHistoryStore: &pageSizeAwareRawHistoryStore{
			historyBranchUtil: historyBranchUtil,
			nodes:             nodes,
			firstPageSize:     firstPageSize,
		},
	}
	return NewExecutionManager(
		store,
		serializer,
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	), branchToken, store
}

func BenchmarkReadFullPageRawEventsFilteredContinuationBoundaryMetrics(b *testing.B) {
	nodes := rawHistoryNodes(3, 1, rawHistoryBenchmarkBlobSize)
	nodes = append(nodes,
		InternalHistoryNode{
			NodeID:        3,
			TransactionID: 0,
			Events:        rawHistoryBlobs(1, 4, rawHistoryBenchmarkBlobSize)[0],
		},
		InternalHistoryNode{
			NodeID:        3,
			TransactionID: 0,
			Events:        rawHistoryBlobs(1, 5, rawHistoryBenchmarkBlobSize)[0],
		},
	)
	nodes = append(nodes, rawHistoryNodes(8, 4, rawHistoryBenchmarkBlobSize)...)
	manager, branchToken, store := newInstrumentedPageSizeAwareRawHistoryExecutionManager(b, nodes, 3)

	var consumed, totalCalls, totalFetchedBytes, totalReturnedBytes int
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		store.calls = 0
		store.fetchedBytes = 0
		blobs, size, nextPageToken, err := ReadFullPageRawEvents(
			context.Background(),
			manager,
			&ReadHistoryBranchRequest{
				BranchToken: branchToken,
				MinEventID:  1,
				MaxEventID:  100,
				PageSize:    rawHistoryBenchmarkPageSize,
			},
		)
		if err != nil {
			b.Fatal(err)
		}
		if len(blobs) < rawHistoryBenchmarkPageSize {
			b.Fatalf("got %d blobs, want at least %d", len(blobs), rawHistoryBenchmarkPageSize)
		}

		totalCalls += store.calls
		totalFetchedBytes += store.fetchedBytes
		totalReturnedBytes += size
		consumed += size + len(nextPageToken)
		for _, blob := range blobs {
			consumed += len(blob.Data) + int(blob.Data[(index+consumed)%len(blob.Data)])
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(totalCalls)/float64(b.N), "store_calls/op")
	b.ReportMetric(float64(totalFetchedBytes)/float64(b.N), "fetched_bytes/op")
	b.ReportMetric(float64(totalReturnedBytes)/float64(b.N), "returned_bytes/op")
	rawHistoryBenchmarkSink = consumed
}
