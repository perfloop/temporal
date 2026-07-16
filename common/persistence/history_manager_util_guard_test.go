package persistence

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
)

// pageSizeAwareRawHistoryStore exercises the same ExecutionManager-to-store
// boundary as production while making the persistence page limit observable.
// It constructs a fresh response payload for each call, like a store decoding
// rows into history nodes, rather than returning a prebuilt slice.
type pageSizeAwareRawHistoryStore struct {
	ExecutionStore
	historyBranchUtil HistoryBranchUtil
	nodes             []InternalHistoryNode
	firstPageSize     int
}

func (s *pageSizeAwareRawHistoryStore) GetName() string {
	return "page-size-aware-raw-history-store"
}

func (s *pageSizeAwareRawHistoryStore) GetHistoryBranchUtil() HistoryBranchUtil {
	return s.historyBranchUtil
}

func (s *pageSizeAwareRawHistoryStore) ReadHistoryBranch(
	_ context.Context,
	request *InternalReadHistoryBranchRequest,
) (*InternalReadHistoryBranchResponse, error) {
	if request.PageSize <= 0 {
		return nil, fmt.Errorf("unexpected non-positive page size %d", request.PageSize)
	}

	offset := 0
	if len(request.NextPageToken) != 0 {
		var err error
		offset, err = strconv.Atoi(string(request.NextPageToken))
		if err != nil {
			return nil, fmt.Errorf("invalid store page token %q: %w", request.NextPageToken, err)
		}
	}
	if offset > len(s.nodes) {
		return nil, fmt.Errorf("store page token offset %d exceeds %d nodes", offset, len(s.nodes))
	}

	pageSize := request.PageSize
	if offset == 0 && s.firstPageSize > 0 {
		pageSize = s.firstPageSize
	}
	end := min(offset+pageSize, len(s.nodes))
	response := &InternalReadHistoryBranchResponse{
		Nodes: cloneRawHistoryNodes(s.nodes[offset:end]),
	}
	if end < len(s.nodes) {
		response.NextPageToken = []byte(strconv.Itoa(end))
	}
	return response, nil
}

func cloneRawHistoryNodes(nodes []InternalHistoryNode) []InternalHistoryNode {
	cloned := make([]InternalHistoryNode, len(nodes))
	for index, node := range nodes {
		cloned[index] = node
		cloned[index].Events = &commonpb.DataBlob{
			Data:         append([]byte(nil), node.Events.Data...),
			EncodingType: node.Events.EncodingType,
		}
	}
	return cloned
}

func newPageSizeAwareRawHistoryExecutionManager(
	b *testing.B,
	nodes []InternalHistoryNode,
	firstPageSize int,
) (ExecutionManager, []byte) {
	b.Helper()

	serializer := serialization.NewSerializer()
	historyBranchUtil := NewHistoryBranchUtil(serializer)
	branchID := "branch-id"
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

	store := &pageSizeAwareRawHistoryStore{
		historyBranchUtil: historyBranchUtil,
		nodes:             nodes,
		firstPageSize:     firstPageSize,
	}
	return NewExecutionManager(
		store,
		serializer,
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	), branchToken
}

func BenchmarkReadFullPageRawEventsTerminalBoundary(b *testing.B) {
	manager, branchToken := newPageSizeAwareRawHistoryExecutionManager(
		b,
		rawHistoryNodes(10, 1, rawHistoryBenchmarkBlobSize),
		0,
	)
	benchmarkReadFullPageRawEventsAtExecutionManagerBoundary(b, manager, branchToken)
}

func BenchmarkReadFullPageRawEventsFilteredContinuationBoundary(b *testing.B) {
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
	manager, branchToken := newPageSizeAwareRawHistoryExecutionManager(b, nodes, 3)
	benchmarkReadFullPageRawEventsAtExecutionManagerBoundary(b, manager, branchToken)
}

func benchmarkReadFullPageRawEventsAtExecutionManagerBoundary(
	b *testing.B,
	manager ExecutionManager,
	branchToken []byte,
) {
	b.Helper()

	var consumed int
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
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

		consumed += size + len(nextPageToken)
		for _, blob := range blobs {
			consumed += len(blob.Data) + int(blob.Data[(index+consumed)%len(blob.Data)])
		}
	}
	b.StopTimer()

	rawHistoryBenchmarkSink = consumed
}

func rawHistoryNodes(count int, firstNodeID int64, blobSize int) []InternalHistoryNode {
	blobs := rawHistoryBlobs(count, byte(firstNodeID), blobSize)
	nodes := make([]InternalHistoryNode, len(blobs))
	for index, blob := range blobs {
		nodes[index] = InternalHistoryNode{
			NodeID:        firstNodeID + int64(index),
			TransactionID: 1,
			Events:        blob,
		}
	}
	return nodes
}
