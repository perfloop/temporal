package persistence

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

const (
	rawHistoryBenchmarkPageSize = 10
	rawHistoryBenchmarkBlobSize = 1024
)

var rawHistoryBenchmarkSink int

type rawHistoryPage struct {
	blobs         []*commonpb.DataBlob
	nextPageToken []byte
}

type rawHistoryReadRequest struct {
	pageSize      int
	nextPageToken []byte
}

type scriptedRawHistoryExecutionManager struct {
	ExecutionManager
	pages    map[string]rawHistoryPage
	requests []rawHistoryReadRequest
}

func (m *scriptedRawHistoryExecutionManager) ReadRawHistoryBranch(
	_ context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {
	page, ok := m.pages[string(request.NextPageToken)]
	if !ok {
		return nil, fmt.Errorf("unexpected raw history continuation token %q", request.NextPageToken)
	}

	m.requests = append(m.requests, rawHistoryReadRequest{
		pageSize:      request.PageSize,
		nextPageToken: append([]byte(nil), request.NextPageToken...),
	})
	return &ReadRawHistoryBranchResponse{
		HistoryEventBlobs: page.blobs,
		NextPageToken:     append([]byte(nil), page.nextPageToken...),
		Size:              rawHistoryBlobsSize(page.blobs),
	}, nil
}

type continuationRawHistoryExecutionManager struct {
	ExecutionManager
	firstPage        []*commonpb.DataBlob
	continuationPage []*commonpb.DataBlob
}

func (m *continuationRawHistoryExecutionManager) ReadRawHistoryBranch(
	_ context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {
	if len(request.NextPageToken) == 0 {
		return &ReadRawHistoryBranchResponse{
			HistoryEventBlobs: m.firstPage,
			NextPageToken:     []byte("continuation"),
			Size:              rawHistoryBlobsSize(m.firstPage),
		}, nil
	}

	blobs := m.continuationPage[:request.PageSize]
	return &ReadRawHistoryBranchResponse{
		HistoryEventBlobs: blobs,
		NextPageToken:     []byte("after-page"),
		Size:              rawHistoryBlobsSize(blobs),
	}, nil
}

func TestReadFullPageRawEventsPreservesPaginationSemantics(t *testing.T) {
	t.Parallel()

	firstPage := rawHistoryBlobs(3, 1, 1)
	secondPage := rawHistoryBlobs(7, 4, 1)
	middlePage := rawHistoryBlobs(4, 4, 1)
	thirdPage := rawHistoryBlobs(3, 11, 1)
	atomicPage := rawHistoryBlobs(8, 4, 1)

	tests := []struct {
		name           string
		pages          map[string]rawHistoryPage
		expectedBlobs  []*commonpb.DataBlob
		expectedToken  []byte
		expectedTokens [][]byte
	}{
		{
			name: "fills page across continuation",
			pages: map[string]rawHistoryPage{
				"": {
					blobs:         firstPage,
					nextPageToken: []byte("first"),
				},
				"first": {
					blobs:         secondPage,
					nextPageToken: []byte("after-page"),
				},
			},
			expectedBlobs:  append(append([]*commonpb.DataBlob(nil), firstPage...), secondPage...),
			expectedToken:  []byte("after-page"),
			expectedTokens: [][]byte{nil, []byte("first")},
		},
		{
			name: "retains atomic batch overshoot",
			pages: map[string]rawHistoryPage{
				"": {
					blobs:         firstPage,
					nextPageToken: []byte("atomic"),
				},
				"atomic": {
					blobs:         atomicPage,
					nextPageToken: []byte("after-atomic"),
				},
			},
			expectedBlobs:  append(append([]*commonpb.DataBlob(nil), firstPage...), atomicPage...),
			expectedToken:  []byte("after-atomic"),
			expectedTokens: [][]byte{nil, []byte("atomic")},
		},
		{
			name: "continues until original target is filled",
			pages: map[string]rawHistoryPage{
				"": {
					blobs:         firstPage,
					nextPageToken: []byte("first"),
				},
				"first": {
					blobs:         middlePage,
					nextPageToken: []byte("second"),
				},
				"second": {
					blobs:         thirdPage,
					nextPageToken: []byte("after-page"),
				},
			},
			expectedBlobs: append(
				append(append([]*commonpb.DataBlob(nil), firstPage...), middlePage...),
				thirdPage...,
			),
			expectedToken:  []byte("after-page"),
			expectedTokens: [][]byte{nil, []byte("first"), []byte("second")},
		},
		{
			name: "returns terminal short page",
			pages: map[string]rawHistoryPage{
				"": {
					blobs: firstPage,
				},
			},
			expectedBlobs:  firstPage,
			expectedTokens: [][]byte{nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			manager := &scriptedRawHistoryExecutionManager{pages: tt.pages}
			request := &ReadHistoryBranchRequest{PageSize: rawHistoryBenchmarkPageSize}
			blobs, size, nextPageToken, err := ReadFullPageRawEvents(context.Background(), manager, request)

			require.NoError(t, err)
			require.Equal(t, tt.expectedBlobs, blobs)
			require.Equal(t, rawHistoryBlobsSize(tt.expectedBlobs), size)
			require.Equal(t, tt.expectedToken, nextPageToken)
			require.Equal(t, rawHistoryBenchmarkPageSize, request.PageSize)
			require.Len(t, manager.requests, len(tt.expectedTokens))
			for index, expectedToken := range tt.expectedTokens {
				require.Equal(t, expectedToken, manager.requests[index].nextPageToken)
				require.Greater(t, manager.requests[index].pageSize, 0)
				require.LessOrEqual(t, manager.requests[index].pageSize, rawHistoryBenchmarkPageSize)
			}
		})
	}
}

func BenchmarkReadFullPageRawEventsContinuation(b *testing.B) {
	firstPage := rawHistoryBlobs(3, 1, rawHistoryBenchmarkBlobSize)
	continuationPage := rawHistoryBlobs(rawHistoryBenchmarkPageSize, 4, rawHistoryBenchmarkBlobSize)
	manager := &continuationRawHistoryExecutionManager{
		firstPage:        firstPage,
		continuationPage: continuationPage,
	}

	var consumedBytes int
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		blobs, size, nextPageToken, err := ReadFullPageRawEvents(
			context.Background(),
			manager,
			&ReadHistoryBranchRequest{PageSize: rawHistoryBenchmarkPageSize},
		)
		if err != nil {
			b.Fatal(err)
		}
		if len(blobs) < rawHistoryBenchmarkPageSize {
			b.Fatalf("got %d blobs, want at least %d", len(blobs), rawHistoryBenchmarkPageSize)
		}

		consumedBytes += size + len(nextPageToken)
		for _, blob := range blobs {
			for dataIndex, value := range blob.Data {
				consumedBytes += int(value) + index%len(blob.Data) + dataIndex%len(blob.Data)
			}
		}
	}
	b.StopTimer()

	rawHistoryBenchmarkSink = consumedBytes
}

func rawHistoryBlobs(count int, firstByte byte, size int) []*commonpb.DataBlob {
	blobs := make([]*commonpb.DataBlob, count)
	for index := range blobs {
		data := make([]byte, size)
		for dataIndex := range data {
			data[dataIndex] = firstByte + byte(index) + byte(dataIndex)
		}
		blobs[index] = &commonpb.DataBlob{Data: data}
	}
	return blobs
}

func rawHistoryBlobsSize(blobs []*commonpb.DataBlob) int {
	size := 0
	for _, blob := range blobs {
		size += len(blob.Data)
	}
	return size
}
