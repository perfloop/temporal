package persistence

import "testing"

func BenchmarkReadFullPageRawEventsContinuationBoundary(b *testing.B) {
	manager, branchToken := newPageSizeAwareRawHistoryExecutionManager(
		b,
		rawHistoryNodes(13, 1, rawHistoryBenchmarkBlobSize),
		3,
	)
	benchmarkReadFullPageRawEventsAtExecutionManagerBoundary(b, manager, branchToken)
}
