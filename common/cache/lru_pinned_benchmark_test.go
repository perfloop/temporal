package cache

import (
	"fmt"
	"testing"
)

func BenchmarkPinnedFullInsert(b *testing.B) {
	for _, entries := range []int{1_000, 10_000, 100_000, 128_000} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			benchmarkPinnedFullInsertWithPreparedCache(b, newFullPinnedCacheForBenchmark(b, entries), entries)
		})
	}
}
