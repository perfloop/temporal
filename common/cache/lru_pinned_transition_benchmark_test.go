package cache

import (
	"sync/atomic"
	"testing"
)

func benchmarkPinnedFullInsertWithPreparedCache(b *testing.B, cache Cache, entries int) {
	var workerID atomic.Uint64
	var unexpectedResult atomic.Bool

	b.SetParallelism(1)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := uint64(entries) + workerID.Add(1)<<32
		for pb.Next() {
			if _, err := cache.PutIfNotExist(key, key); err != ErrCacheFull {
				unexpectedResult.Store(true)
			}
			key++
		}
	})
	b.StopTimer()

	if unexpectedResult.Load() {
		b.Fatal("full pinned cache did not return ErrCacheFull")
	}
}

func BenchmarkPinnedFullInsertAfterReleaseGet(b *testing.B) {
	const entries = 128_000

	cache := newFullPinnedCacheForBenchmark(b, entries)
	cache.Release(0)
	if cache.Get(0) != 0 {
		b.Fatal("pinned cache did not return its stored value after release")
	}

	benchmarkPinnedFullInsertWithPreparedCache(b, cache, entries)
}
