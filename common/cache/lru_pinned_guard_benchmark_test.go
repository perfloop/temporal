package cache

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func BenchmarkPinnedFullHitRelease(b *testing.B) {
	for _, entries := range []int{1_000, 10_000, 100_000, 128_000} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			benchmarkPinnedFullHitReleaseParallel(b, entries)
		})
	}
}

func BenchmarkPinnedFullInsertWithLRUEvictable(b *testing.B) {
	for _, entries := range []int{1_000, 10_000, 100_000, 128_000} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			benchmarkPinnedFullInsertWithEvictable(b, entries, false)
		})
	}
}

func BenchmarkPinnedFullInsertWithMRUEvictable(b *testing.B) {
	for _, entries := range []int{1_000, 10_000, 100_000, 128_000} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			benchmarkPinnedFullInsertWithEvictable(b, entries, true)
		})
	}
}

func benchmarkPinnedFullHitReleaseParallel(b *testing.B, entries int) {
	cache := newFullPinnedCacheForBenchmark(b, entries)
	for key := range entries {
		cache.Release(key)
	}

	var workerID atomic.Uint64
	var unexpectedResult atomic.Bool

	b.SetParallelism(1)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := int(workerID.Add(1)-1) % entries
		for pb.Next() {
			if cache.Get(key) != key {
				unexpectedResult.Store(true)
				continue
			}
			cache.Release(key)
		}
	})
	b.StopTimer()

	if unexpectedResult.Load() {
		b.Fatal("pinned cache hit did not return its stored value")
	}
}

// benchmarkPinnedFullInsertWithEvictable measures an insert after a Release.
// When evictableAtMRU is false, each iteration releases the next LRU entry
// while the timer is stopped. Otherwise it releases the newly inserted MRU entry.
func benchmarkPinnedFullInsertWithEvictable(b *testing.B, entries int, evictableAtMRU bool) {
	cache := newFullPinnedCacheForBenchmark(b, entries)
	if evictableAtMRU {
		if cache.Get(0) != 0 {
			b.Fatal("pinned cache did not return its stored value")
		}
		cache.Release(0)
	}
	cache.Release(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := entries + i
		if _, err := cache.PutIfNotExist(key, key); err != nil {
			b.Fatalf("inserting with an evictable entry: %v", err)
		}

		b.StopTimer()
		if evictableAtMRU {
			cache.Release(key)
		} else {
			cache.Release(i + 1)
		}
		b.StartTimer()
	}
	b.StopTimer()
}

func newFullPinnedCacheForBenchmark(b *testing.B, entries int) Cache {
	b.Helper()

	cache := New(entries, &Options{Pin: true})
	for key := range entries {
		_, err := cache.PutIfNotExist(key, key)
		if err != nil {
			b.Fatalf("filling pinned cache: %v", err)
		}
	}
	return cache
}
