package cache

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func BenchmarkPinnedFullInsert(b *testing.B) {
	for _, entries := range []int{1_000, 10_000, 100_000, 128_000} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			benchmarkPinnedFullInsertParallel(b, entries)
		})
	}
}

func benchmarkPinnedFullInsertParallel(b *testing.B, entries int) {
	cache := New(entries, &Options{Pin: true})
	for key := range entries {
		_, err := cache.PutIfNotExist(key, key)
		if err != nil {
			b.Fatalf("filling pinned cache: %v", err)
		}
	}

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
