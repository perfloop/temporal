package cache

import (
	"runtime"
	"testing"
)

func BenchmarkPinnedGetReleaseParallel4_128000(b *testing.B) {
	benchmarkPinnedGetReleaseParallel4(b, 128_000)
}

func BenchmarkPinnedInsertReleaseParallel4_128000(b *testing.B) {
	benchmarkPinnedInsertReleaseParallel4(b, 128_000)
}

func benchmarkPinnedGetReleaseParallel4(b *testing.B, cacheSize int) {
	workerCount, workerIDs := pinnedBenchmarkWorkerIDs()
	cache := New(cacheSize, &Options{Pin: true})
	for key := range cacheSize {
		value, err := cache.PutIfNotExist(key, key)
		if err != nil || value != key {
			b.Fatalf("fill pinned cache at key %d: value=%v err=%v", key, value, err)
		}
	}
	for workerID := range workerCount {
		cache.Release(workerID)
	}

	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerID := <-workerIDs
		defer func() { workerIDs <- workerID }()

		for pb.Next() {
			value := cache.Get(workerID)
			if value != workerID {
				b.Errorf("pinned cache hit for key %d: value=%v, want %d", workerID, value, workerID)
				return
			}
			cache.Release(workerID)
		}
	})
}

func benchmarkPinnedInsertReleaseParallel4(b *testing.B, cacheSize int) {
	workerCount, workerIDs := pinnedBenchmarkWorkerIDs()
	cache := New(cacheSize, &Options{Pin: true})
	for key := range cacheSize - workerCount {
		value, err := cache.PutIfNotExist(key, key)
		if err != nil || value != key {
			b.Fatalf("fill pinned cache at key %d: value=%v err=%v", key, value, err)
		}
	}

	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerID := <-workerIDs
		defer func() { workerIDs <- workerID }()
		key := cacheSize + workerID

		for pb.Next() {
			value, err := cache.PutIfNotExist(key, key)
			if err != nil || value != key {
				b.Errorf("insert pinned cache entry at key %d: value=%v err=%v", key, value, err)
				return
			}
			cache.Release(key)
			cache.Delete(key)
		}
	})
}

func pinnedBenchmarkWorkerIDs() (int, chan int) {
	workerCount := runtime.GOMAXPROCS(0)
	workerIDs := make(chan int, workerCount)
	for workerID := range workerCount {
		workerIDs <- workerID
	}
	return workerCount, workerIDs
}
