package cache

import "testing"

// BenchmarkPinnedDeleteBeforeReleaseLifecycleControlParallel4_128000 measures
// deletion of a still-pinned entry at the same four-worker, 128,000-entry
// shape as the full-cache benchmark. One slot per worker remains free so each
// timed insertion succeeds and does not exercise the full-pinned rejection.
func BenchmarkPinnedDeleteBeforeReleaseLifecycleControlParallel4_128000(b *testing.B) {
	benchmarkPinnedDeleteBeforeReleaseParallel4(b, 128_000)
}

func benchmarkPinnedDeleteBeforeReleaseParallel4(b *testing.B, cacheSize int) {
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
		value := workerID

		for pb.Next() {
			value += workerCount
			actual, err := cache.PutIfNotExist(key, value)
			if err != nil || actual != value {
				b.Errorf("insert pinned cache entry at key %d: value=%v err=%v", key, actual, err)
				return
			}
			cache.Delete(key)
		}
	})
}
