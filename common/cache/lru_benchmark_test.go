package cache

import (
	"sync/atomic"
	"testing"
)

// BenchmarkPinnedCacheFullFailedNewKeyInsert measures rejected new-key inserts
// when all resident entries in a capacity-sized pinned cache are in use.
func BenchmarkPinnedCacheFullFailedNewKeyInsert(b *testing.B) {
	const cacheSize = 128_000

	cache := New(cacheSize, &Options{Pin: true})
	for key := range cacheSize {
		value, err := cache.PutIfNotExist(key, key)
		if err != nil || value != key {
			b.Fatalf("fill pinned cache at key %d: value=%v err=%v", key, value, err)
		}
	}
	if cache.Size() != cacheSize {
		b.Fatalf("filled cache size = %d, want %d", cache.Size(), cacheSize)
	}

	var workerID atomic.Uint64
	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := (workerID.Add(1) << 32) + cacheSize
		for pb.Next() {
			key++
			value, err := cache.PutIfNotExist(key, key)
			if value != nil || err != ErrCacheFull {
				b.Errorf("full pinned insert for key %d: value=%v err=%v, want nil and ErrCacheFull", key, value, err)
				return
			}
		}
	})
}
