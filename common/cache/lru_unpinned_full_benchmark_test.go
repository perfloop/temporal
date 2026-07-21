package cache

import (
	"sync/atomic"
	"testing"
)

func BenchmarkUnpinnedFullInsertParallel4_128000(b *testing.B) {
	const cacheSize = 128_000
	cache := New(cacheSize, nil)
	for key := range cacheSize {
		value, err := cache.PutIfNotExist(key, key)
		if err != nil || value != key {
			b.Fatalf("fill unpinned cache at key %d: value=%v err=%v", key, value, err)
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
			if value != key || err != nil {
				b.Errorf("full unpinned insert for key %d: value=%v err=%v, want key and nil", key, value, err)
				return
			}
		}
	})
}
