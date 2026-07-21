package cache

import "testing"

func BenchmarkPinnedCacheFullInsert(b *testing.B) {
	benchmarkPinnedCacheFullInsert(b, 128_000)
}

func benchmarkPinnedCacheFullInsert(b *testing.B, cacheSize int) {
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

	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := uint64(cacheSize)
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
