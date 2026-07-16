package cache

import (
	"runtime"
	"testing"
)

const pinnedFullInsertHostLimit = 128_000

func BenchmarkPinnedFullInsert(b *testing.B) {
	b.Run("entries_128000", func(b *testing.B) {
		benchmarkPinnedFullInsert(b, pinnedFullInsertHostLimit)
	})
}

func benchmarkPinnedFullInsert(b *testing.B, entries int) {
	cache := New(entries, &Options{Pin: true})
	for key := 0; key < entries; key++ {
		_, err := cache.PutIfNotExist(key, key)
		if err != nil {
			b.Fatal(err)
		}
	}

	runtime.GC()
	b.SetParallelism(1)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := entries
		for pb.Next() {
			existing, err := cache.PutIfNotExist(key, key)
			if existing != nil || err != ErrCacheFull {
				b.Fatalf("PutIfNotExist(%d) = (%v, %v), want (nil, %v)", key, existing, err, ErrCacheFull)
			}
			key++
		}
	})
}
