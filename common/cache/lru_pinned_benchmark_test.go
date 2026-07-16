package cache

import (
	"fmt"
	"runtime"
	"testing"
)

const pinnedFullInsertHostLimit = 128_000

func BenchmarkPinnedFullInsert(b *testing.B) {
	for _, entries := range []int{1_000, 10_000, 100_000, pinnedFullInsertHostLimit} {
		b.Run(fmt.Sprintf("entries_%d", entries), func(b *testing.B) {
			benchmarkPinnedFullInsert(b, entries)
		})
	}
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
