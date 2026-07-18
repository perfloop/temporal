package cache

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const pinnedFullInsertHostCacheCapacity = 128_000

func TestPinnedCacheEvictsReleasedEntriesInLRUOrder(t *testing.T) {
	cache := New(3, &Options{Pin: true})
	for _, key := range []string{"A", "B", "C"} {
		_, err := cache.PutIfNotExist(key, key)
		require.NoError(t, err)
	}

	// The access order is C, B, A. Keep B pinned while A and C are evictable.
	cache.Release("A")
	cache.Release("C")
	_, err := cache.PutIfNotExist("D", "D")
	require.NoError(t, err)
	require.Nil(t, cache.Get("A"))

	// B becomes evictable at the back of the access order. Inserting E must
	// evict B rather than the more recently used released entry C.
	cache.Release("B")
	_, err = cache.PutIfNotExist("E", "E")
	require.NoError(t, err)
	require.Nil(t, cache.Get("B"))
	require.Equal(t, "C", cache.Get("C"))
}

func TestPinnedCacheConcurrentFullInsert(t *testing.T) {
	cache := New(8, &Options{Pin: true})
	for key := range 8 {
		_, err := cache.PutIfNotExist(key, key)
		require.NoError(t, err)
	}

	const (
		workers    = 8
		iterations = 100
	)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := worker * iterations; key < (worker+1)*iterations; key++ {
				if _, err := cache.PutIfNotExist(8+key, key); err != ErrCacheFull {
					t.Errorf("PutIfNotExist() error = %v, want %v", err, ErrCacheFull)
					return
				}
			}
		}()
	}
	wg.Wait()

	require.Equal(t, 8, cache.Size())
}

func BenchmarkPinnedFullInsertAtCapacity(b *testing.B) {
	for _, capacity := range []int{1_000, 10_000, 100_000, pinnedFullInsertHostCacheCapacity} {
		b.Run(strconv.Itoa(capacity), func(b *testing.B) {
			benchmarkPinnedFullInsert(b, capacity)
		})
	}
}

func BenchmarkPinnedFullInsertAtHostLimitParallel(b *testing.B) {
	cache := newFullyPinnedCacheForBenchmark(b, pinnedFullInsertHostCacheCapacity)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		key := pinnedFullInsertHostCacheCapacity
		for pb.Next() {
			key++
			if _, err := cache.PutIfNotExist(key, key); err != ErrCacheFull {
				b.Fatalf("PutIfNotExist() error = %v, want %v", err, ErrCacheFull)
			}
		}
	})
}

func benchmarkPinnedFullInsert(b *testing.B, capacity int) {
	cache := newFullyPinnedCacheForBenchmark(b, capacity)
	key := capacity

	for b.Loop() {
		key++
		if _, err := cache.PutIfNotExist(key, key); err != ErrCacheFull {
			b.Fatalf("PutIfNotExist() error = %v, want %v", err, ErrCacheFull)
		}
	}
}

func BenchmarkPinnedCacheHitRelease(b *testing.B) {
	cache := newFullyPinnedCacheForBenchmark(b, 1)
	const key = 0
	cache.Release(key)

	for b.Loop() {
		if value := cache.Get(key); value != key {
			b.Fatalf("Get() = %v, want %v", value, key)
		}
		cache.Release(key)
	}
}

func newFullyPinnedCacheForBenchmark(b *testing.B, capacity int) Cache {
	b.Helper()

	cache := New(capacity, &Options{Pin: true})
	for key := range capacity {
		if _, err := cache.PutIfNotExist(key, key); err != nil {
			b.Fatalf("PutIfNotExist() during setup error = %v", err)
		}
	}
	return cache
}
