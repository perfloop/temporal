package cache

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	pinnedFullCacheBenchmark1K        = 1_000
	pinnedFullCacheBenchmark10K       = 10_000
	pinnedFullCacheBenchmark100K      = 100_000
	pinnedFullCacheBenchmarkHostLimit = 128_000
)

var pinnedFullCacheBenchmarkValue = struct{}{}

func TestPinnedCacheEvictsLeastRecentlyUsedUnpinnedEntry(t *testing.T) {
	t.Parallel()

	cache := New(3, &Options{Pin: true})
	for _, key := range []string{"A", "B", "C"} {
		_, err := cache.PutIfNotExist(key, key)
		require.NoError(t, err)
	}

	// B becomes evictable before A, but A was accessed earlier. Eviction must
	// still follow access order rather than release order.
	cache.Release("B")
	cache.Release("A")
	_, err := cache.PutIfNotExist("D", "D")
	require.NoError(t, err)

	require.Nil(t, cache.Get("A"))
	require.Equal(t, "B", cache.Get("B"))
	require.Equal(t, "C", cache.Get("C"))
	require.Equal(t, "D", cache.Get("D"))
}

func BenchmarkPinnedFullCacheInsertAt1K(b *testing.B) {
	benchmarkPinnedFullCacheInsert(b, pinnedFullCacheBenchmark1K)
}

func BenchmarkPinnedFullCacheInsertAt10K(b *testing.B) {
	benchmarkPinnedFullCacheInsert(b, pinnedFullCacheBenchmark10K)
}

func BenchmarkPinnedFullCacheInsertAt100K(b *testing.B) {
	benchmarkPinnedFullCacheInsert(b, pinnedFullCacheBenchmark100K)
}

func BenchmarkPinnedFullCacheInsertAtHostLimit(b *testing.B) {
	benchmarkPinnedFullCacheInsert(b, pinnedFullCacheBenchmarkHostLimit)
}

func benchmarkPinnedFullCacheInsert(b *testing.B, cacheSize int) {
	cache := New(cacheSize, &Options{Pin: true})
	for key := range cacheSize {
		value, err := cache.PutIfNotExist(key, pinnedFullCacheBenchmarkValue)
		if err != nil || value != pinnedFullCacheBenchmarkValue {
			b.Fatalf("filling pinned cache: value=%v err=%v", value, err)
		}
	}

	var nextMissKey uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := cacheSize + int(atomic.AddUint64(&nextMissKey, 1))
			value, err := cache.PutIfNotExist(key, pinnedFullCacheBenchmarkValue)
			if err != ErrCacheFull || value != nil {
				b.Fatalf("full pinned cache insert: value=%v err=%v", value, err)
			}
		}
	})
}
