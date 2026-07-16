package cache

import (
	"sync/atomic"
	"testing"
)

type pinnedEvictableBenchmarkValue struct {
	key int
}

func BenchmarkPinnedEvictableReleaseReacquireAtHostLimit(b *testing.B) {
	cache := New(pinnedFullCacheBenchmarkHostLimit, &Options{Pin: true})
	for key := range pinnedFullCacheBenchmarkHostLimit {
		value, err := cache.PutIfNotExist(key, pinnedFullCacheBenchmarkValue)
		if err != nil || value != pinnedFullCacheBenchmarkValue {
			b.Fatalf("filling pinned cache: value=%v err=%v", value, err)
		}
	}
	for key := range pinnedFullCacheBenchmarkHostLimit {
		cache.Release(key)
	}

	var nextKey uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := int(atomic.AddUint64(&nextKey, 1) % pinnedFullCacheBenchmarkHostLimit)
			if value := cache.Get(key); value != pinnedFullCacheBenchmarkValue {
				b.Fatalf("reacquiring evictable entry: value=%v", value)
			}
			cache.Release(key)
		}
	})
}

func BenchmarkPinnedEvictableInsertAtHostLimit(b *testing.B) {
	cacheSize := pinnedFullCacheBenchmarkHostLimit
	values := make([]pinnedEvictableBenchmarkValue, cacheSize+b.N)
	for key := range values {
		values[key].key = key
	}

	evictedKey := -1
	cache := New(cacheSize, &Options{
		Pin: true,
		OnEvict: func(value any) {
			evictedKey = value.(*pinnedEvictableBenchmarkValue).key
		},
	})
	for key := range cacheSize {
		value := &values[key]
		existing, err := cache.PutIfNotExist(value.key, value)
		if err != nil || existing != value {
			b.Fatalf("filling pinned cache: value=%v err=%v", existing, err)
		}
	}
	for key := range cacheSize {
		cache.Release(key)
	}

	b.ResetTimer()
	for operation := range b.N {
		value := &values[cacheSize+operation]
		evictedKey = -1
		existing, err := cache.PutIfNotExist(value.key, value)
		if err != nil || existing != value {
			b.Fatalf("inserting evictable entry: value=%v err=%v", existing, err)
		}
		if evictedKey != operation {
			b.Fatalf("evicted key = %d, want %d", evictedKey, operation)
		}
		cache.Release(value.key)
	}
}
