package cache

import "testing"

func BenchmarkPinnedHitRelease(b *testing.B) {
	const (
		key   = "key"
		value = "value"
	)

	cache := New(1, &Options{Pin: true})
	_, err := cache.PutIfNotExist(key, value)
	if err != nil {
		b.Fatal(err)
	}
	cache.Release(key)

	b.ResetTimer()
	for range b.N {
		if got := cache.Get(key); got != value {
			b.Fatalf("Get(%q) = %v, want %q", key, got, value)
		}
		cache.Release(key)
	}
}
