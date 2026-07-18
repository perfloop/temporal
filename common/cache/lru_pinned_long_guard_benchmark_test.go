package cache

import "testing"

// BenchmarkPinnedFullHitReleaseLong repeats the existing parallel hit/release
// workload with a longer per-sample duration for the adjacent-path guard.
func BenchmarkPinnedFullHitReleaseLong(b *testing.B) {
	b.Run("entries=10000", func(b *testing.B) {
		benchmarkPinnedFullHitReleaseParallel(b, 10_000)
	})
	b.Run("entries=128000", func(b *testing.B) {
		benchmarkPinnedFullHitReleaseParallel(b, 128_000)
	})
}
