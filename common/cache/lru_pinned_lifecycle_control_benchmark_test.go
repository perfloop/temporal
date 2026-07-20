package cache

import "testing"

// BenchmarkPinnedInsertReleaseLifecycleControlParallel4_128000 is a normal-lifecycle
// control for the same four-worker, 128,000-entry cache shape as the pinned-full
// rejection benchmark. It deliberately keeps one spare slot per worker so a timed
// PutIfNotExist succeeds; that holds the full-pinned rejection condition false while
// measuring the new-entry precheck and the corresponding pin, release, and cleanup.
func BenchmarkPinnedInsertReleaseLifecycleControlParallel4_128000(b *testing.B) {
	benchmarkPinnedInsertReleaseParallel4(b, 128_000)
}
