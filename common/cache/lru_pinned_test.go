package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPinnedCacheEvictsLeastRecentlyUsedUnpinnedEntry(t *testing.T) {
	cache := New(3, &Options{Pin: true})
	for _, key := range []string{"A", "B", "C"} {
		_, err := cache.PutIfNotExist(key, key)
		require.NoError(t, err)
	}

	// Move A to the most-recent position, then make A and C evictable. B remains
	// pinned at the least-recent position. C is the least-recent evictable entry,
	// so a new pinned entry must evict C rather than A.
	require.Equal(t, "A", cache.Get("A"))
	cache.Release("A")
	cache.Release("A")
	cache.Release("C")

	_, err := cache.PutIfNotExist("D", "D")
	require.NoError(t, err)
	require.Equal(t, 3, cache.Size())
	require.Nil(t, cache.Get("C"))
	require.Equal(t, "A", cache.Get("A"))
	require.Equal(t, "B", cache.Get("B"))
	require.Equal(t, "D", cache.Get("D"))
}

func TestPinnedCacheReleaseAndGetRestoreFullCache(t *testing.T) {
	cache := New(2, &Options{Pin: true})
	for _, key := range []string{"A", "B"} {
		_, err := cache.PutIfNotExist(key, key)
		require.NoError(t, err)
	}

	// Reacquiring the released entry restores a full cache with no evictable
	// entries, so admission must still fail.
	cache.Release("A")
	require.Equal(t, "A", cache.Get("A"))
	_, err := cache.PutIfNotExist("C", "C")
	require.ErrorIs(t, err, ErrCacheFull)

	// Releasing A again makes it evictable. It is now the least-recent
	// evictable entry because B has remained pinned, so C can replace A.
	cache.Release("A")
	_, err = cache.PutIfNotExist("C", "C")
	require.NoError(t, err)
	require.Nil(t, cache.Get("A"))
	require.Equal(t, "B", cache.Get("B"))
	require.Equal(t, "C", cache.Get("C"))
}

func TestPinnedCacheReleaseUnderflowCanBecomeEvictable(t *testing.T) {
	cache := New(1, &Options{Pin: true})
	_, err := cache.PutIfNotExist("A", "A")
	require.NoError(t, err)

	// Release does not reject unbalanced calls. Preserve the existing behavior where
	// a later Get can bring a negative reference count back to zero and make A evictable.
	cache.Release("A")
	cache.Release("A")
	require.Equal(t, "A", cache.Get("A"))

	_, err = cache.PutIfNotExist("B", "B")
	require.NoError(t, err)
	require.Nil(t, cache.Get("A"))
	require.Equal(t, "B", cache.Get("B"))
}

func TestPinnedCacheEvictsZeroSizeEntryBeforeReturningFull(t *testing.T) {
	cache := New(1, &Options{Pin: true})
	pinned := &testEntryWithCacheSize{cacheSize: 1}
	zeroSize := &testEntryWithCacheSize{}

	_, err := cache.PutIfNotExist("pinned", pinned)
	require.NoError(t, err)
	_, err = cache.PutIfNotExist("zero-size", zeroSize)
	require.NoError(t, err)
	cache.Release("zero-size")

	_, err = cache.PutIfNotExist("new", &testEntryWithCacheSize{cacheSize: 1})
	require.ErrorIs(t, err, ErrCacheFull)
	require.Nil(t, cache.Get("zero-size"))
	require.Same(t, pinned, cache.Get("pinned"))
}

func TestPinnedCacheEvictsEntryThatBecomesZeroSizeOnRelease(t *testing.T) {
	cache := New(1, &Options{Pin: true})
	resized := &testEntryWithCacheSize{cacheSize: 1}

	_, err := cache.PutIfNotExist("resized", resized)
	require.NoError(t, err)
	resized.cacheSize = 0
	cache.Release("resized")

	_, err = cache.PutIfNotExist("pinned", &testEntryWithCacheSize{cacheSize: 1})
	require.NoError(t, err)
	_, err = cache.PutIfNotExist("new", &testEntryWithCacheSize{cacheSize: 1})
	require.ErrorIs(t, err, ErrCacheFull)
	require.Nil(t, cache.Get("resized"))
}

func TestPinnedCacheEvictsAfterPinnedEntryShrinks(t *testing.T) {
	cache := New(8, &Options{Pin: true})
	resized := &testEntryWithCacheSize{cacheSize: 6}

	_, err := cache.PutIfNotExist("resized", resized)
	require.NoError(t, err)
	_, err = cache.PutIfNotExist("evictable", &testEntryWithCacheSize{cacheSize: 2})
	require.NoError(t, err)
	cache.Release("evictable")
	require.Same(t, resized, cache.Get("resized"))

	resized.cacheSize = 4
	cache.Release("resized")
	_, err = cache.PutIfNotExist("new", &testEntryWithCacheSize{cacheSize: 3})
	require.NoError(t, err)
	require.Nil(t, cache.Get("evictable"))
}

func TestPinnedCacheEvictsAfterDeletingPinnedEntry(t *testing.T) {
	cache := New(4, &Options{Pin: true})

	_, err := cache.PutIfNotExist("deleted", &testEntryWithCacheSize{cacheSize: 2})
	require.NoError(t, err)
	_, err = cache.PutIfNotExist("evictable", &testEntryWithCacheSize{cacheSize: 2})
	require.NoError(t, err)
	cache.Release("evictable")
	cache.Delete("deleted")

	_, err = cache.PutIfNotExist("new", &testEntryWithCacheSize{cacheSize: 3})
	require.NoError(t, err)
	require.Nil(t, cache.Get("evictable"))
}
