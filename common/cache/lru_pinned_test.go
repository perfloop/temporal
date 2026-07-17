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

	_, err := cache.PutIfNotExist("D", "D")
	require.ErrorIs(t, err, ErrCacheFull)

	// Move A to the most-recent position, then make A and C evictable. B remains
	// pinned at the least-recent position. C is the least-recent evictable entry,
	// so a new pinned entry must evict C rather than A.
	require.Equal(t, "A", cache.Get("A"))
	cache.Release("A")
	cache.Release("A")
	cache.Release("C")

	_, err = cache.PutIfNotExist("D", "D")
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
