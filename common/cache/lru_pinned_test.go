package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPinnedCacheEvictsLeastRecentlyUsedUnpinnedEntry(t *testing.T) {
	t.Parallel()

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

func TestPinnedCacheTracksEvictableEntries(t *testing.T) {
	t.Parallel()

	cache := New(2, &Options{Pin: true}).(*lru)
	_, err := cache.PutIfNotExist("A", "A")
	require.NoError(t, err)
	require.Zero(t, cache.evictableEntryCount)

	cache.Release("A")
	require.Equal(t, 1, cache.evictableEntryCount)

	require.Equal(t, "A", cache.Get("A"))
	require.Zero(t, cache.evictableEntryCount)

	cache.Release("A")
	require.Equal(t, 1, cache.evictableEntryCount)

	cache.Delete("A")
	require.Zero(t, cache.evictableEntryCount)
}
