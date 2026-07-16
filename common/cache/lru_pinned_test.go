package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPinnedCacheFullInsertEvictsReleasedLeastRecentlyUsed(t *testing.T) {
	t.Parallel()

	cache := New(2, &Options{Pin: true})
	_, err := cache.PutIfNotExist("a", "a")
	require.NoError(t, err)
	_, err = cache.PutIfNotExist("b", "b")
	require.NoError(t, err)

	_, err = cache.PutIfNotExist("c", "c")
	require.ErrorIs(t, err, ErrCacheFull)

	cache.Release("a")
	_, err = cache.PutIfNotExist("c", "c")
	require.NoError(t, err)
	require.Nil(t, cache.Get("a"))

	require.Equal(t, "b", cache.Get("b"))
	cache.Release("b")
	cache.Release("b")
	cache.Release("c")

	_, err = cache.PutIfNotExist("d", "d")
	require.NoError(t, err)
	require.Equal(t, "b", cache.Get("b"))
	require.Nil(t, cache.Get("c"))
	require.Equal(t, "d", cache.Get("d"))
}
