package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestPinnedCacheEvictsAfterNegativeSizeUnderflowBecomesEvictable(t *testing.T) {
	var evicted []any
	cache := New(2, &Options{
		Pin: true,
		OnEvict: func(value any) {
			evicted = append(evicted, value)
		},
	})
	x := &testEntryWithCacheSize{cacheSize: 1}
	b := &testEntryWithCacheSize{}
	a := &testEntryWithCacheSize{cacheSize: 2}
	c := &testEntryWithCacheSize{cacheSize: 1}

	_, err := cache.PutIfNotExist("X", x)
	require.NoError(t, err)
	cache.Release("X")
	_, err = cache.PutIfNotExist("B", b)
	require.NoError(t, err)

	// An unbalanced Release can refresh B to a negative size while its reference
	// count is negative. A later Get restores B to the eviction predicate's zero
	// count, so the aggregate equality must not reject C before scanning X and B.
	b.cacheSize = 1
	cache.Release("B")
	b.cacheSize = -1
	cache.Release("B")
	require.Same(t, b, cache.Get("B"))

	_, err = cache.PutIfNotExist("A", a)
	require.NoError(t, err)
	_, err = cache.PutIfNotExist("C", c)
	require.NoError(t, err)
	require.Nil(t, cache.Get("X"))
	require.Same(t, a, cache.Get("A"))
	require.Same(t, c, cache.Get("C"))
	require.Equal(t, []any{x}, evicted)
}

func TestPinnedCacheScansAfterSizeAccountingOverflow(t *testing.T) {
	entrySize := int(^uint(0)>>1)/2 + 1
	var evicted []any
	cache := New(entrySize, &Options{
		Pin: true,
		OnEvict: func(value any) {
			evicted = append(evicted, value)
		},
	})
	pinned := &testEntryWithCacheSize{cacheSize: entrySize}
	firstEvictable := &testEntryWithCacheSize{cacheSize: entrySize}

	_, err := cache.PutIfNotExist(-1, pinned)
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		_, err = cache.PutIfNotExist(i*2, &testEntryWithCacheSize{cacheSize: -entrySize})
		require.NoError(t, err)
		evictable := &testEntryWithCacheSize{cacheSize: entrySize}
		if i == 0 {
			evictable = firstEvictable
		}
		_, err = cache.PutIfNotExist(i*2+1, evictable)
		require.NoError(t, err)
		cache.Release(i*2 + 1)
	}
	require.Empty(t, evicted)

	// The positive evictable entries wrap the signed aggregate back to the
	// pinned aggregate. The original scan must still evict the first one.
	_, err = cache.PutIfNotExist(99, &testEntryWithCacheSize{cacheSize: 1})
	require.NoError(t, err)
	require.Nil(t, cache.Get(1))
	require.Equal(t, []any{firstEvictable}, evicted)
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
