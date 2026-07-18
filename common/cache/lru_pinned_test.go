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
	cache := New(2, &Options{Pin: true})
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
}

func TestPinnedCacheScansAfterCurrentSizeAdditionOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	maxSize := maxInt - 1
	cache := New(maxSize, &Options{Pin: true})
	pinned := &testEntryWithCacheSize{cacheSize: 2}
	largeSize := maxSize - 1

	_, err := cache.PutIfNotExist("pinned", pinned)
	require.NoError(t, err)
	_, err = cache.PutIfNotExist("first-evictable", &testEntryWithCacheSize{cacheSize: 1})
	require.NoError(t, err)
	cache.Release("first-evictable")
	for _, key := range []string{"large-1", "large-2"} {
		_, err = cache.PutIfNotExist(key, &testEntryWithCacheSize{cacheSize: largeSize})
		require.NoError(t, err)
		cache.Release(key)
	}
	_, err = cache.PutIfNotExist("tail", &testEntryWithCacheSize{cacheSize: 5})
	require.NoError(t, err)
	cache.Release("tail")

	// Adding large-1 overflows currSize while the transient pinned aggregate
	// reaches maxInt exactly. The released positive entries then wrap currSize
	// back to the pinned aggregate, but the first evictable entry must remain
	// discoverable through the original scan.
	newEntry := &testEntryWithCacheSize{cacheSize: largeSize}
	_, err = cache.PutIfNotExist("new", newEntry)
	require.NoError(t, err)
	require.Nil(t, cache.Get("first-evictable"))
	require.Same(t, newEntry, cache.Get("new"))
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

func TestPinnedCacheScansAfterRefCountWrapBecomesEvictable(t *testing.T) {
	var evicted []any
	cache := New(1, &Options{
		Pin: true,
		OnEvict: func(value any) {
			evicted = append(evicted, value)
		},
	})
	old := &testEntryWithCacheSize{cacheSize: 1}
	_, err := cache.PutIfNotExist("old", old)
	require.NoError(t, err)

	lru := cache.(*lru)
	entry := lru.byKey["old"].Value.(*entryImpl)
	// Reaching this state through Get calls would require a full signed-int
	// ref-count wrap. Set the reachable post-wrap boundary directly: old remains
	// counted in pinnedSize, but the next Get makes it evictable at refCount zero.
	entry.refCount = -1

	require.Same(t, old, cache.Get("old"))
	newEntry := &testEntryWithCacheSize{cacheSize: 1}
	_, err = cache.PutIfNotExist("new", newEntry)
	require.NoError(t, err)
	require.Nil(t, cache.Get("old"))
	require.Same(t, newEntry, cache.Get("new"))
	require.Equal(t, []any{old}, evicted)
}
