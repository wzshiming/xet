package lru

import (
	"slices"
	"sync"
	"testing"
)

// TestEvictionPastCapacity fills the cache beyond MaxEntries; eviction runs
// inside Add/GetOrNew while the lock is held and must not self-deadlock.
func TestEvictionPastCapacity(t *testing.T) {
	c := New[int, int](2)
	for i := range 5 {
		c.Add(i, i)
	}
	if _, ok := c.Get(0); ok {
		t.Fatal("oldest entry should have been evicted")
	}
	if v, ok := c.Get(4); !ok || v != 4 {
		t.Fatalf("newest entry missing: %v, %v", v, ok)
	}

	c = New[int, int](2)
	for i := range 5 {
		c.GetOrNew(i, func() (int, bool) { return i, true })
	}
	if _, ok := c.Get(0); ok {
		t.Fatal("oldest entry should have been evicted")
	}
	if v, ok := c.Get(4); !ok || v != 4 {
		t.Fatalf("newest entry missing: %v, %v", v, ok)
	}
}

// drain pops every entry oldest first.
func drain[K comparable, V any](c *Cache[K, V]) []K {
	var keys []K
	for {
		k, _, ok := c.RemoveOldest()
		if !ok {
			return keys
		}
		keys = append(keys, k)
	}
}

func TestRecencyOrder(t *testing.T) {
	c := New[string, int](0)
	c.Add("a", 1)
	c.Add("b", 2)
	c.Add("c", 3)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("Get a: miss")
	}
	if got := drain(c); !slices.Equal(got, []string{"b", "c", "a"}) {
		t.Fatalf("order after Get = %v, want [b c a]", got)
	}
	if c.Len() != 0 {
		t.Fatalf("Len after drain = %d, want 0", c.Len())
	}
}

func TestAddExistingReplacesAndRefreshes(t *testing.T) {
	evicted := 0
	c := New[string, int](3)
	c.OnEvicted = func(string, int) { evicted++ }
	c.Add("a", 1)
	c.Add("b", 2)
	c.Add("c", 3)
	c.Add("a", 10)
	if c.Len() != 3 || evicted != 0 {
		t.Fatalf("Len = %d, evicted = %d after replace; want 3, 0", c.Len(), evicted)
	}
	k, _, ok := c.RemoveOldest()
	if !ok || k != "b" {
		t.Fatalf("oldest after replace = %q (%v), want b", k, ok)
	}
	if got := drain(c); !slices.Equal(got, []string{"c", "a"}) {
		t.Fatalf("rest = %v, want [c a]", got)
	}
	c.Add("a", 1)
	c.Add("a", 10)
	if v, ok := c.Remove("a"); !ok || v != 10 {
		t.Fatalf("replaced value = %d, %v; want 10", v, ok)
	}
	if v, ok := c.Get("a"); ok || v != 0 {
		t.Fatalf("Get after remove = %d, %v; want zero miss", v, ok)
	}
}

func TestLoweredMaxEntriesAppliesOnNextAdd(t *testing.T) {
	c := New[int, int](0)
	for i := range 3 {
		c.Add(i, i)
	}
	c.MaxEntries = 2
	c.Add(0, 0) // replace
	if got := drain(c); !slices.Equal(got, []int{2, 0}) {
		t.Fatalf("after replace under lowered cap = %v, want [2 0]", got)
	}
	c.MaxEntries = 0
	for i := range 3 {
		c.Add(i, i)
	}
	if c.Len() != 3 {
		t.Fatalf("unbounded refill Len = %d, want 3", c.Len())
	}
	c.MaxEntries = 1
	c.GetOrNew(9, func() (int, bool) { return 9, true })
	if got := drain(c); !slices.Equal(got, []int{9}) {
		t.Fatalf("after insert under lowered cap = %v, want [9]", got)
	}
}

func TestEvictionCallsOnEvictedInLRUOrder(t *testing.T) {
	type pair struct {
		k string
		v int
	}
	var got []pair
	c := New[string, int](2)
	c.OnEvicted = func(k string, v int) { got = append(got, pair{k, v}) }
	c.Add("a", 1)
	c.Add("b", 2)
	c.Add("c", 3)
	c.Add("d", 4)
	want := []pair{{"a", 1}, {"b", 2}}
	if !slices.Equal(got, want) || c.Len() != 2 {
		t.Fatalf("evicted = %v (Len %d), want %v (Len 2)", got, c.Len(), want)
	}
}

func TestRemoveReturnsValueAndNotifies(t *testing.T) {
	var evicted []string
	c := New[string, int](0)
	c.OnEvicted = func(k string, _ int) { evicted = append(evicted, k) }
	c.Add("a", 1)
	if v, ok := c.Remove("a"); !ok || v != 1 {
		t.Fatalf("Remove a = %d, %v; want 1, true", v, ok)
	}
	if v, ok := c.Remove("a"); ok || v != 0 {
		t.Fatalf("second Remove a = %d, %v; want zero miss", v, ok)
	}
	if !slices.Equal(evicted, []string{"a"}) || c.Len() != 0 {
		t.Fatalf("evicted = %v, Len = %d; want [a], 0", evicted, c.Len())
	}
	if k, v, ok := c.RemoveOldest(); ok || k != "" || v != 0 {
		t.Fatalf("RemoveOldest on empty = %q, %d, %v; want zero miss", k, v, ok)
	}
}

func TestUnboundedNeverEvicts(t *testing.T) {
	for _, max := range []int{0, -1} {
		c := New[int, int](max)
		c.OnEvicted = func(k, _ int) { t.Fatalf("MaxEntries %d evicted %d", max, k) }
		for i := range 1000 {
			c.Add(i, i)
		}
		if c.Len() != 1000 {
			t.Fatalf("MaxEntries %d: Len = %d, want 1000", max, c.Len())
		}
	}
}

func TestGetOrNew(t *testing.T) {
	c := New[string, int](0)
	calls := 0
	if v, ok := c.GetOrNew("a", func() (int, bool) { calls++; return 0, false }); ok || v != 0 || c.Len() != 0 {
		t.Fatalf("failed newFunc = %d, %v, Len %d; want zero miss and nothing stored", v, ok, c.Len())
	}
	if v, ok := c.GetOrNew("a", func() (int, bool) { calls++; return 7, true }); !ok || v != 7 {
		t.Fatalf("GetOrNew miss = %d, %v; want 7, true", v, ok)
	}
	if v, ok := c.GetOrNew("a", func() (int, bool) { calls++; return 8, true }); !ok || v != 7 {
		t.Fatalf("GetOrNew hit = %d, %v; want cached 7", v, ok)
	}
	if calls != 2 {
		t.Fatalf("newFunc calls = %d, want 2 (hit must not call it)", calls)
	}
}

func TestClear(t *testing.T) {
	var evicted []string
	c := New[string, int](0)
	c.OnEvicted = func(k string, _ int) { evicted = append(evicted, k) }
	c.Add("a", 1)
	c.Add("b", 2)
	c.Clear()
	slices.Sort(evicted)
	if !slices.Equal(evicted, []string{"a", "b"}) || c.Len() != 0 {
		t.Fatalf("evicted = %v, Len = %d; want [a b], 0", evicted, c.Len())
	}
	c.Add("c", 3)
	if v, ok := c.Get("c"); !ok || v != 3 {
		t.Fatalf("cache unusable after Clear: %d, %v", v, ok)
	}
}

func TestZeroValueUsable(t *testing.T) {
	var c Cache[string, int]
	if _, ok := c.Get("a"); ok {
		t.Fatal("zero cache hit")
	}
	if _, ok := c.Remove("a"); ok {
		t.Fatal("zero cache Remove hit")
	}
	c.Add("a", 1)
	if v, ok := c.Get("a"); !ok || v != 1 || c.Len() != 1 {
		t.Fatalf("zero cache after Add = %d, %v, Len %d", v, ok, c.Len())
	}
}

// TestConcurrentAccess hammers one cache from many goroutines; the race detector pins the lock.
func TestConcurrentAccess(t *testing.T) {
	c := New[int, int](64)
	c.OnEvicted = func(int, int) {}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 2000 {
				k := (g*2000 + i) % 100
				switch i % 5 {
				case 0:
					c.Add(k, i)
				case 1:
					c.Get(k)
				case 2:
					c.GetOrNew(k, func() (int, bool) { return i, true })
				case 3:
					c.Remove(k)
				default:
					c.RemoveOldest()
				}
			}
		}()
	}
	wg.Wait()
	if n := c.Len(); n > 64 {
		t.Fatalf("Len = %d exceeds MaxEntries 64", n)
	}
}
