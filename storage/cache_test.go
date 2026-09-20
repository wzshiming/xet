package storage

import (
	"errors"
	"testing"
	"time"
)

func TestCacheEvictsLeastRecentlyUsed(t *testing.T) {
	c := NewCache[string, int](2)
	c.Add("a", 1)
	c.Add("b", 2)
	c.Get("a") // a is now more recent than b
	c.Add("c", 3)
	if n := c.Len(); n != 2 {
		t.Fatalf("Len() = %d, want 2", n)
	}
	if _, ok := c.Get("b"); ok {
		t.Fatal("least recently used entry survived beyond capacity")
	}
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %d, %v; want 1, true", v, ok)
	}
}

func TestCacheSizeBelowOneDisablesCaching(t *testing.T) {
	for _, size := range []int{0, -1} {
		c := NewCache[string, int](size)
		c.Add("a", 1)
		if n := c.Len(); n != 0 {
			t.Fatalf("NewCache(%d).Len() after Add = %d, want 0", size, n)
		}
		if _, ok := c.Get("a"); ok {
			t.Fatalf("NewCache(%d) retained an entry", size)
		}
		loads := 0
		for range 2 {
			if v, err := c.GetOrLoad("k", func() (int, error) { loads++; return 7, nil }); err != nil || v != 7 {
				t.Fatalf("NewCache(%d).GetOrLoad() = %d, %v; want 7, nil", size, v, err)
			}
		}
		if loads != 2 {
			t.Fatalf("NewCache(%d) ran the loader %d times, want 2", size, loads)
		}
		c.Remove("a")
	}
}

func TestCacheGetOrLoadDoesNotCacheErrors(t *testing.T) {
	c := NewCache[string, int](2)
	loads := 0
	fail := errors.New("load failed")
	if _, err := c.GetOrLoad("k", func() (int, error) { loads++; return 0, fail }); !errors.Is(err, fail) {
		t.Fatalf("GetOrLoad() error = %v, want %v", err, fail)
	}
	if n := c.Len(); n != 0 {
		t.Fatalf("Len() after failed load = %d, want 0", n)
	}
	v, err := c.GetOrLoad("k", func() (int, error) { loads++; return 7, nil })
	if err != nil || v != 7 {
		t.Fatalf("GetOrLoad() = %d, %v; want 7, nil", v, err)
	}
	if _, err := c.GetOrLoad("k", func() (int, error) { loads++; return 0, fail }); err != nil {
		t.Fatalf("GetOrLoad() cached hit returned %v", err)
	}
	if loads != 2 {
		t.Fatalf("loader ran %d times, want 2", loads)
	}
}

func TestCacheGetOrLoadRunsLoaderOutsideLock(t *testing.T) {
	c := NewCache[string, int](2)
	entered, release := make(chan struct{}), make(chan struct{})
	go func() {
		_, _ = c.GetOrLoad("slow", func() (int, error) {
			close(entered)
			<-release
			return 1, nil
		})
	}()
	<-entered
	defer close(release)
	added := make(chan struct{})
	go func() {
		c.Add("other", 2)
		close(added)
	}()
	select {
	case <-added:
	case <-time.After(5 * time.Second):
		t.Fatal("Add blocked while a loader was running")
	}
}
