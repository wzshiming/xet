// Package pins refcounts the keys in-flight writers depend on so a sweep pass spares them.
package pins

import "sync"

// Table refcounts pinned keys; a key pinned at any instant of a pass is held until the last running pass ends. The zero value is ready and a nil Table pins nothing.
type Table[K comparable] struct {
	mu     sync.Mutex
	refs   map[K]int
	passes int
	seen   map[K]struct{} // every key pinned at any instant of the running passes
}

// New returns a ready table; the zero value is ready too.
func New[K comparable]() *Table[K] {
	return &Table[K]{}
}

// Pin keeps k pinned until unpin, which is idempotent, runs.
func (t *Table[K]) Pin(k K) (unpin func()) {
	if t == nil {
		return func() {}
	}
	t.mu.Lock()
	if t.refs == nil {
		t.refs = map[K]int{}
	}
	t.refs[k]++
	if t.passes > 0 {
		t.seen[k] = struct{}{}
	}
	t.mu.Unlock()
	return sync.OnceFunc(func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.refs[k]--
		if t.refs[k] == 0 {
			delete(t.refs, k)
		}
	})
}

// Pinned reports whether k is pinned now.
func (t *Table[K]) Pinned(k K) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.refs[k] > 0
}

// BeginPass starts a pass; passes nest, and a key pinned now or during any of them is held until the last ends.
func (t *Table[K]) BeginPass() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.passes == 0 {
		t.seen = make(map[K]struct{}, len(t.refs))
		for k := range t.refs {
			t.seen[k] = struct{}{}
		}
	}
	t.passes++
}

// EndPass ends a pass BeginPass started; once the last ends, only pinned keys stay held.
func (t *Table[K]) EndPass() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// Like sync.WaitGroup's negative counter, an unbalanced call is a programmer error.
	if t.passes == 0 {
		panic("pins: EndPass without BeginPass")
	}
	t.passes--
	if t.passes == 0 {
		t.seen = nil
	}
}

// Held reports whether k is pinned now or was at any instant of the running passes.
func (t *Table[K]) Held(k K) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	_, seen := t.seen[k]
	return seen || t.refs[k] > 0
}

// DeleteUnlessHeld runs del unless k is held, under the lock so no Pin lands between the check and the delete.
func (t *Table[K]) DeleteUnlessHeld(k K, del func() error) (held bool, err error) {
	if t == nil {
		return false, del()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, seen := t.seen[k]; seen || t.refs[k] > 0 {
		return true, nil
	}
	return false, del()
}
