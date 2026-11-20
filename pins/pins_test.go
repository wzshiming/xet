package pins_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pins"
)

func TestTableNew(t *testing.T) {
	p, other := pins.New[xet.XorbHash](), pins.New[xet.XorbHash]()
	h := xet.XorbHash{1}
	unpin := p.Pin(h)
	if !p.Pinned(h) {
		t.Fatal("Pinned() = false on a New table holding a pin, want a ready table")
	}
	if other.Pinned(h) {
		t.Fatal("Pinned() = true on another New table, want each New table to hold only its own pins")
	}
	unpin()
	if p.Pinned(h) {
		t.Fatal("Pinned() = true on a New table after its unpin")
	}
}

// A key stays pinned until every pin taken on it is released.
func TestTableCountsEachPin(t *testing.T) {
	var p pins.Table[xet.XorbHash]
	h := xet.XorbHash{1}
	unpinFirst, unpinSecond := p.Pin(h), p.Pin(h)
	unpinFirst()
	if !p.Pinned(h) {
		t.Fatal("Pinned() = false with one of two pins still held")
	}
	unpinSecond()
	if p.Pinned(h) {
		t.Fatal("Pinned() = true after both pins were released")
	}
}

// A repeated unpin releases nothing more than its own pin.
func TestTableUnpinIsIdempotent(t *testing.T) {
	var p pins.Table[xet.XorbHash]
	h := xet.XorbHash{1}
	unpin, _ := p.Pin(h), p.Pin(h)
	unpin()
	unpin()
	if !p.Pinned(h) {
		t.Fatal("Pinned() = false after one of two pins was unpinned twice: the repeat released the other pin")
	}
}

// A nil table pins and holds nothing, even within a pass, and deletes whatever is asked.
func TestTableNilPinsNothing(t *testing.T) {
	var p *pins.Table[xet.XorbHash]
	h := xet.XorbHash{1}
	p.BeginPass()
	unpin := p.Pin(h)
	if unpin == nil {
		t.Fatal("Pin() on a nil table returned a nil unpin")
	}
	if pinned, held := p.Pinned(h), p.Held(h); pinned || held {
		t.Fatalf("Pinned(), Held() on a nil table = %v, %v; want false, false", pinned, held)
	}
	errDel := errors.New("del failed")
	calls := 0
	held, err := p.DeleteUnlessHeld(h, func() error {
		calls++
		return errDel
	})
	if held || calls != 1 || !errors.Is(err, errDel) {
		t.Fatalf("DeleteUnlessHeld() on a nil table = %v, %v with %d del calls; want false, the del error, 1 call", held, err, calls)
	}
	unpin()
	p.EndPass()
}

// A key pinned when a pass begins stays held until the pass ends, though unpinned during it.
func TestTablePassHoldsKeyPinnedAtBegin(t *testing.T) {
	var p pins.Table[xet.XorbHash]
	h := xet.XorbHash{1}
	unpin := p.Pin(h)
	p.BeginPass()
	unpin()
	if !p.Held(h) {
		t.Fatal("Held() = false after an unpin during the pass; want held until the pass ends")
	}
	p.EndPass()
	if p.Held(h) {
		t.Fatal("Held() = true after the pass ended with no pin left")
	}
}

// A key pinned and released during a pass stays held until the pass ends.
func TestTablePassHoldsKeyPinnedDuringIt(t *testing.T) {
	var p pins.Table[xet.XorbHash]
	h := xet.XorbHash{1}
	p.BeginPass()
	p.Pin(h)()
	if !p.Held(h) {
		t.Fatal("Held() = false for a key pinned and released during the pass; want held until the pass ends")
	}
	p.EndPass()
	if p.Held(h) {
		t.Fatal("Held() = true after the pass ended with no pin left")
	}
}

// Passes nest: a key pinned when the first began or at any instant of any of them is held until the last ends.
func TestTablePassesNest(t *testing.T) {
	var p pins.Table[xet.XorbHash]
	before, outer, inner := xet.XorbHash{1}, xet.XorbHash{2}, xet.XorbHash{3}
	unpinBefore := p.Pin(before)
	p.BeginPass()
	p.Pin(outer)()
	p.BeginPass()
	p.Pin(inner)()
	unpinBefore()
	p.EndPass()
	if b, o, i := p.Held(before), p.Held(outer), p.Held(inner); !b || !o || !i {
		t.Fatalf("Held() once the inner pass ended = %v, %v, %v for the keys pinned before both passes, in the outer only, in the inner; want all held until the outer pass ends", b, o, i)
	}
	p.EndPass()
	if b, o, i := p.Held(before), p.Held(outer), p.Held(inner); b || o || i {
		t.Fatalf("Held() once both passes ended with no pin left = %v, %v, %v; want false for every key", b, o, i)
	}
}

func TestTableEndPassWithoutBeginPanics(t *testing.T) {
	const want = "pins: EndPass without BeginPass"
	var p pins.Table[xet.XorbHash]
	defer func() {
		if r := recover(); r != want {
			t.Fatalf("EndPass() without BeginPass recovered %v, want panic %q", r, want)
		}
	}()
	p.EndPass()
}

// DeleteUnlessHeld never runs del for a pinned key.
func TestTableDeleteUnlessHeldSparesPinnedKey(t *testing.T) {
	var p pins.Table[xet.XorbHash]
	h := xet.XorbHash{1}
	p.Pin(h)
	held, err := p.DeleteUnlessHeld(h, func() error {
		t.Error("del ran for a pinned key")
		return nil
	})
	if !held || err != nil {
		t.Fatalf("DeleteUnlessHeld() = %v, %v; want true, nil", held, err)
	}
}

// DeleteUnlessHeld spares a string key pinned and released during the pass until the pass ends.
func TestTableDeleteUnlessHeldSparesKeySeenInPass(t *testing.T) {
	var p pins.Table[string]
	k := "a"
	p.BeginPass()
	p.Pin(k)()
	calls := 0
	del := func() error {
		calls++
		return nil
	}
	if held, err := p.DeleteUnlessHeld(k, del); !held || err != nil || calls != 0 {
		t.Fatalf("DeleteUnlessHeld() during the pass = %v, %v with %d del calls; want true, nil, 0: the key was pinned in it", held, err, calls)
	}
	p.EndPass()
	if held, err := p.DeleteUnlessHeld(k, del); held || err != nil || calls != 1 {
		t.Fatalf("DeleteUnlessHeld() after the pass = %v, %v with %d del calls; want false, nil, 1", held, err, calls)
	}
}

// DeleteUnlessHeld runs del for a key nobody holds, though another is pinned, and returns its error.
func TestTableDeleteUnlessHeldDeletesFreeKey(t *testing.T) {
	var p pins.Table[xet.XorbHash]
	p.Pin(xet.XorbHash{2})
	errDel := errors.New("del failed")
	calls := 0
	held, err := p.DeleteUnlessHeld(xet.XorbHash{1}, func() error {
		calls++
		return errDel
	})
	if held || calls != 1 || !errors.Is(err, errDel) {
		t.Fatalf("DeleteUnlessHeld() = %v, %v with %d del calls; want false, the del error, 1 call", held, err, calls)
	}
}

// A Pin issued while DeleteUnlessHeld runs del on its key returns only after del returned.
func TestTablePinWaitsForDeleteInProgress(t *testing.T) {
	var p pins.Table[xet.XorbHash]
	h := xet.XorbHash{1}
	entered, unblock, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(release)
	var delReturned atomic.Bool
	go func() {
		defer close(returned)
		_, _ = p.DeleteUnlessHeld(h, func() error {
			close(entered)
			<-unblock
			delReturned.Store(true)
			return nil
		})
	}()
	select {
	case <-entered:
	case <-returned:
		t.Fatal("DeleteUnlessHeld() returned without running del")
	}

	pinned := make(chan bool, 1)
	go func() {
		unpin := p.Pin(h)
		pinned <- delReturned.Load()
		unpin()
	}()
	select {
	case <-pinned:
		t.Fatal("Pin() returned while del of its key was still running")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case afterDel := <-pinned:
		if !afterDel {
			t.Fatal("Pin() returned before del did")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Pin() still blocked 10s after del returned")
	}
}

// Pins, unpins and checks from many goroutines beside passes stay consistent and leave nothing held.
func TestTableConcurrentUse(t *testing.T) {
	var p pins.Table[xet.XorbHash]
	hashes := []xet.XorbHash{{1}, {2}}
	var wg sync.WaitGroup
	for g := range 8 {
		h := hashes[g%len(hashes)]
		wg.Go(func() {
			for range 1000 {
				unpin := p.Pin(h)
				pinned := p.Pinned(h)
				unpin()
				if !pinned {
					t.Error("Pinned() = false while a pin on the key was held")
					return
				}
			}
		})
	}
	wg.Go(func() {
		for range 100 {
			p.BeginPass()
			for _, h := range hashes {
				_, _ = p.DeleteUnlessHeld(h, func() error { return nil })
			}
			p.EndPass()
		}
	})
	wg.Wait()
	for _, h := range hashes {
		if p.Pinned(h) || p.Held(h) {
			t.Errorf("key %s still held after every unpin and pass", h)
		}
	}
}
