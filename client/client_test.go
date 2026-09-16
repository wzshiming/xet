package client

import (
	"slices"
	"testing"

	"github.com/wzshiming/xet/progress"
)

func TestWithProgressObserver(t *testing.T) {
	var calls []string
	record := func(label string) progress.ProgressFunc {
		return func(string, int64, int64) { calls = append(calls, label) }
	}
	for _, tc := range []struct {
		name string
		opts []Options
		want []string
	}{
		{"observer alone", []Options{WithProgressObserver(record("a"))}, []string{"a"}},
		{"observer after func", []Options{WithProgressFunc(record("f")), WithProgressObserver(record("a"))}, []string{"f", "a"}},
		{"observers chain", []Options{WithProgressFunc(record("f")), WithProgressObserver(record("a")), WithProgressObserver(record("b"))}, []string{"f", "a", "b"}},
		{"nil observer keeps func", []Options{WithProgressFunc(record("f")), WithProgressObserver(nil)}, []string{"f"}},
		{"nil observer alone", []Options{WithProgressObserver(nil)}, nil},
		{"func replaces observers", []Options{WithProgressFunc(record("f")), WithProgressObserver(record("a")), WithProgressFunc(record("g"))}, []string{"g"}},
		{"func replaces func", []Options{WithProgressFunc(record("f")), WithProgressFunc(record("g"))}, []string{"g"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls = nil
			c, err := NewClient(tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			if c.progressFunc == nil {
				if tc.want != nil {
					t.Fatalf("progressFunc is nil, want calls %q", tc.want)
				}
				return
			}
			c.progressFunc("x", 1, 2)
			if !slices.Equal(calls, tc.want) {
				t.Fatalf("calls = %q, want %q", calls, tc.want)
			}
		})
	}
}

// Options applied to a copy of a client, as the mirror does per fetch attempt, leave the original's callback alone.
func TestWithProgressObserverOnCopy(t *testing.T) {
	var calls []string
	record := func(label string) progress.ProgressFunc {
		return func(string, int64, int64) { calls = append(calls, label) }
	}
	c, err := NewClient(WithProgressFunc(record("f")))
	if err != nil {
		t.Fatal(err)
	}
	xc := *c
	WithProgressObserver(record("a"))(&xc)
	xc.progressFunc("x", 1, 2)
	c.progressFunc("x", 1, 2)
	if want := []string{"f", "a", "f"}; !slices.Equal(calls, want) {
		t.Fatalf("calls = %q, want %q", calls, want)
	}
}
