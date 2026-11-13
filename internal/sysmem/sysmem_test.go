//go:build darwin || linux || windows

package sysmem

import "testing"

func TestUsable(t *testing.T) {
	bytes, ok := Usable()
	if !ok {
		t.Fatal("Usable reported no size")
	}
	if bytes < 256<<20 || bytes > 64<<40 {
		t.Fatalf("Usable = %d bytes; want between 256 MiB and 64 TiB", bytes)
	}
	t.Logf("Usable = %d bytes", bytes)
}
