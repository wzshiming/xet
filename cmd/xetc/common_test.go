package main

import (
	"testing"
	"time"
)

func TestFormatTransferRate(t *testing.T) {
	if got, want := formatTransferRate(2048), "2.0 KiB/s"; got != want {
		t.Fatalf("unexpected transfer rate: got %q want %q", got, want)
	}
}

func TestSpeedTrackerUpdate(t *testing.T) {
	tracker := newSpeedTracker(time.Second)
	base := time.Unix(100, 0)

	if got := tracker.Update(0, base); got != 0 {
		t.Fatalf("expected initial rate 0, got %d", got)
	}

	if got, want := tracker.Update(1024, base.Add(500*time.Millisecond)), int64(2048); got != want {
		t.Fatalf("unexpected instantaneous rate: got %d want %d", got, want)
	}

	if got, want := tracker.Update(1536, base.Add(1500*time.Millisecond)), int64(512); got != want {
		t.Fatalf("unexpected windowed rate: got %d want %d", got, want)
	}
}

func TestFormatByteSize(t *testing.T) {
	tests := map[int64]string{
		0:               "0 B",
		1023:            "1023 B",
		1024:            "1.0 KiB",
		5 * 1024:        "5.0 KiB",
		2 * 1024 * 1024: "2.0 MiB",
	}

	for input, want := range tests {
		if got := formatByteSize(input); got != want {
			t.Fatalf("formatByteSize(%d): got %q want %q", input, got, want)
		}
	}
}
