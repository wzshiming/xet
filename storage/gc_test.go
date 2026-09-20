package storage

import (
	"testing"
	"time"
)

// walks compare mtimes against the cutoff truncated to whole seconds. For
// every sub-second cutoff phase, an object written at or after the raw
// cutoff must still read in-grace when its mtime is reported at S3's second
// precision — truncation may only widen the shield, never shrink it.
func TestObjectCutoffTruncationInvariant(t *testing.T) {
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	for phaseMs := 0; phaseMs < 1000; phaseMs += 25 {
		cutoff := base.Add(time.Duration(phaseMs) * time.Millisecond)
		objCutoff := cutoff.Truncate(time.Second)
		for deltaMs := 0; deltaMs < 2500; deltaMs += 25 {
			event := cutoff.Add(time.Duration(deltaMs) * time.Millisecond)
			reported := event.Truncate(time.Second)
			if reported.Before(objCutoff) {
				t.Fatalf("object at cutoff+%dms (phase %dms) reads dead: reported %v < objCutoff %v",
					deltaMs, phaseMs, reported, objCutoff)
			}
		}
	}
}
