package storage

import (
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/wzshiming/xet"
)

// pinnedRegistry returns a registry whose clock reads *now.
func pinnedRegistry(ttl time.Duration, now *time.Time) *GrantRegistry {
	r := NewGrantRegistry(ttl)
	r.now = func() time.Time { return *now }
	return r
}

func TestGrantRegistryExpiresAtBoundary(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	r := pinnedRegistry(time.Minute, &now)
	hash := xet.XorbHash{7}
	id := r.Issue(hash, 1234, 0xc0ffee)
	expiresAt := now.Add(time.Minute)

	now = expiresAt.Add(-time.Nanosecond)
	got, err := r.Lookup(id)
	if err != nil || got.XorbHash != hash || got.Length != 1234 || got.Checksum != 0xc0ffee || !got.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("Lookup 1ns before expiry = %+v, %v; want the issued entry expiring at %v", got, err, expiresAt)
	}
	now = expiresAt
	if _, err := r.Lookup(id); !errors.Is(err, ErrUploadGrantExpired) {
		t.Fatalf("Lookup at ExpiresAt = %v, want ErrUploadGrantExpired", err)
	}
}

func TestGrantRegistryPurgesAfterOneExtraTTL(t *testing.T) {
	const ttl = time.Minute
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	r := pinnedRegistry(ttl, &now)
	id := r.Issue(xet.XorbHash{1}, 1, 1)
	expiresAt := now.Add(ttl)

	// Purging is lazy, on Issue.
	now = expiresAt.Add(ttl - time.Nanosecond)
	r.Issue(xet.XorbHash{2}, 1, 1)
	if _, err := r.Lookup(id); !errors.Is(err, ErrUploadGrantExpired) {
		t.Fatalf("Lookup %v past expiry = %v, want ErrUploadGrantExpired (kept)", ttl-time.Nanosecond, err)
	}
	now = expiresAt.Add(ttl + time.Nanosecond)
	r.Issue(xet.XorbHash{3}, 1, 1)
	if _, err := r.Lookup(id); !errors.Is(err, ErrUploadGrantNotFound) {
		t.Fatalf("Lookup %v past expiry = %v, want ErrUploadGrantNotFound (purged)", ttl+time.Nanosecond, err)
	}
}

func TestGrantRegistryIssuesRandomHexIDs(t *testing.T) {
	hexID := regexp.MustCompile(`^[0-9a-f]{32}$`)
	r := NewGrantRegistry(time.Minute)
	ids := map[string]bool{}
	for range 1000 {
		id := r.Issue(xet.XorbHash{}, 0, 0)
		if !hexID.MatchString(id) {
			t.Fatalf("Issue() = %q, want 32 lowercase hex characters", id)
		}
		if ids[id] {
			t.Fatalf("Issue() repeated %q", id)
		}
		ids[id] = true
	}
	// Sequential or clock-derived ids would leave some position constant.
	for i := range 32 {
		seen := map[byte]bool{}
		for id := range ids {
			seen[id[i]] = true
		}
		if len(seen) == 1 {
			t.Fatalf("every issued id has the same character at position %d, want random ids", i)
		}
	}
}

func TestNewGrantID(t *testing.T) {
	hexID := regexp.MustCompile(`^[0-9a-f]{32}$`)
	ids := make(map[string]bool, 10000)
	for range 10000 {
		id := NewGrantID()
		if !hexID.MatchString(id) {
			t.Fatalf("NewGrantID() = %q, want 32 lowercase hex characters", id)
		}
		if ids[id] {
			t.Fatalf("NewGrantID() repeated %q", id)
		}
		ids[id] = true
	}
	r := NewGrantRegistry(time.Minute)
	for range 100 {
		if id := r.Issue(xet.XorbHash{}, 0, 0); !ValidGrantID(id) {
			t.Fatalf("Issue() = %q, want an id ValidGrantID accepts", id)
		}
	}
}

func TestGrantRegistryDefaultTTL(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Minute} {
		now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
		r := pinnedRegistry(ttl, &now)
		got, err := r.Lookup(r.Issue(xet.XorbHash{}, 0, 0))
		if want := now.Add(DefaultUploadGrantTTL); err != nil || !got.ExpiresAt.Equal(want) {
			t.Fatalf("NewGrantRegistry(%v) entry expires at %v, %v; want %v", ttl, got.ExpiresAt, err, want)
		}
	}
}

func TestGrantRegistryLookupUnknownOrRemoved(t *testing.T) {
	r := NewGrantRegistry(time.Minute)
	removed := r.Issue(xet.XorbHash{}, 0, 0)
	r.Remove(removed)
	for _, id := range []string{removed, "0123456789abcdef0123456789abcdef"} {
		if _, err := r.Lookup(id); !errors.Is(err, ErrUploadGrantNotFound) {
			t.Fatalf("Lookup(%q) = %v, want ErrUploadGrantNotFound", id, err)
		}
	}
}

func TestValidGrantID(t *testing.T) {
	for _, c := range []struct {
		id   string
		want bool
	}{
		{"0123456789abcdef0123456789abcdef", true},
		{"0123456789ABCDEF0123456789ABCDEF", false},
		{"0123456789abcdef0123456789abcde", false},
		{"0123456789abcdef0123456789abcdef0", false},
		{"0123456789abcdef0123456789abcdeg", false},
		{"../../../../../../../../../../ab", false},
		{"", false},
	} {
		if got := ValidGrantID(c.id); got != c.want {
			t.Errorf("ValidGrantID(%q) = %v, want %v", c.id, got, c.want)
		}
	}
}

func TestXorbUploadPath(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef"
	if got, want := XorbUploadPath(id), "/v1/xorb-uploads/"+id; got != want {
		t.Fatalf("XorbUploadPath(%q) = %q, want %q", id, got, want)
	}
}
