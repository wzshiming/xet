package storage

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/wzshiming/xet"
)

// DefaultUploadGrantTTL is how long an issued grant accepts its upload.
const DefaultUploadGrantTTL = time.Hour

// GrantEntry is what an issued grant expects of its upload.
type GrantEntry struct {
	XorbHash  xet.XorbHash
	Length    int64
	Checksum  uint64 // CRC-64/NVME of the serialized xorb
	ExpiresAt time.Time
}

// GrantRegistry keeps the upload grants one process issued; they do not survive a restart.
type GrantRegistry struct {
	mu      sync.Mutex
	entries map[string]GrantEntry
	ttl     time.Duration
	now     func() time.Time // nil means time.Now
}

// NewGrantRegistry returns a registry whose grants live for ttl; a non-positive
// ttl means DefaultUploadGrantTTL.
func NewGrantRegistry(ttl time.Duration) *GrantRegistry {
	if ttl <= 0 {
		ttl = DefaultUploadGrantTTL
	}
	return &GrantRegistry{entries: map[string]GrantEntry{}, ttl: ttl}
}

func (r *GrantRegistry) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

// NewGrantID returns a fresh random id of 32 lowercase hex characters.
func NewGrantID() string {
	var raw [16]byte
	rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}

// Issue records a grant under a fresh NewGrantID, purging the entries that
// expired more than one TTL ago.
func (r *GrantRegistry) Issue(xorbHash xet.XorbHash, length int64, checksum uint64) string {
	id := NewGrantID()

	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	for old, entry := range r.entries {
		if now.Sub(entry.ExpiresAt) > r.ttl {
			delete(r.entries, old)
		}
	}
	r.entries[id] = GrantEntry{XorbHash: xorbHash, Length: length, Checksum: checksum, ExpiresAt: now.Add(r.ttl)}
	return id
}

// Lookup returns the grant issued as id: ErrUploadGrantNotFound when it is
// unknown, ErrUploadGrantExpired once ExpiresAt is reached.
func (r *GrantRegistry) Lookup(id string) (GrantEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[id]
	if !ok {
		return GrantEntry{}, ErrUploadGrantNotFound
	}
	if !r.clock().Before(entry.ExpiresAt) {
		return GrantEntry{}, ErrUploadGrantExpired
	}
	return entry, nil
}

// Remove forgets the grant issued as id.
func (r *GrantRegistry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, id)
}
