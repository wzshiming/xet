package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/wzshiming/xet"
)

type claims struct {
	Permission Permission `json:"permission"`
	ExpiresAt  *int64     `json:"exp"`
	File       string     `json:"file,omitempty"`
	SHA256     string     `json:"sha256,omitempty"`
}

// Issuer signs and validates HMAC-SHA256 tokens carrying a Grant and verifies bindings against route targets.
type Issuer struct {
	secret []byte
	ttl    time.Duration
	now    func() time.Time
}

// NewIssuer generates a secret when omitted; non-positive ttl defaults to 15 minutes and nil now uses time.Now.
func NewIssuer(secret []byte, ttl time.Duration, now func() time.Time) (*Issuer, error) {
	if len(secret) == 0 {
		secret = make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return nil, fmt.Errorf("generate token secret: %w", err)
		}
	}
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	if now == nil {
		now = time.Now
	}
	return &Issuer{secret: secret, ttl: ttl, now: now}, nil
}

// Sign returns a token carrying g, its Unix expiry, and any signing error.
func (t *Issuer) Sign(g Grant) (token string, exp int64, err error) {
	switch g.Permission {
	case Read, Write:
	default:
		return "", 0, fmt.Errorf("unsupported permission %q", g.Permission)
	}
	exp = t.now().Add(t.ttl).Unix()
	claim := claims{
		Permission: g.Permission,
		ExpiresAt:  &exp,
	}
	if g.File != nil {
		claim.File = g.File.String()
	}
	if g.SHA256 != "" {
		digest, ok := canonicalSHA256(g.SHA256)
		if !ok {
			return "", 0, fmt.Errorf("invalid sha256 %q", g.SHA256)
		}
		claim.SHA256 = digest
	}
	payload, err := json.Marshal(claim)
	if err != nil {
		return "", exp, err
	}
	input := base64.RawURLEncoding.EncodeToString(payload)
	return input + "." + base64.RawURLEncoding.EncodeToString(t.mac(input)), exp, nil
}

func (t *Issuer) mac(input string) []byte {
	mac := hmac.New(sha256.New, t.secret)
	mac.Write([]byte(input))
	return mac.Sum(nil)
}

// canonicalSHA256 returns the lowercase form of a 64-digit hex digest.
func canonicalSHA256(digest string) (string, bool) {
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size {
		return "", false
	}
	return hex.EncodeToString(decoded), true
}

// Validate returns the Grant carried by a valid, unexpired token signed with this issuer's secret.
func (t *Issuer) Validate(token string) (Grant, bool) {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Grant{}, false
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || !hmac.Equal(signature, t.mac(parts[0])) {
		return Grant{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Grant{}, false
	}
	var claim struct {
		claims
		File   json.RawMessage `json:"file"`
		SHA256 json.RawMessage `json:"sha256"`
	}
	if err := json.Unmarshal(payload, &claim); err != nil || claim.ExpiresAt == nil || t.now().Unix() >= *claim.ExpiresAt {
		return Grant{}, false
	}
	switch claim.Permission {
	case Read, Write:
	default:
		return Grant{}, false
	}
	grant := Grant{Permission: claim.Permission}
	if claim.File != nil {
		var file string
		if err := json.Unmarshal(claim.File, &file); err != nil {
			return Grant{}, false
		}
		parsed, err := xet.ParseFileHash(file)
		if err != nil {
			return Grant{}, false
		}
		grant.File = &parsed
	}
	if claim.SHA256 != nil {
		var digest string
		if err := json.Unmarshal(claim.SHA256, &digest); err != nil {
			return Grant{}, false
		}
		canonical, ok := canonicalSHA256(digest)
		if !ok {
			return Grant{}, false
		}
		grant.SHA256 = canonical
	}
	return grant, true
}

// Authorize requires a valid signed token matching the required grant.
func (t *Issuer) Authorize(r *http.Request, required Grant) error {
	tok, ok := BearerToken(r)
	if !ok {
		return ErrUnauthenticated
	}
	granted, ok := t.Validate(tok)
	if !ok {
		return ErrUnauthenticated
	}
	if granted.Permission != required.Permission {
		return ErrForbidden
	}
	// Untargeted routes (xorbs, chunks, listing, gc) check permission only; targeted routes must match every binding the token carries.
	if required.File == nil && required.SHA256 == "" {
		return nil
	}
	if granted.File != nil && (required.File == nil || *granted.File != *required.File) {
		return ErrForbidden
	}
	if granted.SHA256 != "" && granted.SHA256 != required.SHA256 {
		return ErrForbidden
	}
	return nil
}
