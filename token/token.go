// Package token implements short-lived HMAC-signed bearer tokens that can be
// minted and validated statelessly, e.g. as the AuthFunc of a CAS server.
package token

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const prefix = "xtk1"

// Issuer mints and validates short-lived bearer tokens signed with an HMAC
// secret.
type Issuer struct {
	secret []byte
	ttl    time.Duration
}

// NewIssuer creates an Issuer. An empty secret is replaced by a random one,
// scoping the tokens to the lifetime of the process. A non-positive ttl
// defaults to 15 minutes.
func NewIssuer(secret []byte, ttl time.Duration) (*Issuer, error) {
	if len(secret) == 0 {
		secret = make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return nil, fmt.Errorf("generate token secret: %w", err)
		}
	}
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	return &Issuer{secret: secret, ttl: ttl}, nil
}

func (t *Issuer) sign(expStr string) string {
	mac := hmac.New(sha256.New, t.secret)
	mac.Write([]byte(prefix + "." + expStr))
	return hex.EncodeToString(mac.Sum(nil))
}

// Mint returns a bearer token and its expiry as a Unix timestamp.
func (t *Issuer) Mint(now time.Time) (string, int64) {
	exp := now.Add(t.ttl).Unix()
	expStr := strconv.FormatInt(exp, 10)
	return prefix + "." + expStr + "." + t.sign(expStr), exp
}

// Validate reports whether token was minted by this issuer and is not expired.
func (t *Issuer) Validate(token string, now time.Time) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != prefix {
		return false
	}
	exp, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || now.Unix() >= exp {
		return false
	}
	return hmac.Equal([]byte(t.sign(parts[1])), []byte(parts[2]))
}
