package token

import (
	"testing"
	"time"
)

func TestIssuer(t *testing.T) {
	issuer, err := NewIssuer(nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	token, exp := issuer.Mint(now)
	if exp <= now.Unix() {
		t.Fatalf("exp = %d, want after %d", exp, now.Unix())
	}
	if !issuer.Validate(token, now) {
		t.Fatal("freshly minted token rejected")
	}
	if issuer.Validate(token, now.Add(2*time.Minute)) {
		t.Fatal("expired token accepted")
	}
	if issuer.Validate(token+"x", now) {
		t.Fatal("tampered token accepted")
	}
	if issuer.Validate("garbage", now) {
		t.Fatal("garbage token accepted")
	}
	other, err := NewIssuer(nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if other.Validate(token, now) {
		t.Fatal("token minted by another issuer accepted")
	}

	t.Run("fixed secret shared across issuers", func(t *testing.T) {
		secret := []byte("0123456789abcdef0123456789abcdef")
		a, err := NewIssuer(secret, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		b, err := NewIssuer(secret, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		token, _ := a.Mint(now)
		if !b.Validate(token, now) {
			t.Fatal("token minted with shared secret rejected")
		}
	})
}
