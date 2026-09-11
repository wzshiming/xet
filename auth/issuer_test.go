package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
)

var _ Authorizer = (*Issuer)(nil)

func signPayload(t *testing.T, secret []byte, payloadJSON string) string {
	t.Helper()
	segment := base64.RawURLEncoding.EncodeToString([]byte(payloadJSON))
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(segment))
	return segment + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestIssuerTokenFormat(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Unix(1_800_000_000, 0)
	issuer, err := NewIssuer(secret, time.Minute, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	var file xet.FileHash
	var digest [32]byte
	for index := range file {
		file[index] = 0x11
		digest[index] = 0x22
	}
	for _, test := range []struct {
		name     string
		grant    Grant
		payload  string
		expected string
	}{
		{
			name:     "unbound",
			grant:    Grant{Permission: Read},
			payload:  `{"permission":"read","exp":1800000060}`,
			expected: "eyJwZXJtaXNzaW9uIjoicmVhZCIsImV4cCI6MTgwMDAwMDA2MH0.0WWkE-UPuM7RihXpgpCHp1czYeo_YcNLdPzacVmC30A",
		},
		{
			name:     "bound",
			grant:    Grant{Permission: Read, File: file, SHA256: digest},
			payload:  `{"permission":"read","exp":1800000060,"file":"1111111111111111111111111111111111111111111111111111111111111111","sha256":"2222222222222222222222222222222222222222222222222222222222222222"}`,
			expected: "eyJwZXJtaXNzaW9uIjoicmVhZCIsImV4cCI6MTgwMDAwMDA2MCwiZmlsZSI6IjExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTEiLCJzaGEyNTYiOiIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyIn0.by-PDp7AAvzdldlFpQ0pKZMuw1WYLkSna4COvI651RI",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, err := json.Marshal(json.RawMessage(test.payload))
			if err != nil {
				t.Fatal(err)
			}
			if independent := signPayload(t, secret, string(payload)); independent != test.expected {
				t.Fatalf("independent token = %q, want %q", independent, test.expected)
			}
			token, exp, err := issuer.Sign(test.grant)
			if err != nil {
				t.Fatal(err)
			}
			if token != test.expected || exp != 1_800_000_060 {
				t.Fatalf("Sign() = (%q, %d), want (%q, 1800000060)", token, exp, test.expected)
			}
			parts := strings.Split(token, ".")
			if len(parts) != 2 {
				t.Fatalf("token has %d parts, want 2", len(parts))
			}
			decoded, err := base64.RawURLEncoding.DecodeString(parts[0])
			if err != nil {
				t.Fatal(err)
			}
			if string(decoded) != test.payload {
				t.Fatalf("payload = %s, want %s", decoded, test.payload)
			}
			var keys map[string]json.RawMessage
			if err := json.Unmarshal(decoded, &keys); err != nil {
				t.Fatal(err)
			}
			if test.grant.File == (xet.FileHash{}) && (keys["file"] != nil || keys["sha256"] != nil) {
				t.Fatal("unbound payload contains file or sha256")
			}
			if got, ok := issuer.Validate(token); !ok || got != test.grant {
				t.Fatalf("Validate() = (%+v, %v), want (%+v, true)", got, ok, test.grant)
			}
		})
	}
}

func TestIssuerRejectsZeroTarget(t *testing.T) {
	issuer, err := NewIssuer(nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, permission := range []Permission{Read, Write} {
		token, exp, err := issuer.Sign(Grant{Permission: permission, Targeted: true})
		if err == nil || token != "" || exp != 0 {
			t.Fatalf("Sign(%q, zero target) = (%q, %d, %v), want rejection", permission, token, exp, err)
		}
	}
}

func TestIssuerRejectsInvalidTokens(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Unix(1_800_000_000, 0)
	issuer, err := NewIssuer(secret, time.Minute, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	payload := `{"permission":"read","exp":1800000060}`
	token := signPayload(t, secret, payload)
	parts := strings.Split(token, ".")
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	signature[0] ^= 1
	other := strings.Split(signPayload(t, secret, `{"permission":"read","exp":1800000061}`), ".")
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte("!"))
	for _, test := range []struct {
		name  string
		token string
	}{
		{"empty", ""},
		{"one segment", parts[0]},
		{"old JWT shape", "header.payload.sig"},
		{"extra segment", token + ".extra"},
		{"empty payload", "." + parts[1]},
		{"empty signature", parts[0] + "."},
		{"empty segments", "."},
		{"many dots", strings.Repeat(".", 1024)},
		{"bad payload base64 with valid signature", "!." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))},
		{"bad signature base64", parts[0] + ".!"},
		{"wrong secret", signPayload(t, []byte("another secret"), payload)},
		{"flipped signature", parts[0] + "." + base64.RawURLEncoding.EncodeToString(signature)},
		{"different payload signature", parts[0] + "." + other[1]},
		{"payload tamper", base64.RawURLEncoding.EncodeToString([]byte(strings.Replace(payload, "read", "write", 1))) + "." + parts[1]},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, ok := issuer.Validate(test.token); ok || got != (Grant{}) {
				t.Fatalf("Validate() = (%+v, %v), want denial", got, ok)
			}
		})
	}
}

func TestIssuerClaims(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Unix(1_800_000_000, 0)
	issuer, err := NewIssuer(secret, time.Minute, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		payload string
		valid   bool
	}{
		{"valid", `{"permission":"read","exp":1800000060}`, true},
		{"formatted payload", `{ "exp": 1800000060, "permission": "read" }`, true},
		{"unknown keys ignored", `{"permission":"read","exp":1800000060,"nbf":1800000061}`, true},
		{"expired", `{"permission":"read","exp":1799999999}`, false},
		{"at expiry", `{"permission":"read","exp":1800000000}`, false},
		{"missing expiry", `{"permission":"read"}`, false},
		{"null expiry", `{"permission":"read","exp":null}`, false},
		{"string expiry", `{"permission":"read","exp":"1800000060"}`, false},
		{"float expiry", `{"permission":"read","exp":1800000060.0}`, false},
		{"fractional expiry", `{"permission":"read","exp":1800000060.5}`, false},
		{"boolean expiry", `{"permission":"read","exp":true}`, false},
		{"overflow expiry", `{"permission":"read","exp":9223372036854775808}`, false},
		{"missing permission", `{"exp":1800000060}`, false},
		{"unknown permission", `{"permission":"owner","exp":1800000060}`, false},
		{"admin permission", `{"permission":"admin","exp":1800000060}`, false},
		{"case sensitive permission", `{"permission":"Read","exp":1800000060}`, false},
		{"invalid JSON", `{`, false},
		{"trailing JSON", `{"permission":"read","exp":1800000060}{}`, false},
		{"null payload", `null`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := issuer.Validate(signPayload(t, secret, test.payload))
			if ok != test.valid || (ok && got != (Grant{Permission: Read})) || (!ok && got != (Grant{})) {
				t.Fatalf("Validate() = (%+v, %v), want valid %v", got, ok, test.valid)
			}
		})
	}
	for _, field := range []string{"file", "sha256"} {
		for _, value := range []string{`""`, `"ab"`, `"` + strings.Repeat("a", 63) + `"`, `"` + strings.Repeat("a", 66) + `"`, `"` + strings.Repeat("z", 64) + `"`, `null`, `123`} {
			t.Run(field+"/"+value, func(t *testing.T) {
				payload := fmt.Sprintf(`{"permission":"read","exp":1800000060,%q:%s}`, field, value)
				if got, ok := issuer.Validate(signPayload(t, secret, payload)); ok || got != (Grant{}) {
					t.Fatalf("Validate() = (%+v, %v), want invalid binding rejected", got, ok)
				}
			})
		}
	}
}

func TestIssuer(t *testing.T) {
	ttl := time.Minute
	now := time.Unix(1_800_000_000, 0)
	clock := now
	issuer, err := NewIssuer(nil, ttl, func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewIssuer(nil, ttl, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for _, grant := range []Grant{
		{Permission: Read},
		{Permission: Write},
		{Permission: Read, File: xet.FileHash{1}},
		{Permission: Write, SHA256: [32]byte{2}},
	} {
		t.Run(fmt.Sprintf("%+v", grant), func(t *testing.T) {
			clock = now
			token, exp, err := issuer.Sign(grant)
			if err != nil {
				t.Fatal(err)
			}
			if exp != now.Add(ttl).Unix() {
				t.Fatalf("exp = %d, want %d", exp, now.Add(ttl).Unix())
			}
			if got, ok := issuer.Validate(token); !ok || got != grant {
				t.Fatalf("Validate() = (%+v, %v), want (%+v, true)", got, ok, grant)
			}
			clock = time.Unix(exp, 0).Add(-time.Nanosecond)
			if _, ok := issuer.Validate(token); !ok {
				t.Fatal("token rejected before expiry")
			}
			clock = time.Unix(exp, 0)
			if _, ok := issuer.Validate(token); ok {
				t.Fatal("token accepted at expiry")
			}
			if _, ok := other.Validate(token); ok {
				t.Fatal("token minted by another issuer accepted")
			}
		})
	}
	for _, perm := range []Permission{"", "x", "admin"} {
		token, exp, err := issuer.Sign(Grant{Permission: perm})
		if err == nil || err.Error() != fmt.Sprintf("unsupported permission %q", perm) || token != "" || exp != 0 {
			t.Fatalf("Sign(%q) = (%q, %d, %v), want unsupported permission error", perm, token, exp, err)
		}
	}

	t.Run("fixed secret shared across issuers", func(t *testing.T) {
		secret := []byte("0123456789abcdef0123456789abcdef")
		first, err := NewIssuer(secret, ttl, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		second, err := NewIssuer(secret, ttl, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		token, _, err := first.Sign(Grant{Permission: Write})
		if err != nil {
			t.Fatal(err)
		}
		if grant, ok := second.Validate(token); !ok || grant != (Grant{Permission: Write}) {
			t.Fatal("token minted with shared secret rejected")
		}
	})

	t.Run("default ttl", func(t *testing.T) {
		for _, ttl := range []time.Duration{0, -time.Minute} {
			issuer, err := NewIssuer(nil, ttl, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			_, exp, err := issuer.Sign(Grant{Permission: Read})
			if err != nil {
				t.Fatal(err)
			}
			if exp != now.Add(15*time.Minute).Unix() {
				t.Fatalf("exp = %d, want default ttl", exp)
			}
		}
	})
	issuer, err = NewIssuer(nil, ttl, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, exp, err := issuer.Sign(Grant{Permission: Read})
	if err != nil {
		t.Fatal(err)
	}
	wallNow := time.Now
	if delta := time.Unix(exp, 0).Sub(wallNow().Add(ttl)); delta < -3*time.Second || delta > 3*time.Second {
		t.Fatalf("exp = %d, want current time plus ttl", exp)
	}
}

func TestIssuerAuthorize(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	clock := now
	issuer, err := NewIssuer([]byte("s3cret"), time.Minute, func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	randomIssuer, err := NewIssuer(nil, time.Minute, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	readToken, _, err := issuer.Sign(Grant{Permission: Read})
	if err != nil {
		t.Fatal(err)
	}
	adminToken := signPayload(t, []byte("s3cret"), fmt.Sprintf(`{"permission":"admin","exp":%d}`, now.Add(time.Minute).Unix()))
	unknown := signPayload(t, []byte("s3cret"), fmt.Sprintf(`{"permission":"owner","exp":%d}`, now.Add(time.Minute).Unix()))
	clock = clock.Add(-2 * time.Minute)
	expired, _, err := issuer.Sign(Grant{Permission: Read})
	if err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(2 * time.Minute)
	file := xet.FileHash{1}
	other := xet.FileHash{2}
	digest := [32]byte{3}
	otherDigest := [32]byte{4}
	for _, test := range []struct {
		name     string
		issuer   *Issuer
		header   string
		grant    Grant
		required Grant
		want     error
	}{
		{"missing", issuer, "", Grant{}, Grant{Permission: Read}, ErrUnauthenticated},
		{"empty bearer", issuer, "Bearer ", Grant{}, Grant{Permission: Read}, ErrUnauthenticated},
		{"malformed bearer", issuer, "Bearer " + readToken + " extra", Grant{}, Grant{Permission: Read}, ErrUnauthenticated},
		{"garbage", issuer, "Bearer garbage", Grant{}, Grant{Permission: Read}, ErrUnauthenticated},
		{"expired", issuer, "Bearer " + expired, Grant{}, Grant{Permission: Read}, ErrUnauthenticated},
		{"unknown permission", issuer, "Bearer " + unknown, Grant{}, Grant{Permission: Read}, ErrUnauthenticated},
		{"read/read", issuer, "", Grant{Permission: Read}, Grant{Permission: Read}, nil},
		{"read/write", issuer, "", Grant{Permission: Read}, Grant{Permission: Write}, ErrForbidden},
		{"read/unknown", issuer, "", Grant{Permission: Read}, Grant{Permission: "unknown"}, ErrForbidden},
		{"write/read", issuer, "", Grant{Permission: Write}, Grant{Permission: Read}, ErrForbidden},
		{"write/write", issuer, "", Grant{Permission: Write}, Grant{Permission: Write}, nil},
		{"former admin/read", issuer, "Bearer " + adminToken, Grant{}, Grant{Permission: Read}, ErrUnauthenticated},
		{"former admin/write", issuer, "Bearer " + adminToken, Grant{}, Grant{Permission: Write}, ErrUnauthenticated},
		{"former admin/unknown", issuer, "Bearer " + adminToken, Grant{}, Grant{Permission: "admin"}, ErrUnauthenticated},
		{"signing secret read", issuer, "Bearer s3cret", Grant{}, Grant{Permission: Read}, ErrUnauthenticated},
		{"signing secret write", issuer, "Bearer s3cret", Grant{}, Grant{Permission: Write}, ErrUnauthenticated},
		{"random secret is not a bearer", randomIssuer, "Bearer " + string(randomIssuer.secret), Grant{}, Grant{Permission: Read}, ErrUnauthenticated},
		{"unbound target", issuer, "", Grant{Permission: Read}, Grant{Permission: Read, File: file}, nil},
		{"file match", issuer, "", Grant{Permission: Read, File: file}, Grant{Permission: Read, File: file}, nil},
		{"file mismatch", issuer, "", Grant{Permission: Read, File: file}, Grant{Permission: Read, File: other}, ErrForbidden},
		{"file untargeted", issuer, "", Grant{Permission: Read, File: file}, Grant{Permission: Read}, nil},
		{"file zero target", issuer, "", Grant{Permission: Read, File: file}, Grant{Permission: Read, Targeted: true}, ErrForbidden},
		{"sha256 zero target", issuer, "", Grant{Permission: Write, SHA256: digest}, Grant{Permission: Write, Targeted: true}, ErrForbidden},
		{"unbound zero target", issuer, "", Grant{Permission: Read}, Grant{Permission: Read, Targeted: true}, nil},
		{"file wrong permission", issuer, "", Grant{Permission: Read, File: file}, Grant{Permission: Write, File: file}, ErrForbidden},
		{"file unavailable", issuer, "", Grant{Permission: Read, File: file}, Grant{Permission: Read, SHA256: digest}, ErrForbidden},
		{"sha256 xorb", issuer, "", Grant{Permission: Write, SHA256: digest}, Grant{Permission: Write}, nil},
		{"sha256 shard match", issuer, "", Grant{Permission: Write, SHA256: digest}, Grant{Permission: Write, File: file, SHA256: digest}, nil},
		{"sha256 shard missing metadata", issuer, "", Grant{Permission: Write, SHA256: digest}, Grant{Permission: Write, File: file}, ErrForbidden},
		{"sha256 mismatch", issuer, "", Grant{Permission: Write, SHA256: digest}, Grant{Permission: Write, File: file, SHA256: otherDigest}, ErrForbidden},
		{"sha256 unlink", issuer, "", Grant{Permission: Write, SHA256: digest}, Grant{Permission: Write, SHA256: digest}, nil},
		{"both match", issuer, "", Grant{Permission: Write, File: file, SHA256: digest}, Grant{Permission: Write, File: file, SHA256: digest}, nil},
		{"both file mismatch", issuer, "", Grant{Permission: Write, File: file, SHA256: digest}, Grant{Permission: Write, File: other, SHA256: digest}, ErrForbidden},
		{"both sha256 mismatch", issuer, "", Grant{Permission: Write, File: file, SHA256: digest}, Grant{Permission: Write, File: file, SHA256: otherDigest}, ErrForbidden},
		{"both untargeted", issuer, "", Grant{Permission: Write, File: file, SHA256: digest}, Grant{Permission: Write}, nil},
		{"signing secret target", issuer, "Bearer s3cret", Grant{}, Grant{Permission: Write, File: other, SHA256: otherDigest}, ErrUnauthenticated},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.grant != (Grant{}) {
				token, _, err := test.issuer.Sign(test.grant)
				if err != nil {
					t.Fatal(err)
				}
				test.header = "Bearer " + token
			}
			request := httptest.NewRequest("GET", "/", nil)
			request.Header.Set("Authorization", test.header)
			if err := test.issuer.Authorize(request, test.required); !errors.Is(err, test.want) {
				t.Fatalf("Authorize() = %v, want %v", err, test.want)
			}
		})
	}
}
