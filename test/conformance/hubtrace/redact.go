package hubtrace

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/wzshiming/xet/client/hftest"
)

const redacted = "<redacted>"

var (
	hfTokenRe = regexp.MustCompile(`hf_[A-Za-z0-9]{20,}`)
	jwtRe     = regexp.MustCompile(`eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}`)
	urlRe     = regexp.MustCompile(`(?i)https?://[^\s<>"]+`)
	// queryRe spots a query run wherever it stands, such as a signed path logged without its origin.
	queryRe = regexp.MustCompile(`\?[^\s<>"?]*`)
	// signedKeyRe spots a query key carrying credentials, once percent-decoded.
	signedKeyRe = regexp.MustCompile(`(?i)^(x-amz-[a-z-]+|signature|policy|key-pair-id|token)$`)
	// cookieRe spots a cookie header or pair in text; the rest of its line is the value.
	cookieRe = regexp.MustCompile(`(?i)\b((?:set-cookie|cookie)[ \t]*[:=][ \t]*)[^\r\n]*`)
	// cdnRepoRe spots the hub CDN's repository id segment, /xet-bridge-<region>/<24 hex>/, which names the private repository.
	cdnRepoRe = regexp.MustCompile(`(/xet-bridge-[a-z0-9-]+/)[0-9a-f]{24}(/)`)
)

// Redact replaces Hugging Face user tokens, JWT-shaped tokens, cookie values, CDN repository ids, the queries of absolute URLs and signed queries of relative ones anywhere in s.
func Redact(s string) string {
	s = cookieRe.ReplaceAllString(s, "${1}"+redacted)
	s = urlRe.ReplaceAllStringFunc(s, redactURLQuery)
	s = queryRe.ReplaceAllStringFunc(s, redactSignedQuery)
	// After the URL passes: a placeholder's "<" would otherwise end an absolute URL early.
	s = cdnRepoRe.ReplaceAllString(s, "${1}"+redacted+"${2}")
	s = hfTokenRe.ReplaceAllLiteralString(s, redacted)
	return jwtRe.ReplaceAllLiteralString(s, redacted)
}

// redacted builds the trace exchange numbered seq: credentials, cookies and signed URL queries removed, everything else verbatim.
func (ex *exchange) redacted(seq int) hftest.Exchange {
	return hftest.Exchange{
		Seq:             seq,
		Origin:          ex.origin,
		Method:          ex.method,
		Path:            redactPath(ex.path),
		RequestHeaders:  redactHeaders(ex.reqHeader),
		RequestBody:     redactBody(ex.reqBody, ex.reqHeader.Get("Content-Type")),
		Status:          ex.status,
		ResponseHeaders: redactHeaders(ex.respHeader),
		ResponseBody:    redactBody(ex.respBody, ex.respHeader.Get("Content-Type")),
	}
}

func redactPath(p string) string {
	if path, query, ok := strings.Cut(p, "?"); ok && signedQuery(query) {
		p = path + "?" + redacted
	}
	return Redact(p)
}

func redactHeaders(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, vs := range h {
		rv := make([]string, 0, len(vs))
		switch http.CanonicalHeaderKey(k) {
		case "Cookie", "Set-Cookie", "X-Amz-Cf-Id", "X-Amz-Cf-Pop":
			continue
		case "Authorization":
			for _, v := range vs {
				rv = append(rv, redactAuthorization(v))
			}
		case "X-Xet-Access-Token":
			for range vs {
				rv = append(rv, redacted)
			}
		default:
			for _, v := range vs {
				rv = append(rv, Redact(v))
			}
		}
		out[k] = rv
	}
	return out
}

// redactAuthorization keeps the scheme of a bearer credential so its presence stays visible.
func redactAuthorization(v string) string {
	if len(v) >= 7 && strings.EqualFold(v[:7], "Bearer ") {
		return "Bearer " + redacted
	}
	return redacted
}

// redactURLQuery replaces the whole query of the URL u; an empty one, the remains of a redacted URL, is left as is.
func redactURLQuery(u string) string {
	if base, query, ok := strings.Cut(u, "?"); ok && query != "" {
		return base + "?" + redacted
	}
	return u
}

// redactSignedQuery replaces the query run q, "?" included, when it carries credentials.
func redactSignedQuery(q string) string {
	if signedQuery(q[1:]) {
		return "?" + redacted
	}
	return q
}

// signedQuery reports whether the query q, "?" excluded, carries credentials under a key matched percent-decoded.
func signedQuery(q string) bool {
	for param := range strings.SplitSeq(q, "&") {
		key, _, ok := strings.Cut(param, "=")
		if !ok {
			continue
		}
		if decoded, err := url.QueryUnescape(key); err == nil {
			key = decoded
		}
		if signedKeyRe.MatchString(key) {
			return true
		}
	}
	return false
}

// redactBody copies b with token values removed from JSON text, then URL queries and token patterns from any text: a 302 body names its presigned Location.
func redactBody(b *hftest.Body, contentType string) *hftest.Body {
	if b == nil {
		return nil
	}
	out := *b
	if out.Text == "" {
		return &out
	}
	if isJSON(contentType) {
		if text, ok := rewriteJSON(out.Text, redactJSONString); ok {
			out.Text = text
		}
	}
	out.Text = Redact(out.Text)
	return &out
}

// Scope removes the rest of the repository's inventory from the JSON listings in tr, so a private repository's other files never reach a fixture: tree entries and siblings outside prefix, a directory path ending in "/", are dropped, and repository ids and metadata become placeholders. The ids the listings named are then replaced wherever they recur, such as the CDN paths of a redirect.
func Scope(tr *hftest.Trace, prefix string) {
	s := &scoper{prefix: prefix}
	for i := range tr.Exchanges {
		ex := &tr.Exchanges[i]
		if ex.ResponseBody == nil || ex.ResponseBody.Text == "" || !isJSON(ex.ResponseHeaders.Get("Content-Type")) {
			continue
		}
		if text, ok := transformJSON(ex.ResponseBody.Text, s.value); ok {
			ex.ResponseBody.Text = text
		}
	}
	if len(s.ids) == 0 {
		return
	}
	for i := range tr.Exchanges {
		ex := &tr.Exchanges[i]
		ex.Path = s.scrub(ex.Path)
		for _, h := range []http.Header{ex.RequestHeaders, ex.ResponseHeaders} {
			for _, vs := range h {
				for j, v := range vs {
					vs[j] = s.scrub(v)
				}
			}
		}
		for _, b := range []*hftest.Body{ex.RequestBody, ex.ResponseBody} {
			if b != nil {
				b.Text = s.scrub(b.Text)
			}
		}
	}
}

// scoper prunes listings to prefix, collecting the repository ids they named.
type scoper struct {
	prefix string
	ids    []string
}

// scrub replaces every collected id in s.
func (s *scoper) scrub(str string) string {
	for _, id := range s.ids {
		str = strings.ReplaceAll(str, id, redacted)
	}
	return str
}

// value prunes the decoded JSON value v the way Scope describes.
func (s *scoper) value(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			switch k {
			case "_id":
				if id, ok := e.(string); ok && id != "" && id != redacted && !slices.Contains(s.ids, id) {
					s.ids = append(s.ids, id)
				}
				x[k] = redacted
			case "library_name", "pipeline_tag":
				x[k] = redacted
			case "cardData", "config", "transformersInfo", "widgetData":
				x[k] = map[string]any{}
			case "tags", "spaces":
				x[k] = []any{}
			case "usedStorage":
				x[k] = json.Number("0")
			default:
				x[k] = s.value(e)
			}
		}
	case []any:
		kept := x[:0]
		for _, e := range x {
			if name, ok := entryName(e); ok && name != strings.TrimSuffix(s.prefix, "/") && !strings.HasPrefix(name, s.prefix) {
				continue
			}
			kept = append(kept, s.value(e))
		}
		return kept
	}
	return v
}

// entryName returns the path a tree entry or sibling e names.
func entryName(e any) (string, bool) {
	obj, ok := e.(map[string]any)
	if !ok {
		return "", false
	}
	for _, key := range []string{"path", "rfilename"} {
		if name, ok := obj[key].(string); ok {
			return name, true
		}
	}
	return "", false
}

// redactJSONString redacts the JSON string value s found under key.
func redactJSONString(key, s string) string {
	switch key {
	case "accessToken", "access_token", "token":
		return redacted
	}
	switch strings.ToLower(key) {
	case "cookie", "cookies", "set-cookie":
		return redacted
	}
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		return redactURLQuery(s)
	}
	return s
}

// rewriteJSON re-encodes text, one JSON document or one per line, with fn applied to every string value under its object key; ok is false when text is not JSON.
func rewriteJSON(text string, fn func(key, s string) string) (string, bool) {
	return transformJSON(text, func(v any) any { return walkStrings(v, "", fn) })
}

// transformJSON re-encodes text, one JSON document or one per line, as transform leaves each decoded document; ok is false when text is not JSON.
func transformJSON(text string, transform func(any) any) (string, bool) {
	if out, ok := transformJSONDocument(text, transform); ok {
		return out, true
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		out, ok := transformJSONDocument(line, transform)
		if !ok {
			return text, false
		}
		lines[i] = out
	}
	return strings.Join(lines, "\n"), true
}

// transformJSONDocument re-encodes the single JSON document text compactly, numbers verbatim, keys sorted.
func transformJSONDocument(text string, transform func(any) any) (string, bool) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var v any
	if dec.Decode(&v) != nil {
		return "", false
	}
	if _, err := dec.Token(); err != io.EOF {
		return "", false
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if enc.Encode(transform(v)) != nil {
		return "", false
	}
	return strings.TrimSuffix(buf.String(), "\n"), true
}

// walkStrings returns v with fn applied to every string value, keyed by the innermost object key above it.
func walkStrings(v any, key string, fn func(key, s string) string) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = walkStrings(e, k, fn)
		}
	case []any:
		for i, e := range x {
			x[i] = walkStrings(e, key, fn)
		}
	case string:
		return fn(key, x)
	}
	return v
}
