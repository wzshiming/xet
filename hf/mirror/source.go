package mirror

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/hf"
)

type target struct {
	cacheKey         string
	upstreamURL      string
	repoType         string
	repoID           string
	revision         string
	upstreamRevision string
	file             string
}

func (h *Handler) parseTarget(r *http.Request) (target, bool) {
	escapedRequestPath := r.URL.EscapedPath()
	trimmed := strings.Trim(escapedRequestPath, "/")
	segments := strings.Split(trimmed, "/")
	if len(segments) < 4 {
		return target{}, false
	}
	start := 0
	first, err := url.PathUnescape(segments[0])
	if err != nil {
		return target{}, false
	}
	repoType := "model"
	if first == "datasets" || first == "spaces" {
		start = 1
		repoType = strings.TrimSuffix(first, "s")
	}
	resolve := -1
	// Hub repo IDs are either name or namespace/name. Prefer the namespaced
	// form so a repository itself may be named "resolve".
	for _, i := range []int{start + 2, start + 1} {
		if i >= len(segments) {
			continue
		}
		segment, err := url.PathUnescape(segments[i])
		if err == nil && segment == "resolve" {
			resolve = i
			break
		}
	}
	if resolve < start+1 || resolve+2 >= len(segments) {
		return target{}, false
	}
	revision, err := url.PathUnescape(segments[resolve+1])
	if err != nil || revision == "" {
		return target{}, false
	}
	repoSegments := segments[start:resolve]
	if len(repoSegments) == 0 {
		return target{}, false
	}
	decodedRepoSegments := make([]string, 0, len(repoSegments))
	for _, segment := range repoSegments {
		decoded, err := url.PathUnescape(segment)
		if err != nil || decoded == "" {
			return target{}, false
		}
		decodedRepoSegments = append(decodedRepoSegments, decoded)
	}
	fileSegments := segments[resolve+2:]
	decodedFileSegments := make([]string, 0, len(fileSegments))
	for _, segment := range fileSegments {
		decoded, err := url.PathUnescape(segment)
		if err != nil || decoded == "" || decoded == "." || decoded == ".." {
			return target{}, false
		}
		decodedFileSegments = append(decodedFileSegments, decoded)
	}
	file := strings.Join(decodedFileSegments, "/")
	if file == "" {
		return target{}, false
	}

	upstreamRevision := revision
	rawRequestPath := escapedRequestPath

	u := *h.upstream
	rawPath := strings.TrimRight(h.upstream.EscapedPath(), "/") + "/" + strings.TrimLeft(rawRequestPath, "/")
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		return target{}, false
	}
	u.Path = decodedPath
	u.RawPath = rawPath
	u.RawQuery = r.URL.RawQuery
	// Keep caches from different configured upstreams isolated even if they
	// expose the same repository and resolve path.
	cacheKey := strings.TrimRight(h.upstream.String(), "/") + "\n" + escapedRequestPath
	if r.URL.RawQuery != "" {
		cacheKey += "?" + r.URL.RawQuery
	}
	return target{
		cacheKey:         cacheKey,
		upstreamURL:      u.String(),
		repoType:         repoType,
		repoID:           strings.Join(decodedRepoSegments, "/"),
		revision:         revision,
		upstreamRevision: upstreamRevision,
		file:             file,
	}, true
}

type sourceKind uint8

const (
	sourceHTTP sourceKind = iota
	sourceXET
)

type sourceInfo struct {
	kind           sourceKind
	size           int64
	etag           string
	identity       string
	modTime        time.Time
	header         map[string]string
	fileHash       xet.FileHash
	provider       client.AuthProvider
	expectedSHA256 string
}

type upstreamError struct {
	status int
}

func (e *upstreamError) Error() string { return fmt.Sprintf("upstream returned status %d", e.status) }

func (h *Handler) resolveSource(ctx context.Context, target target) (sourceInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, target.upstreamURL, nil)
	if err != nil {
		return sourceInfo{}, err
	}
	h.prepareUpstreamRequest(req)
	headClient := *h.httpClient
	headClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := headClient.Do(req)
	if err != nil {
		return sourceInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return sourceInfo{}, &upstreamError{status: resp.StatusCode}
	}

	info := sourceInfo{
		size:    responseSize(resp),
		etag:    firstHeader(resp.Header, "X-Linked-Etag", "ETag"),
		modTime: parseHTTPTime(resp.Header.Get("Last-Modified")),
		header:  selectHeaders(resp.Header),
	}
	if expected, ok := sha256ETag(resp.Header.Get("X-Linked-Etag")); ok {
		info.expectedSHA256 = expected
	}
	fileHash, provider, xetErr := hf.ResolveResponseWithToken(ctx, h.httpClient, resp, h.upstreamToken)
	if xetErr == nil {
		info.kind = sourceXET
		info.fileHash = fileHash
		info.provider = provider
		info.identity = "xet:" + fileHash.String()
		return info, nil
	}
	if hasXETLinks(resp.Header) {
		return sourceInfo{}, xetErr
	}

	// Follow the redirect internally to obtain ordinary HTTP object metadata.
	if resp.StatusCode >= 300 || info.size < 0 {
		finalReq, err := http.NewRequestWithContext(ctx, http.MethodHead, target.upstreamURL, nil)
		if err != nil {
			return sourceInfo{}, err
		}
		h.prepareUpstreamRequest(finalReq)
		followClient := *h.httpClient
		followClient.CheckRedirect = nil
		finalResp, err := followClient.Do(finalReq)
		if err != nil {
			return sourceInfo{}, err
		}
		defer finalResp.Body.Close()
		if finalResp.StatusCode >= 400 {
			return sourceInfo{}, &upstreamError{status: finalResp.StatusCode}
		}
		if finalSize := responseSize(finalResp); finalSize >= 0 {
			info.size = finalSize
		}
		if info.etag == "" {
			info.etag = firstHeader(finalResp.Header, "X-Linked-Etag", "ETag")
		}
		if info.expectedSHA256 == "" {
			if expected, ok := sha256ETag(finalResp.Header.Get("X-Linked-Etag")); ok {
				info.expectedSHA256 = expected
			}
		}
		if info.modTime.IsZero() {
			info.modTime = parseHTTPTime(finalResp.Header.Get("Last-Modified"))
		}
		for key, value := range selectHeaders(finalResp.Header) {
			if info.header[key] == "" {
				info.header[key] = value
			}
		}
	}
	if info.size < 0 {
		repo, repoErr := h.loadUpstreamRepo(ctx, target.repoType, target.repoID, target.upstreamRevision)
		if repoErr == nil {
			file, ok := repo.byPath[target.file]
			if !ok || file.Type != "blob" {
				return sourceInfo{}, &upstreamError{status: http.StatusNotFound}
			}
			info.size = file.Size
			if isSHA256(file.SHA256) {
				info.etag = quoteETag(file.SHA256)
				info.expectedSHA256 = file.SHA256
			}
			info.header["X-Repo-Commit"] = repo.Commit
		} else {
			return sourceInfo{}, repoErr
		}
	}
	info.kind = sourceHTTP
	if info.header["X-Repo-Commit"] == "" {
		info.header["X-Repo-Commit"] = syntheticRepoCommit(target)
	}
	info.identity = fmt.Sprintf("http:%s:%d:%s", info.etag, info.size, info.modTime.UTC().Format(time.RFC3339Nano))
	return info, nil
}

func (h *Handler) prepareUpstreamRequest(req *http.Request) {
	req.Header.Set("Accept-Encoding", "identity")
	if h.upstreamToken != "" {
		req.Header.Set("Authorization", "Bearer "+h.upstreamToken)
	}
}

func responseSize(resp *http.Response) int64 {
	if value := resp.Header.Get("X-Linked-Size"); value != "" {
		size, err := strconv.ParseInt(value, 10, 64)
		if err == nil && size >= 0 {
			return size
		}
	}
	if resp.StatusCode < 300 {
		if value := resp.Header.Get("Content-Length"); value != "" {
			size, err := strconv.ParseInt(value, 10, 64)
			if err == nil && size >= 0 {
				return size
			}
		}
		if resp.ContentLength >= 0 {
			return resp.ContentLength
		}
	}
	return -1
}

func firstHeader(header http.Header, keys ...string) string {
	for _, key := range keys {
		if value := header.Get(key); value != "" {
			return value
		}
	}
	return ""
}

func selectHeaders(header http.Header) map[string]string {
	selected := map[string]string{}
	for _, key := range []string{"Content-Type", "Content-Disposition", "Cache-Control", "X-Repo-Commit"} {
		if value := header.Get(key); value != "" {
			selected[key] = value
		}
	}
	return selected
}

func parseHTTPTime(value string) time.Time {
	parsed, _ := http.ParseTime(value)
	return parsed
}

func hasXETLinks(header http.Header) bool {
	for _, link := range header.Values("Link") {
		if strings.Contains(link, "xet-reconstruction-info") || strings.Contains(link, "xet-auth") {
			return true
		}
	}
	return false
}
