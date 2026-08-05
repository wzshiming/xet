package mirror

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const mutableRevisionTTL = 30 * time.Second

var errIncompatibleHFAPI = errors.New("upstream does not implement the Hugging Face repository API")

type upstreamFile struct {
	Path          string
	Size          int64
	SHA256        string
	Revision      string
	Type          string
	IsLFS         bool
	CommittedDate int64
}

type upstreamRepo struct {
	Commit  string
	Files   []upstreamFile
	byPath  map[string]upstreamFile
	fetched time.Time
}

type upstreamRepoCall struct {
	done chan struct{}
	repo *upstreamRepo
	err  error
}

func (h *Handler) loadUpstreamRepo(ctx context.Context, repoType, repoID, revision string) (*upstreamRepo, error) {
	key := repoType + "/" + repoID + "@" + revision
	now := time.Now()

	h.mu.Lock()
	if repo := h.repos[key]; repo != nil && (isCommitHash(revision) || now.Sub(repo.fetched) < mutableRevisionTTL) {
		h.mu.Unlock()
		return repo, nil
	}
	if call := h.repoCalls[key]; call != nil {
		h.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-call.done:
			return call.repo, call.err
		}
	}
	call := &upstreamRepoCall{done: make(chan struct{})}
	h.repoCalls[key] = call
	h.mu.Unlock()

	repo, err := h.fetchHFRepo(ctx, repoType, repoID, revision)
	if errors.Is(err, errIncompatibleHFAPI) {
		repo, err = h.fetchLegacyRepo(ctx, repoType, repoID, revision)
	}

	h.mu.Lock()
	if err == nil {
		h.repos[key] = repo
		if isCommitHash(repo.Commit) {
			h.repos[repoType+"/"+repoID+"@"+repo.Commit] = repo
		}
	}
	call.repo = repo
	call.err = err
	close(call.done)
	delete(h.repoCalls, key)
	h.mu.Unlock()
	return repo, err
}

func (h *Handler) fetchHFRepo(ctx context.Context, repoType, repoID, revision string) (*upstreamRepo, error) {
	request := hfRepoAPIRequest{kind: hfAPIRepoInfo, repoType: repoType, repoID: repoID, revision: revision}
	status, _, body, err := h.fetchStandardHFAPI(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &upstreamError{status: status}
	}
	var info struct {
		SHA string `json:"sha"`
	}
	if err := json.Unmarshal(body, &info); err != nil || !isCommitHash(info.SHA) {
		return nil, errIncompatibleHFAPI
	}

	request = hfRepoAPIRequest{kind: hfAPITree, repoType: repoType, repoID: repoID, revision: info.SHA}
	status, _, body, err = h.fetchStandardHFAPI(ctx, request, nil)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &upstreamError{status: status}
	}
	files, err := parseHFTree(body)
	if err != nil {
		return nil, errIncompatibleHFAPI
	}
	return newUpstreamRepo(strings.ToLower(info.SHA), files), nil
}

type legacyFile struct {
	Path          string `json:"Path"`
	Size          int64  `json:"Size"`
	SHA256        string `json:"Sha256"`
	Revision      string `json:"Revision"`
	Type          string `json:"Type"`
	IsLFS         bool   `json:"IsLFS"`
	CommittedDate int64  `json:"CommittedDate"`
}

type legacyFilesResponse struct {
	Code    int    `json:"Code"`
	Success bool   `json:"Success"`
	Message string `json:"Message"`
	Data    struct {
		Files           []legacyFile `json:"Files"`
		LatestCommitter struct {
			ID            string `json:"Id"`
			ShortID       string `json:"ShortId"`
			CommittedDate int64  `json:"CommittedDate"`
		} `json:"LatestCommitter"`
	} `json:"Data"`
}

// fetchLegacyRepo is a capability fallback for upstreams that expose HF-style
// resolve URLs but use the ModelScope legacy repository API for metadata.
func (h *Handler) fetchLegacyRepo(ctx context.Context, repoType, repoID, revision string) (*upstreamRepo, error) {
	segment, ok := legacyRepoSegment(repoType)
	if !ok {
		return nil, &upstreamError{status: http.StatusNotFound}
	}
	legacyRevision := revision
	if legacyRevision == "main" {
		legacyRevision = "master"
	}
	u := *h.upstream
	rawPath := strings.TrimRight(h.upstream.EscapedPath(), "/") + "/api/v1/" + segment + "/" + escapeRepoID(repoID)
	if segment == "datasets" {
		rawPath += "/repo/tree"
	} else {
		rawPath += "/repo/files"
	}
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		return nil, err
	}
	u.Path = decodedPath
	u.RawPath = rawPath
	query := url.Values{"Revision": []string{legacyRevision}, "Recursive": []string{"True"}}
	pageSize := 0
	if segment == "datasets" {
		pageSize = 200
		query.Set("PageSize", strconv.Itoa(pageSize))
	}
	var legacyFiles []legacyFile
	var latestID, latestShortID string
	for page := 1; ; page++ {
		if pageSize != 0 {
			query.Set("PageNumber", strconv.Itoa(page))
		}
		u.RawQuery = query.Encode()
		payload, err := h.fetchLegacyFilesPage(ctx, u)
		if err != nil {
			return nil, err
		}
		if payload.Data.Files == nil && page == 1 {
			return nil, &upstreamError{status: http.StatusNotFound}
		}
		legacyFiles = append(legacyFiles, payload.Data.Files...)
		if payload.Data.LatestCommitter.ID != "" {
			latestID = payload.Data.LatestCommitter.ID
		}
		if payload.Data.LatestCommitter.ShortID != "" {
			latestShortID = payload.Data.LatestCommitter.ShortID
		}
		if pageSize == 0 || len(payload.Data.Files) < pageSize {
			break
		}
	}

	files := make([]upstreamFile, 0, len(legacyFiles))
	for _, file := range legacyFiles {
		files = append(files, upstreamFile{
			Path:          strings.TrimPrefix(file.Path, "/"),
			Size:          file.Size,
			SHA256:        strings.ToLower(strings.TrimSpace(file.SHA256)),
			Revision:      strings.ToLower(strings.TrimSpace(file.Revision)),
			Type:          strings.ToLower(file.Type),
			IsLFS:         file.IsLFS,
			CommittedDate: file.CommittedDate,
		})
	}
	commit := legacyCommit(legacyRevision, files, latestID, latestShortID)
	return newUpstreamRepo(commit, files), nil
}

func (h *Handler) fetchLegacyFilesPage(ctx context.Context, u url.URL) (legacyFilesResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return legacyFilesResponse{}, err
	}
	h.prepareUpstreamRequest(req)
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return legacyFilesResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return legacyFilesResponse{}, &upstreamError{status: resp.StatusCode}
	}
	var payload legacyFilesResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return legacyFilesResponse{}, fmt.Errorf("decode legacy repository file tree: %w", err)
	}
	if (payload.Code != 0 && payload.Code != http.StatusOK) || !payload.Success {
		status := payload.Code
		if status < 400 || status > 599 {
			status = http.StatusBadGateway
		}
		return legacyFilesResponse{}, &upstreamError{status: status}
	}
	return payload, nil
}

func newUpstreamRepo(commit string, files []upstreamFile) *upstreamRepo {
	repo := &upstreamRepo{
		Commit:  strings.ToLower(commit),
		Files:   files,
		byPath:  make(map[string]upstreamFile, len(files)),
		fetched: time.Now(),
	}
	for i := range repo.Files {
		file := &repo.Files[i]
		file.Path = strings.TrimPrefix(file.Path, "/")
		file.Type = strings.ToLower(file.Type)
		if file.Type == "" || file.Type == "file" {
			file.Type = "blob"
		}
		if file.Path != "" {
			repo.byPath[file.Path] = *file
		}
	}
	return repo
}

func legacyCommit(revision string, files []upstreamFile, latestID, latestShortID string) string {
	if isCommitHash(revision) {
		return strings.ToLower(revision)
	}
	if isCommitHash(latestID) {
		return strings.ToLower(latestID)
	}
	latestShortID = strings.ToLower(strings.TrimSpace(latestShortID))
	var newest upstreamFile
	for _, file := range files {
		if latestShortID != "" && strings.HasPrefix(file.Revision, latestShortID) && isCommitHash(file.Revision) {
			return file.Revision
		}
		if file.CommittedDate > newest.CommittedDate && isCommitHash(file.Revision) {
			newest = file
		}
	}
	if newest.Revision != "" {
		return newest.Revision
	}
	ordered := append([]upstreamFile(nil), files...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Path < ordered[j].Path })
	hasher := sha256.New()
	for _, file := range ordered {
		_, _ = fmt.Fprintf(hasher, "%s\x00%d\x00%s\x00%s\n", file.Path, file.Size, file.SHA256, file.Type)
	}
	return hex.EncodeToString(hasher.Sum(nil))[:40]
}

func (h *Handler) serveHFRepoAPI(w http.ResponseWriter, r *http.Request) bool {
	request, ok := parseHFRepoAPIRequest(r)
	if !ok {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return true
	}

	status, header, body, err := h.fetchStandardHFAPI(r.Context(), request, r.URL.Query())
	if err == nil && standardHFAPIResponse(request.kind, status, body) {
		h.writeStandardHFAPI(w, r, status, header, body)
		return true
	}
	if err != nil {
		http.Error(w, "Failed to query upstream repository", http.StatusBadGateway)
		return true
	}
	if status >= 400 {
		h.writeStandardHFAPI(w, r, status, header, body)
		return true
	}

	repo, err := h.fetchLegacyRepo(r.Context(), request.repoType, request.repoID, request.revision)
	if err != nil {
		var upstreamErr *upstreamError
		if errors.As(err, &upstreamErr) {
			http.Error(w, http.StatusText(upstreamErr.status), upstreamErr.status)
		} else {
			http.Error(w, "Failed to query upstream repository", http.StatusBadGateway)
		}
		return true
	}
	h.cacheRepo(request.repoType, request.repoID, request.revision, repo)
	h.writeTranslatedRepoAPI(w, r, request, repo)
	return true
}

func (h *Handler) fetchStandardHFAPI(ctx context.Context, request hfRepoAPIRequest, query url.Values) (int, http.Header, []byte, error) {
	u := *h.upstream
	rawPath := strings.TrimRight(h.upstream.EscapedPath(), "/") + request.standardPath()
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		return 0, nil, nil, err
	}
	u.Path = decodedPath
	u.RawPath = rawPath
	if query != nil {
		u.RawQuery = query.Encode()
	} else if request.kind == hfAPITree {
		u.RawQuery = "recursive=true"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, nil, nil, err
	}
	h.prepareUpstreamRequest(req)
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header.Clone(), body, nil
}

func standardHFAPIResponse(kind hfAPIKind, status int, body []byte) bool {
	if status >= 400 {
		return json.Valid(body)
	}
	if kind == hfAPIRepoInfo {
		var info struct {
			ID  string `json:"id"`
			SHA string `json:"sha"`
		}
		return json.Unmarshal(body, &info) == nil && info.ID != "" && isCommitHash(info.SHA)
	}
	_, err := parseHFTree(body)
	return err == nil
}

func parseHFTree(body []byte) ([]upstreamFile, error) {
	var entries []struct {
		Type string `json:"type"`
		OID  string `json:"oid"`
		Size int64  `json:"size"`
		Path string `json:"path"`
		LFS  *struct {
			OID  string `json:"oid"`
			Size int64  `json:"size"`
		} `json:"lfs"`
	}
	if err := json.Unmarshal(body, &entries); err != nil || entries == nil {
		return nil, errIncompatibleHFAPI
	}
	files := make([]upstreamFile, 0, len(entries))
	for _, entry := range entries {
		if entry.Path == "" || entry.Size < 0 || (entry.Type != "file" && entry.Type != "directory") {
			return nil, errIncompatibleHFAPI
		}
		file := upstreamFile{Path: entry.Path, Size: entry.Size, Revision: entry.OID, Type: entry.Type}
		if entry.LFS != nil && isSHA256(entry.LFS.OID) {
			file.SHA256 = strings.ToLower(entry.LFS.OID)
			file.IsLFS = true
			file.Size = entry.LFS.Size
		} else if isSHA256(entry.OID) {
			file.SHA256 = strings.ToLower(entry.OID)
		}
		files = append(files, file)
	}
	return files, nil
}

func (h *Handler) writeStandardHFAPI(w http.ResponseWriter, r *http.Request, status int, upstreamHeader http.Header, body []byte) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if decoder.Decode(&value) == nil {
		value = sanitizeUpstreamURLs(value, strings.TrimRight(h.upstream.String(), "/"), h.baseURL(r))
		body, _ = json.Marshal(value)
	}
	w.Header().Set("Content-Type", "application/json")
	if contentType := upstreamHeader.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	for _, link := range upstreamHeader.Values("Link") {
		w.Header().Add("Link", rewriteUpstreamLink(link, strings.TrimRight(h.upstream.String(), "/"), h.baseURL(r)))
	}
	if commit := upstreamHeader.Get("X-Repo-Commit"); commit != "" {
		w.Header().Set("X-Repo-Commit", commit)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

func rewriteUpstreamLink(link, upstreamBase, localBase string) string {
	for _, sourceBase := range []string{upstreamBase, "https://huggingface.co", "http://huggingface.co"} {
		link = strings.ReplaceAll(link, "<"+sourceBase, "<"+localBase)
	}
	return link
}

func sanitizeUpstreamURLs(value any, upstreamBase, localBase string) any {
	switch value := value.(type) {
	case string:
		for _, sourceBase := range []string{upstreamBase, "https://huggingface.co", "http://huggingface.co"} {
			if strings.HasPrefix(value, sourceBase) {
				return localBase + strings.TrimPrefix(value, sourceBase)
			}
		}
		return value
	case []any:
		for i := range value {
			value[i] = sanitizeUpstreamURLs(value[i], upstreamBase, localBase)
		}
		return value
	case map[string]any:
		for key := range value {
			value[key] = sanitizeUpstreamURLs(value[key], upstreamBase, localBase)
		}
		return value
	default:
		return value
	}
}

func (h *Handler) writeTranslatedRepoAPI(w http.ResponseWriter, r *http.Request, request hfRepoAPIRequest, repo *upstreamRepo) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Repo-Commit", repo.Commit)
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	encoder := json.NewEncoder(w)
	if request.kind == hfAPIRepoInfo {
		siblings := make([]map[string]any, 0, len(repo.Files))
		for _, file := range repo.Files {
			if file.Type == "blob" {
				siblings = append(siblings, map[string]any{"rfilename": file.Path})
			}
		}
		_ = encoder.Encode(map[string]any{"id": request.repoID, "sha": repo.Commit, "siblings": siblings})
		return
	}

	entries := make([]map[string]any, 0, len(repo.Files))
	for _, file := range repo.Files {
		if request.path != "" && file.Path != request.path && !strings.HasPrefix(file.Path, request.path+"/") {
			continue
		}
		if file.Type != "blob" {
			continue
		}
		oid := file.Revision
		if !isCommitHash(oid) && !isSHA256(oid) {
			oid = syntheticCommit(file.Path + "\x00" + file.SHA256)
		}
		entries = append(entries, map[string]any{"type": "file", "oid": oid, "size": file.Size, "path": file.Path})
	}
	_ = encoder.Encode(entries)
}

func (h *Handler) cacheRepo(repoType, repoID, revision string, repo *upstreamRepo) {
	h.mu.Lock()
	h.repos[repoType+"/"+repoID+"@"+revision] = repo
	if isCommitHash(repo.Commit) {
		h.repos[repoType+"/"+repoID+"@"+repo.Commit] = repo
	}
	h.mu.Unlock()
}

type hfAPIKind uint8

const (
	hfAPIRepoInfo hfAPIKind = iota
	hfAPITree
)

type hfRepoAPIRequest struct {
	kind     hfAPIKind
	repoType string
	repoID   string
	revision string
	path     string
	bareInfo bool
}

func (r hfRepoAPIRequest) standardPath() string {
	segment, _ := hfRepoSegment(r.repoType)
	if r.kind == hfAPIRepoInfo && r.bareInfo {
		return "/api/" + segment + "/" + escapeRepoID(r.repoID)
	}
	marker := "revision"
	if r.kind == hfAPITree {
		marker = "tree"
	}
	path := "/api/" + segment + "/" + escapeRepoID(r.repoID) + "/" + marker + "/" + url.PathEscape(r.revision)
	if r.kind == hfAPITree && r.path != "" {
		path += "/" + url.PathEscape(r.path)
	}
	return path
}

func parseHFRepoAPIRequest(r *http.Request) (hfRepoAPIRequest, bool) {
	segments := strings.Split(strings.Trim(r.URL.EscapedPath(), "/"), "/")
	if len(segments) < 3 || segments[0] != "api" {
		return hfRepoAPIRequest{}, false
	}
	repoType := strings.TrimSuffix(segments[1], "s")
	if _, ok := hfRepoSegment(repoType); !ok {
		return hfRepoAPIRequest{}, false
	}
	if len(segments) == 3 || len(segments) == 4 {
		repoParts := make([]string, 0, len(segments)-2)
		for _, segment := range segments[2:] {
			decoded, err := url.PathUnescape(segment)
			if err != nil || decoded == "" {
				return hfRepoAPIRequest{}, false
			}
			repoParts = append(repoParts, decoded)
		}
		return hfRepoAPIRequest{
			kind: hfAPIRepoInfo, repoType: repoType, repoID: strings.Join(repoParts, "/"), revision: "main", bareInfo: true,
		}, true
	}
	marker := -1
	kind := hfAPIRepoInfo
	for i := 3; i < len(segments)-1; i++ {
		decoded, err := url.PathUnescape(segments[i])
		if err != nil {
			return hfRepoAPIRequest{}, false
		}
		if decoded == "revision" || decoded == "tree" {
			marker = i
			if decoded == "tree" {
				kind = hfAPITree
			}
			break
		}
	}
	if marker < 3 || marker+1 >= len(segments) {
		return hfRepoAPIRequest{}, false
	}
	repoParts := make([]string, 0, marker-2)
	for _, segment := range segments[2:marker] {
		decoded, err := url.PathUnescape(segment)
		if err != nil || decoded == "" {
			return hfRepoAPIRequest{}, false
		}
		repoParts = append(repoParts, decoded)
	}
	revision, err := url.PathUnescape(segments[marker+1])
	if err != nil || revision == "" {
		return hfRepoAPIRequest{}, false
	}
	path := ""
	if kind == hfAPITree && marker+2 < len(segments) {
		path, err = url.PathUnescape(strings.Join(segments[marker+2:], "/"))
		if err != nil {
			return hfRepoAPIRequest{}, false
		}
	}
	return hfRepoAPIRequest{kind: kind, repoType: repoType, repoID: strings.Join(repoParts, "/"), revision: revision, path: strings.Trim(path, "/")}, true
}

func hfRepoSegment(repoType string) (string, bool) {
	switch repoType {
	case "model":
		return "models", true
	case "dataset":
		return "datasets", true
	case "space":
		return "spaces", true
	default:
		return "", false
	}
}

func legacyRepoSegment(repoType string) (string, bool) {
	if repoType == "space" {
		return "studios", true
	}
	return hfRepoSegment(repoType)
}

func escapeRepoID(repoID string) string {
	parts := strings.Split(repoID, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}

func isCommitHash(value string) bool {
	return len(value) == 40 && isHex(value)
}

func isSHA256(value string) bool {
	return len(value) == 64 && isHex(value)
}

func isHex(value string) bool {
	_, err := hex.DecodeString(value)
	return err == nil
}

func syntheticCommit(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])[:40]
}

func syntheticRepoCommit(target target) string {
	if isCommitHash(target.revision) {
		return strings.ToLower(target.revision)
	}
	return syntheticCommit(target.repoType + "\x00" + target.repoID + "\x00" + target.revision)
}

func quoteETag(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
		return value
	}
	return strconv.Quote(value)
}
