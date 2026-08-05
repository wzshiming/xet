package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/wzshiming/httpseek"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/upload"
)

type job struct {
	target target
	info   sourceInfo
	path   string

	mu               sync.Mutex
	downloaded       int64
	downloadComplete bool
	done             bool
	err              error
	notify           chan struct{}
	readers          int
	removeWhenIdle   bool
}

func (h *Handler) getOrStart(target target) (*job, error) {
	h.mu.Lock()
	if existing := h.jobs[target.cacheKey]; existing != nil {
		existing.acquireReader()
		h.mu.Unlock()
		return existing, nil
	}
	h.mu.Unlock()

	info, err := h.resolveSource(h.ctx, target)
	if err != nil {
		return nil, err
	}
	partialName := keyDigest(target.cacheKey) + "-" + keyDigest(info.identity) + ".part"
	partialPath := filepath.Join(h.partialDir(), partialName)
	stat, statErr := os.Stat(partialPath)
	downloaded := int64(0)
	if statErr == nil {
		downloaded = stat.Size()
	} else if !os.IsNotExist(statErr) {
		return nil, statErr
	}
	if info.size >= 0 && downloaded > info.size {
		if err := os.Truncate(partialPath, 0); err != nil {
			return nil, err
		}
		downloaded = 0
	}
	j := &job{
		target:     target,
		info:       info,
		path:       partialPath,
		downloaded: downloaded,
		notify:     make(chan struct{}),
	}

	h.mu.Lock()
	if existing := h.jobs[target.cacheKey]; existing != nil {
		existing.acquireReader()
		h.mu.Unlock()
		return existing, nil
	}
	j.acquireReader()
	h.jobs[target.cacheKey] = j
	h.wg.Add(1)
	h.mu.Unlock()
	go h.runJob(j)
	return j, nil
}

func (h *Handler) runJob(j *job) {
	defer h.wg.Done()
	err := h.download(j)
	if err == nil {
		j.markDownloadComplete()
		err = h.convert(j)
	}
	j.finish(err)
	h.mu.Lock()
	if h.jobs[j.target.cacheKey] == j {
		delete(h.jobs, j.target.cacheKey)
	}
	h.mu.Unlock()
}

func (h *Handler) download(j *job) error {
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Sync()
		_ = f.Close()
	}()
	if _, err := f.Seek(j.downloadedSize(), io.SeekStart); err != nil {
		return err
	}
	if j.info.size >= 0 && j.downloadedSize() == j.info.size {
		return nil
	}
	w := &notifyingWriteSeeker{file: f, job: j, offset: j.downloadedSize()}
	if j.info.kind == sourceXET {
		upstreamHTTP := cloneHTTPClient(h.httpClient)
		upstreamClient, err := client.NewClient(
			client.WithHTTPClient(upstreamHTTP),
			client.WithCacheDir(h.workDir()),
			client.WithConcurrency(h.concurrency),
		)
		if err != nil {
			return err
		}
		if err := upstreamClient.DownloadFileWithAuthProvider(h.ctx, j.info.provider, j.info.fileHash, w); err != nil {
			return fmt.Errorf("download upstream XET file: %w", err)
		}
	} else {
		req, err := http.NewRequestWithContext(h.ctx, http.MethodGet, j.target.upstreamURL, nil)
		if err != nil {
			return err
		}
		h.prepareUpstreamRequest(req)
		seeker := httpseek.NewSeekerWithHTTPClient(h.ctx, h.httpClient, req)
		defer seeker.Close()
		resume := j.downloadedSize()
		if resume != 0 {
			if _, err := seeker.Seek(resume, io.SeekStart); err != nil {
				return fmt.Errorf("resume upstream HTTP file: %w", err)
			}
		}
		reader := httpseek.NewMustReadSeeker(seeker, resume, func(retry int, err error) error {
			if ctxErr := h.ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if retry >= 5 {
				return err
			}
			return nil
		})
		if _, err := io.Copy(w, reader); err != nil {
			return fmt.Errorf("download upstream HTTP file: %w", err)
		}
	}
	if j.info.size >= 0 && j.downloadedSize() != j.info.size {
		return fmt.Errorf("downloaded size %d does not match upstream size %d", j.downloadedSize(), j.info.size)
	}
	return nil
}

func (h *Handler) convert(j *job) error {
	f, err := os.Open(j.path)
	if err != nil {
		return err
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		_ = f.Close()
		return fmt.Errorf("hash downloaded file: %w", err)
	}
	digest := hex.EncodeToString(hasher.Sum(nil))
	if expected := j.info.expectedSHA256; expected != "" && digest != expected {
		_ = f.Close()
		_ = os.Truncate(j.path, 0)
		return fmt.Errorf("downloaded SHA-256 %s does not match upstream %s", digest, expected)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return err
	}
	fileHash, err := upload.UploadFile(h.ctx, storageUploadAdapter{storage: h.storage}, f,
		upload.WithConcurrency(h.concurrency),
		upload.WithCacheDir(h.workDir()),
		upload.WithEnableSHA256(true),
	)
	_ = f.Close()
	if err != nil {
		return fmt.Errorf("convert downloaded file to XET: %w", err)
	}
	if j.info.kind == sourceXET && fileHash != j.info.fileHash {
		_ = os.Truncate(j.path, 0)
		return fmt.Errorf("converted file hash %s does not match upstream XET hash %s", fileHash.String(), j.info.fileHash.String())
	}
	entry := &cacheEntry{
		CacheKey: keyDigest(j.target.cacheKey),
		FileHash: fileHash.String(),
		SHA256:   digest,
		Size:     j.downloadedSize(),
		Header:   j.info.header,
	}
	if err := h.storeEntry(entry); err != nil {
		return fmt.Errorf("store mirror cache entry: %w", err)
	}
	j.scheduleRemove()
	return nil
}

func (j *job) updateDownloaded(size int64) {
	j.mu.Lock()
	if size > j.downloaded {
		j.downloaded = size
		j.signalLocked()
	}
	j.mu.Unlock()
}

func (j *job) downloadedSize() int64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.downloaded
}

func (j *job) markDownloadComplete() {
	j.mu.Lock()
	j.downloadComplete = true
	j.signalLocked()
	j.mu.Unlock()
}

func (j *job) finish(err error) {
	j.mu.Lock()
	j.done = true
	j.err = err
	j.signalLocked()
	j.mu.Unlock()
}

func (j *job) signalLocked() {
	close(j.notify)
	j.notify = make(chan struct{})
}

func (j *job) acquireReader() {
	j.mu.Lock()
	j.readers++
	j.mu.Unlock()
}

func (j *job) releaseReader() {
	remove := false
	j.mu.Lock()
	j.readers--
	if j.readers == 0 && j.removeWhenIdle {
		remove = true
	}
	j.mu.Unlock()
	if remove {
		_ = os.Remove(j.path)
	}
}

func (j *job) scheduleRemove() {
	remove := false
	j.mu.Lock()
	j.removeWhenIdle = true
	if j.readers == 0 {
		remove = true
	}
	j.mu.Unlock()
	if remove {
		_ = os.Remove(j.path)
	}
}

type notifyingWriteSeeker struct {
	file   *os.File
	job    *job
	offset int64
}

func (w *notifyingWriteSeeker) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	w.offset += int64(n)
	w.job.updateDownloaded(w.offset)
	return n, err
}

func (w *notifyingWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	position, err := w.file.Seek(offset, whence)
	if err == nil {
		w.offset = position
	}
	return position, err
}

type growingReadSeeker struct {
	ctx    context.Context
	job    *job
	file   *os.File
	offset int64
}

func newGrowingReadSeeker(ctx context.Context, j *job) (*growingReadSeeker, error) {
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &growingReadSeeker{ctx: ctx, job: j, file: f}, nil
}

func (r *growingReadSeeker) Read(p []byte) (int, error) {
	for {
		r.job.mu.Lock()
		available := r.job.downloaded - r.offset
		downloadComplete := r.job.downloadComplete
		done := r.job.done
		jobErr := r.job.err
		notify := r.job.notify
		r.job.mu.Unlock()
		if available > 0 {
			if int64(len(p)) > available {
				p = p[:available]
			}
			n, err := r.file.ReadAt(p, r.offset)
			r.offset += int64(n)
			if n != 0 && err == io.EOF {
				err = nil
			}
			return n, err
		}
		if downloadComplete {
			return 0, io.EOF
		}
		if done {
			if jobErr != nil {
				return 0, jobErr
			}
			return 0, io.EOF
		}
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case <-notify:
		}
	}
}

func (r *growingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	var position int64
	switch whence {
	case io.SeekStart:
		position = offset
	case io.SeekCurrent:
		position = r.offset + offset
	case io.SeekEnd:
		if r.job.info.size < 0 {
			return 0, fmt.Errorf("upstream size is not known")
		}
		position = r.job.info.size + offset
	default:
		return 0, fmt.Errorf("invalid seek whence %d", whence)
	}
	if position < 0 {
		return 0, fmt.Errorf("negative seek position")
	}
	r.offset = position
	return position, nil
}

func (r *growingReadSeeker) Close() error { return r.file.Close() }

func cloneHTTPClient(source *http.Client) *http.Client {
	cloned := *source
	if transport, ok := source.Transport.(*http.Transport); ok {
		cloned.Transport = transport.Clone()
	}
	return &cloned
}

func sha256ETag(etag string) (string, bool) {
	etag = strings.TrimSpace(etag)
	etag = strings.TrimPrefix(etag, "W/")
	etag = strings.Trim(etag, `"`)
	if len(etag) != sha256.Size*2 {
		return "", false
	}
	if _, err := hex.DecodeString(etag); err != nil {
		return "", false
	}
	return strings.ToLower(etag), true
}
