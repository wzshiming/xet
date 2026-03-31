package download

import (
	"context"
	"fmt"
	"net/http"

	"github.com/wzshiming/xet/xorb"
)

// ClientAdapter provides access to client operations needed for reconstruction decoding
type ClientAdapter interface {
	DownloadXorb(ctx context.Context, url string, header http.Header) (*xorb.Xorb, error)
}

type options struct {
	concurrency int
}

func WithConcurrency(concurrency int) func(*options) {
	return func(o *options) {
		o.concurrency = concurrency
	}
}

func (o *options) concurrencyValue() int {
	if o == nil || o.concurrency <= 0 {
		return 1
	}
	return o.concurrency
}

type xorbFetchTask struct {
	key    string
	url    string
	header http.Header
}

type xorbPrefetcher struct {
	entries map[string]*xorbPrefetchEntry
}

type xorbPrefetchEntry struct {
	task  xorbFetchTask
	ready chan struct{}
	xorb  *xorb.Xorb
	err   error
}

func newXorbPrefetcher(ctx context.Context, client ClientAdapter, tasks []xorbFetchTask, concurrency int) *xorbPrefetcher {
	entries := make(map[string]*xorbPrefetchEntry, len(tasks))
	ordered := make([]*xorbPrefetchEntry, 0, len(tasks))
	for _, task := range tasks {
		if _, ok := entries[task.key]; ok {
			continue
		}
		entry := &xorbPrefetchEntry{
			task: xorbFetchTask{
				key:    task.key,
				url:    task.url,
				header: task.header.Clone(),
			},
			ready: make(chan struct{}),
		}
		entries[task.key] = entry
		ordered = append(ordered, entry)
	}

	if len(ordered) == 0 {
		return &xorbPrefetcher{entries: entries}
	}

	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(ordered) {
		concurrency = len(ordered)
	}

	queue := make(chan *xorbPrefetchEntry, len(ordered))
	for _, entry := range ordered {
		queue <- entry
	}
	close(queue)

	for range concurrency {
		go func() {
			for entry := range queue {
				entry.xorb, entry.err = client.DownloadXorb(ctx, entry.task.url, entry.task.header.Clone())
				close(entry.ready)
			}
		}()
	}

	return &xorbPrefetcher{entries: entries}
}

func (p *xorbPrefetcher) Wait(key string) (*xorb.Xorb, error) {
	entry, ok := p.entries[key]
	if !ok {
		return nil, fmt.Errorf("xorb fetch task %q not found", key)
	}

	<-entry.ready
	if entry.err != nil {
		return nil, entry.err
	}

	return entry.xorb, nil
}
