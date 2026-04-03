package download

import (
	"sync/atomic"
	"testing"
)

func TestOrderEntrySortsByURLPriorityThenByteRange(t *testing.T) {
	entries := []*xorbPrefetchEntry{
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 200, End: 300}, url: "url-B"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 0, End: 100}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 100, End: 200}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h2", Start: 0, End: 100}, url: "url-B"}},
	}

	// url-A entry 0-100 is needed first (order 0),
	// url-B entry 200-300 is needed second (order 1),
	// url-A entry 100-200 is needed third (order 2),
	// url-B entry 0-100 is needed fourth (order 3).
	orderMap := map[fetchKey]int{
		{Hash: "h1", Start: 0, End: 100}:   0,
		{Hash: "h1", Start: 200, End: 300}: 1,
		{Hash: "h1", Start: 100, End: 200}: 2,
		{Hash: "h2", Start: 0, End: 100}:   3,
	}

	orderEntry(entries, orderMap)

	// url-A has min order 0, url-B has min order 1.
	// url-A entries sorted by byte range: 0-100, 100-200.
	// url-B entries sorted by byte range: 0-100, 200-300.
	expected := []struct {
		url   string
		start int64
		end   int64
	}{
		{"url-A", 0, 100},
		{"url-A", 100, 200},
		{"url-B", 0, 100},
		{"url-B", 200, 300},
	}

	if len(entries) != len(expected) {
		t.Fatalf("expected %d entries, got %d", len(expected), len(entries))
	}

	for i, exp := range expected {
		got := entries[i]
		if got.task.url != exp.url || got.task.key.Start != exp.start || got.task.key.End != exp.end {
			t.Errorf("entry[%d]: expected url=%s start=%d end=%d, got url=%s start=%d end=%d",
				i, exp.url, exp.start, exp.end, got.task.url, got.task.key.Start, got.task.key.End)
		}
	}
}

func TestOrderEntrySameURLByteRangeOrder(t *testing.T) {
	entries := []*xorbPrefetchEntry{
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 300, End: 400}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 100, End: 200}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 0, End: 100}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 200, End: 300}, url: "url-A"}},
	}

	orderMap := map[fetchKey]int{
		{Hash: "h1", Start: 300, End: 400}: 0,
		{Hash: "h1", Start: 100, End: 200}: 1,
		{Hash: "h1", Start: 0, End: 100}:   2,
		{Hash: "h1", Start: 200, End: 300}: 3,
	}

	orderEntry(entries, orderMap)

	// All same URL, so sorted by byte range start.
	for i := 1; i < len(entries); i++ {
		if entries[i].task.key.Start < entries[i-1].task.key.Start {
			t.Errorf("entry[%d] start=%d < entry[%d] start=%d: not sorted by byte range",
				i, entries[i].task.key.Start, i-1, entries[i-1].task.key.Start)
		}
	}
}

func TestBuildJobsGroupsByURL(t *testing.T) {
	entries := []*xorbPrefetchEntry{
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 0, End: 100}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 100, End: 200}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h2", Start: 0, End: 100}, url: "url-B"}},
	}

	orderMap := map[fetchKey]int{
		{Hash: "h1", Start: 0, End: 100}:   0,
		{Hash: "h1", Start: 100, End: 200}: 1,
		{Hash: "h2", Start: 0, End: 100}:   2,
	}

	var active atomic.Int32
	p := &xorbPrefetcher{}
	var jobs []*xorbPrefetchJob
	p.buildJobs(entries, orderMap, 1, &active, func(job *xorbPrefetchJob) {
		jobs = append(jobs, job)
	})

	// With concurrency=1 and active=0, batchSize = remaining/1 = remaining,
	// but limited by URL group boundary.
	// job1 = url-A (2 entries), job2 = url-B (1 entry).
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	if len(jobs[0].entries) != 2 {
		t.Errorf("job[0]: expected 2 entries, got %d", len(jobs[0].entries))
	}
	if len(jobs[1].entries) != 1 {
		t.Errorf("job[1]: expected 1 entry, got %d", len(jobs[1].entries))
	}
	if jobs[0].entries[0].task.url != "url-A" {
		t.Errorf("job[0] should be url-A, got %s", jobs[0].entries[0].task.url)
	}
	if jobs[1].entries[0].task.url != "url-B" {
		t.Errorf("job[1] should be url-B, got %s", jobs[1].entries[0].task.url)
	}
}

func TestBuildJobsDynamicBatchSize(t *testing.T) {
	entries := []*xorbPrefetchEntry{
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 0, End: 100}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 100, End: 200}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 200, End: 300}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 300, End: 400}, url: "url-A"}},
	}

	orderMap := map[fetchKey]int{
		{Hash: "h1", Start: 0, End: 100}:   0,
		{Hash: "h1", Start: 100, End: 200}: 1,
		{Hash: "h1", Start: 200, End: 300}: 2,
		{Hash: "h1", Start: 300, End: 400}: 3,
	}

	var active atomic.Int32
	p := &xorbPrefetcher{}
	var jobs []*xorbPrefetchJob
	p.buildJobs(entries, orderMap, 4, &active, func(job *xorbPrefetchJob) {
		jobs = append(jobs, job)
	})

	// With concurrency=4 and active=0, freeWorkers=4,
	// batchSize = remaining/4 = 1 per iteration.
	// All same URL, so 4 jobs of 1 entry each.
	if len(jobs) != 4 {
		t.Fatalf("expected 4 jobs, got %d", len(jobs))
	}
	for i, job := range jobs {
		if len(job.entries) != 1 {
			t.Errorf("job[%d]: expected 1 entry, got %d", i, len(job.entries))
		}
	}
}

func TestBuildJobsDynamicWithActiveWorkers(t *testing.T) {
	entries := []*xorbPrefetchEntry{
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 0, End: 100}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 100, End: 200}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 200, End: 300}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 300, End: 400}, url: "url-A"}},
	}

	orderMap := map[fetchKey]int{
		{Hash: "h1", Start: 0, End: 100}:   0,
		{Hash: "h1", Start: 100, End: 200}: 1,
		{Hash: "h1", Start: 200, End: 300}: 2,
		{Hash: "h1", Start: 300, End: 400}: 3,
	}

	var active atomic.Int32
	active.Store(3) // 3 workers are busy
	p := &xorbPrefetcher{}
	var jobs []*xorbPrefetchJob
	p.buildJobs(entries, orderMap, 4, &active, func(job *xorbPrefetchJob) {
		jobs = append(jobs, job)
	})

	// With concurrency=4 and active=3, freeWorkers=1,
	// batchSize = 4/1 = 4 (all in one batch).
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if len(jobs[0].entries) != 4 {
		t.Errorf("job[0]: expected 4 entries, got %d", len(jobs[0].entries))
	}
}

func TestBuildJobsURLGroupBoundary(t *testing.T) {
	// 3 entries from url-A, 2 from url-B, concurrency=2.
	entries := []*xorbPrefetchEntry{
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 0, End: 100}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 100, End: 200}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 200, End: 300}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h2", Start: 0, End: 100}, url: "url-B"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h2", Start: 100, End: 200}, url: "url-B"}},
	}

	orderMap := map[fetchKey]int{
		{Hash: "h1", Start: 0, End: 100}:   0,
		{Hash: "h1", Start: 100, End: 200}: 1,
		{Hash: "h1", Start: 200, End: 300}: 2,
		{Hash: "h2", Start: 0, End: 100}:   3,
		{Hash: "h2", Start: 100, End: 200}: 4,
	}

	var active atomic.Int32
	p := &xorbPrefetcher{}
	var jobs []*xorbPrefetchJob
	p.buildJobs(entries, orderMap, 2, &active, func(job *xorbPrefetchJob) {
		jobs = append(jobs, job)
	})

	// All entries in each job must share the same URL.
	for i, job := range jobs {
		url := job.entries[0].task.url
		for j, entry := range job.entries {
			if entry.task.url != url {
				t.Errorf("job[%d] entry[%d]: expected url=%s, got url=%s", i, j, url, entry.task.url)
			}
		}
	}

	// Verify total entries across all jobs equals input.
	total := 0
	for _, job := range jobs {
		total += len(job.entries)
	}
	if total != 5 {
		t.Errorf("expected 5 total entries across jobs, got %d", total)
	}
}

func TestBuildJobsEmpty(t *testing.T) {
	var active atomic.Int32
	p := &xorbPrefetcher{}
	var jobs []*xorbPrefetchJob
	p.buildJobs(nil, nil, 4, &active, func(job *xorbPrefetchJob) {
		jobs = append(jobs, job)
	})

	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs for empty input, got %d", len(jobs))
	}
}
