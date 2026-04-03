package download

import (
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

func TestOrderEntryURLTieBreaker(t *testing.T) {
	// Two URLs with the same priority (both have min order 0).
	entries := []*xorbPrefetchEntry{
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 0, End: 100}, url: "url-B"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h2", Start: 0, End: 100}, url: "url-A"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h1", Start: 100, End: 200}, url: "url-B"}},
		{task: xorbFetchTask{key: fetchKey{Hash: "h2", Start: 100, End: 200}, url: "url-A"}},
	}

	orderMap := map[fetchKey]int{
		{Hash: "h2", Start: 0, End: 100}: 0,
		{Hash: "h1", Start: 0, End: 100}: 0,
	}

	orderEntry(entries, orderMap)

	// Both URLs have priority 0, so tie-break by URL string.
	// url-A < url-B, so url-A entries come first.
	// Entries within same URL are contiguous and sorted by byte range.
	expected := []struct {
		url   string
		start int64
	}{
		{"url-A", 0},
		{"url-A", 100},
		{"url-B", 0},
		{"url-B", 100},
	}

	if len(entries) != len(expected) {
		t.Fatalf("expected %d entries, got %d", len(expected), len(entries))
	}

	for i, exp := range expected {
		got := entries[i]
		if got.task.url != exp.url || got.task.key.Start != exp.start {
			t.Errorf("entry[%d]: expected url=%s start=%d, got url=%s start=%d",
				i, exp.url, exp.start, got.task.url, got.task.key.Start)
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

	p := &xorbPrefetcher{}
	var jobs []*xorbPrefetchJob
	p.buildJobs(entries, orderMap, func(job *xorbPrefetchJob) {
		jobs = append(jobs, job)
	})

	// One job per URL group: job1 = url-A (2 entries), job2 = url-B (1 entry).
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

func TestBuildJobsKeepsURLGroupIntact(t *testing.T) {
	// All entries for the same URL should be grouped into one job
	// to enable multipart batching and maximize bandwidth.
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

	p := &xorbPrefetcher{}
	var jobs []*xorbPrefetchJob
	p.buildJobs(entries, orderMap, func(job *xorbPrefetchJob) {
		jobs = append(jobs, job)
	})

	// All 4 entries share url-A, so they must be in a single job.
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if len(jobs[0].entries) != 4 {
		t.Errorf("job[0]: expected 4 entries, got %d", len(jobs[0].entries))
	}
}

func TestBuildJobsURLGroupBoundary(t *testing.T) {
	// 3 entries from url-A, 2 from url-B.
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

	p := &xorbPrefetcher{}
	var jobs []*xorbPrefetchJob
	p.buildJobs(entries, orderMap, func(job *xorbPrefetchJob) {
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

	// Two URL groups: url-A (3 entries) and url-B (2 entries).
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	if len(jobs[0].entries) != 3 {
		t.Errorf("job[0]: expected 3 entries, got %d", len(jobs[0].entries))
	}
	if len(jobs[1].entries) != 2 {
		t.Errorf("job[1]: expected 2 entries, got %d", len(jobs[1].entries))
	}
}

func TestBuildJobsEmpty(t *testing.T) {
	p := &xorbPrefetcher{}
	var jobs []*xorbPrefetchJob
	p.buildJobs(nil, nil, func(job *xorbPrefetchJob) {
		jobs = append(jobs, job)
	})

	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs for empty input, got %d", len(jobs))
	}
}
