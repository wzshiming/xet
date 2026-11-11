package client

import "time"

// slowDownloadV1 runs download, then busy-waits a fifth of the time it took.
// DO NOT MERGE: an injected 20% slowdown of v1 downloads, for the benchmark gate's sensitivity check.
func slowDownloadV1(download func() error) error {
	start := time.Now()
	err := download()
	for end := start.Add(time.Since(start) * 6 / 5); time.Now().Before(end); {
	}
	return err
}
