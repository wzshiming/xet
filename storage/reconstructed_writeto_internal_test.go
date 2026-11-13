package storage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/wzshiming/xet/shard"
)

var errWriteAfterCapture = errors.New("write failed after capturing the pipeline")

// pipeCapture fails every Write, first noting the pipeline of the file being written.
type pipeCapture struct {
	f    *reconstructedFile
	pipe *pipeline
}

func (w *pipeCapture) Write([]byte) (int, error) {
	w.pipe = w.f.pipe
	return 0, errWriteAfterCapture
}

// A failed Write leaves WriteTo's file without pipeline, frame or sticky
// error, and no frame of the pipeline it stopped holding a buffer.
func TestReconstructedFileWriteToFailedWriteReturnsBuffers(t *testing.T) {
	store, chunks, term := pipelineFixtureForTest(t)
	digest := sha256.Sum256(bytes.Join(chunks, nil))
	sh := &shard.Shard{Files: []shard.FileBlock{{
		Entries:     []shard.FileDataSequenceEntry{term},
		MetadataExt: &shard.FileMetadataExt{SHA256Hash: shard.NewSHA256Hash(digest)},
	}}}
	rc, err := NewReconstructedFile(t.Context(), store, sh, digest)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	wt, ok := rc.(io.WriterTo)
	if !ok {
		t.Fatalf("reconstructed file %T is no io.WriterTo; want a WriteTo writing its decoded chunks", rc)
	}
	f := rc.(*reconstructedFile)
	w := &pipeCapture{f: f}
	if n, err := wt.WriteTo(w); n != 0 || !errors.Is(err, errWriteAfterCapture) {
		t.Fatalf("WriteTo into a failing writer = %d, %v; want 0, %v", n, err, errWriteAfterCapture)
	}
	if w.pipe == nil {
		t.Fatal("WriteTo wrote with no pipeline running")
	}
	if f.pipe != nil || f.cur != nil || f.err != nil {
		t.Errorf("after the failed Write the file holds pipe=%t cur=%t err=%v; want no pipeline, no frame and no sticky error", f.pipe != nil, f.cur != nil, f.err)
	}
	assertFramesHoldNoBuffer(t, w.pipe)
}
