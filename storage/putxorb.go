package storage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

// PutXorb stores the xorb read from r through st while xorb.Validate checks it
// against xorbHash: st reads the bytes Validate consumes, then io.EOF or the
// validation error, so a rejected xorb is not stored.
func PutXorb(ctx context.Context, st Storage, xorbHash xet.XorbHash, r io.Reader) (bool, error) {
	v := &validatingReader{r: r, xorbHash: xorbHash}
	inserted, err := st.PutXorb(ctx, xorbHash, v)
	if verr := v.stop(); verr != nil {
		return false, fmt.Errorf("validate xorb: %w", verr)
	}
	return inserted, err
}

// validatingReader yields the bytes xorb.Validate consumes from r, starting
// Validate on the first Read.
type validatingReader struct {
	r        io.Reader
	xorbHash xet.XorbHash
	pr       *io.PipeReader
	done     chan struct{}
	err      error
}

func (v *validatingReader) Read(p []byte) (int, error) {
	if v.pr == nil {
		pr, pw := io.Pipe()
		v.pr, v.done = pr, make(chan struct{})
		go func() {
			defer close(v.done)
			v.err = xorb.Validate(io.TeeReader(v.r, pw), v.xorbHash)
			pw.CloseWithError(v.err)
		}()
	}
	return v.pr.Read(p)
}

// stop ends a validation the store stopped reading and returns its error,
// ignoring the one the stop itself causes.
func (v *validatingReader) stop() error {
	if v.pr == nil {
		return nil
	}
	v.pr.Close()
	<-v.done
	if errors.Is(v.err, io.ErrClosedPipe) {
		return nil
	}
	return v.err
}
