package chaos_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// direction is one way through a proxied TCP connection.
type direction int

const (
	toServer direction = iota // from the client to the target
	toClient                  // from the target to the client
)

// cutMode is how a cut ends a connection.
type cutMode int

const (
	cutClose     cutMode = iota // FIN to the destination right after the last byte; the source is drained, then closed
	cutRst                      // reset both sides
	cutBlackhole                // forward nothing more either way and hold both sockets until closeExisting or Close
)

// tcpProxy is a netem-like TCP forwarder in front of one target. Each
// connection keeps the impairments armed when it was accepted; cuts count
// bytes, and only delay and rate use time.
type tcpProxy struct {
	ln        net.Listener
	target    string
	wg        sync.WaitGroup // the accept loop and every connection
	closeOnce sync.Once
	sent      [2]atomic.Int64 // payload forwarded per direction
	ncuts     atomic.Int64

	mu       sync.Mutex
	cond     *sync.Cond // broadcast on Close, maxConns changes and connections ending
	closed   bool
	links    [2]tcpLink // impairments for the connections accepted from now on
	refusing int        // connections left to refuse, negative for all
	limit    int        // maxConns, 0 for none
	live     map[*tcpConn]struct{}
	accepted int
	nrefused int

	rngMu sync.Mutex
	rng   *rand.Rand // jitter source, seeded fixed
}

// tcpLink is the impairment of one direction.
type tcpLink struct {
	delay, jitter time.Duration
	rate, piece   int // bytes per second and write size
	cut           *tcpCut
}

// tcpCut ends a connection once n bytes went one way; with once set, only the
// first connection to get there.
type tcpCut struct {
	n    int
	mode cutMode
	once *atomic.Bool
}

// tcpConn is one proxied connection.
type tcpConn struct {
	p        *tcpProxy
	links    [2]tcpLink
	client   *net.TCPConn
	ctx      context.Context // done once the connection is reset
	cancel   context.CancelFunc
	hole     chan struct{} // closed when a blackhole cut stops forwarding
	holeOnce sync.Once
	finished chan struct{} // closed once the connection is gone

	mu     sync.Mutex
	server *net.TCPConn
	closed bool
}

// chunk is one read from a source, free to leave at due.
type chunk struct {
	data   []byte
	due    time.Time
	eof    bool // the source half-closed
	failed bool // the source broke
}

// chunkQueue is an unbounded FIFO from a direction's reader to its writer, so
// reading never waits on delay or rate.
type chunkQueue struct {
	mu     sync.Mutex
	chunks []chunk
	ready  chan struct{} // holds a token after a push
}

func (q *chunkQueue) push(ch chunk) {
	q.mu.Lock()
	q.chunks = append(q.chunks, ch)
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

func (q *chunkQueue) take() (chunk, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.chunks) == 0 {
		return chunk{}, false
	}
	ch := q.chunks[0]
	q.chunks[0] = chunk{}
	q.chunks = q.chunks[1:]
	return ch, true
}

// newTCPProxy forwards a loopback listener to targetAddr until the test ends.
func newTCPProxy(t *testing.T, targetAddr string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{ln: ln, target: targetAddr, live: make(map[*tcpConn]struct{}), rng: rand.New(rand.NewPCG(1, 2))}
	p.cond = sync.NewCond(&p.mu)
	p.wg.Go(p.acceptLoop)
	t.Cleanup(p.Close)
	return p
}

func (p *tcpProxy) Addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) URL() string { return "http://" + p.Addr() }

// delay holds every chunk going dir for d plus a jitter drawn uniformly from [-jitter, +jitter].
func (p *tcpProxy) delay(dir direction, d, jitter time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.links[dir].delay, p.links[dir].jitter = d, jitter
}

// rate writes at most piece bytes at a time going dir and waits m/bytesPerSec after writing m bytes.
func (p *tcpProxy) rate(dir direction, bytesPerSec, piece int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.links[dir].rate, p.links[dir].piece = bytesPerSec, piece
}

// cutAfter cuts every connection once n bytes went dir.
func (p *tcpProxy) cutAfter(dir direction, n int, mode cutMode) {
	p.setCut(dir, &tcpCut{n: n, mode: mode})
}

// cutOnce cuts only the first connection to get n bytes through dir.
func (p *tcpProxy) cutOnce(dir direction, n int, mode cutMode) {
	p.setCut(dir, &tcpCut{n: n, mode: mode, once: new(atomic.Bool)})
}

func (p *tcpProxy) setCut(dir direction, cut *tcpCut) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.links[dir].cut = cut
}

// refuse resets the next n accepted connections, or every one until clear when n < 0.
func (p *tcpProxy) refuse(n int) {
	if n == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refusing = n
}

// maxConns leaves connections in the backlog while n are open; 0 lifts the limit.
func (p *tcpProxy) maxConns(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.limit = n
	p.cond.Broadcast()
}

// clear disarms every impairment for the connections accepted from now on.
func (p *tcpProxy) clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.links = [2]tcpLink{}
	p.refusing, p.limit = 0, 0
	p.cond.Broadcast()
}

// closeExisting resets every open connection, blackholed ones included, and
// returns once they are gone; new connections are still accepted.
func (p *tcpProxy) closeExisting() {
	live := p.snapshot()
	for _, c := range live {
		c.abort()
	}
	for _, c := range live {
		<-c.finished
	}
}

// conns counts the connections accepted and proxied, refused ones aside.
func (p *tcpProxy) conns() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accepted
}

// open counts the proxied connections not gone yet, blackholed ones included.
func (p *tcpProxy) open() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.live)
}

// refused counts the connections reset by refuse.
func (p *tcpProxy) refused() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nrefused
}

// bytes is the payload forwarded in dir over every connection, HTTP heads included.
func (p *tcpProxy) bytes(dir direction) int64 { return p.sent[dir].Load() }

// cuts counts the cuts fired.
func (p *tcpProxy) cuts() int { return int(p.ncuts.Load()) }

// Close stops accepting, resets every open connection and waits for every forwarder.
func (p *tcpProxy) Close() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.cond.Broadcast()
		p.mu.Unlock()
		_ = p.ln.Close()
		for _, c := range p.snapshot() {
			c.abort()
		}
		p.wg.Wait()
	})
}

func (p *tcpProxy) snapshot() []*tcpConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Collect(maps.Keys(p.live))
}

func (p *tcpProxy) acceptLoop() {
	for p.waitSlot() {
		conn, err := p.ln.Accept()
		if err != nil || !p.admit(conn.(*net.TCPConn)) {
			return
		}
	}
}

// waitSlot holds the accept loop while maxConns connections are open and
// reports whether the proxy still runs.
func (p *tcpProxy) waitSlot() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.holdLocked()
	return !p.closed
}

func (p *tcpProxy) holdLocked() {
	for !p.closed && p.limit > 0 && len(p.live) >= p.limit {
		p.cond.Wait()
	}
}

// admit refuses or starts an accepted connection and reports whether the proxy still runs.
func (p *tcpProxy) admit(client *net.TCPConn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.refusing != 0 {
		if p.refusing > 0 {
			p.refusing--
		}
		p.nrefused++
		p.wg.Go(func() { refuseConn(client) })
		return true
	}
	// maxConns may have dropped while Accept blocked: hold the connection as the backlog would.
	p.holdLocked()
	if p.closed {
		rstClose(client)
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &tcpConn{p: p, links: p.links, client: client, ctx: ctx, cancel: cancel, hole: make(chan struct{}), finished: make(chan struct{})}
	p.live[c] = struct{}{}
	p.accepted++
	p.wg.Go(c.run)
	return true
}

// refuseGrace bounds how long a refused connection waits for its client to
// write or close before the reset.
const refuseGrace = 50 * time.Millisecond

// refuseConn resets a refused connection. A reset sent right after Accept can
// beat the client's connect to completion and fail the dial itself, so it
// waits for the client's first byte or close, or refuseGrace at most.
func refuseConn(client *net.TCPConn) {
	_ = client.SetReadDeadline(time.Now().Add(refuseGrace))
	_, _ = client.Read(make([]byte, 1))
	rstClose(client)
}

// release forgets a finished connection and frees its maxConns slot.
func (p *tcpProxy) release(c *tcpConn) {
	c.cancel()
	p.mu.Lock()
	delete(p.live, c)
	p.cond.Broadcast()
	p.mu.Unlock()
	close(c.finished)
}

// lag is how long one chunk waits: d plus a jitter drawn uniformly from [-jitter, +jitter].
func (p *tcpProxy) lag(link tcpLink) time.Duration {
	if link.jitter <= 0 {
		return max(link.delay, 0)
	}
	p.rngMu.Lock()
	j := time.Duration(p.rng.Int64N(2*int64(link.jitter)+1)) - link.jitter
	p.rngMu.Unlock()
	return max(link.delay+j, 0)
}

func (c *tcpConn) run() {
	defer c.p.release(c)
	var dialer net.Dialer
	conn, err := dialer.DialContext(c.ctx, "tcp", c.p.target)
	if err != nil {
		c.abort()
		return
	}
	server := conn.(*net.TCPConn)
	if !c.attach(server) {
		return
	}
	srcs := [2]*net.TCPConn{toServer: c.client, toClient: server}
	dsts := [2]*net.TCPConn{toServer: server, toClient: c.client}
	var readers, writers sync.WaitGroup
	for _, dir := range []direction{toServer, toClient} {
		q := &chunkQueue{ready: make(chan struct{}, 1)}
		readers.Go(func() { c.pump(dir, srcs[dir], q) })
		writers.Go(func() { c.forward(dir, dsts[dir], q) })
	}
	writers.Wait()
	if c.holed() {
		<-c.ctx.Done()
	}
	c.closeBoth()
	readers.Wait()
}

// attach keeps the target side unless the connection was reset meanwhile.
func (c *tcpConn) attach(server *net.TCPConn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = server.Close()
		return false
	}
	c.server = server
	return true
}

// abort resets both sockets, which ends every forwarder of the connection.
func (c *tcpConn) abort() {
	c.cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	rstClose(c.client)
	if c.server != nil {
		rstClose(c.server)
	}
}

// closeBoth closes a connection both directions are done with.
func (c *tcpConn) closeBoth() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	_ = c.client.Close()
	_ = c.server.Close()
}

func (c *tcpConn) holed() bool {
	select {
	case <-c.hole:
		return true
	default:
		return false
	}
}

// pump queues what src sends, each read stamped with the time it may leave.
func (c *tcpConn) pump(dir direction, src *net.TCPConn, q *chunkQueue) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if c.holed() {
			return
		}
		due := time.Now().Add(c.p.lag(c.links[dir]))
		if n > 0 {
			q.push(chunk{data: bytes.Clone(buf[:n]), due: due})
		}
		if err != nil {
			eof := errors.Is(err, io.EOF)
			q.push(chunk{due: due, eof: eof, failed: !eof})
			return
		}
	}
}

// forward writes the queued chunks to dst once due, paced by rate, and fires
// the cut at its exact byte count.
func (c *tcpConn) forward(dir direction, dst *net.TCPConn, q *chunkQueue) {
	link := c.links[dir]
	cut := link.cut
	sent := 0
	var next time.Time // earliest next write under rate
	if cut != nil && cut.n <= 0 {
		if c.fire(dst, q, cut) {
			return
		}
		cut = nil
	}
	for {
		ch, ok := c.pop(q)
		if !ok || !c.sleepUntil(ch.due) {
			return
		}
		if ch.eof {
			_ = dst.CloseWrite()
			return
		}
		if ch.failed {
			c.abort()
			return
		}
		for data := ch.data; len(data) > 0; {
			m := len(data)
			if link.piece > 0 {
				m = min(m, link.piece)
			}
			if cut != nil {
				m = min(m, cut.n-sent)
			}
			if !c.sleepUntil(next) {
				return
			}
			k, err := dst.Write(data[:m])
			sent += k
			c.p.sent[dir].Add(int64(k))
			if err != nil {
				c.abort()
				return
			}
			if link.rate > 0 {
				next = time.Now().Add(time.Duration(k) * time.Second / time.Duration(link.rate))
			}
			data = data[k:]
			if cut != nil && sent >= cut.n {
				if c.fire(dst, q, cut) {
					return
				}
				cut = nil
			}
		}
	}
}

// fire applies cut and reports whether it did; a cutOnce another connection
// fired first is left alone.
func (c *tcpConn) fire(dst *net.TCPConn, q *chunkQueue, cut *tcpCut) bool {
	if cut.once != nil && !cut.once.CompareAndSwap(false, true) {
		return false
	}
	c.p.ncuts.Add(1)
	switch cut.mode {
	case cutClose:
		_ = dst.CloseWrite()
		c.discard(q)
	case cutRst:
		c.abort()
	case cutBlackhole:
		c.holeOnce.Do(func() { close(c.hole) })
	}
	return true
}

// discard drops what the source still sends until it closes.
func (c *tcpConn) discard(q *chunkQueue) {
	for {
		ch, ok := c.pop(q)
		switch {
		case !ok || ch.eof:
			return
		case ch.failed:
			c.abort()
			return
		}
	}
}

// pop waits for the next chunk; false once the connection is reset or blackholed.
func (c *tcpConn) pop(q *chunkQueue) (chunk, bool) {
	for {
		if ch, ok := q.take(); ok {
			return ch, true
		}
		select {
		case <-q.ready:
		case <-c.ctx.Done():
			return chunk{}, false
		case <-c.hole:
			return chunk{}, false
		}
	}
}

// sleepUntil waits for t and reports whether the connection still forwards.
func (c *tcpConn) sleepUntil(t time.Time) bool {
	if d := time.Until(t); d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-c.ctx.Done():
		case <-c.hole:
		}
	}
	return c.ctx.Err() == nil && !c.holed()
}
