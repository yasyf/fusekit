package sourceauthority

import (
	"context"
	"errors"
	"os"
	"sync"

	"github.com/yasyf/daemonkit"
)

// testSourceSession is one in-process spawned lane: the same unary contract a
// handoff socketpair carries, without a child process behind it.
type testSourceSession struct {
	handle    daemonkit.Handler
	contract  daemonkit.Contract
	dir       string
	serveCtx  context.Context
	dropServe context.CancelFunc

	mu     sync.Mutex
	closed bool

	closeOnce sync.Once
	closeErr  error
}

func startTestSourceSession(contract daemonkit.Contract, handle daemonkit.Handler) (*testSourceSession, error) {
	directory, err := os.MkdirTemp("", "fusekit-source-session-")
	if err != nil {
		return nil, err
	}
	serveCtx, dropServe := context.WithCancel(context.Background())
	return &testSourceSession{
		handle: handle, contract: contract, dir: directory,
		serveCtx: serveCtx, dropServe: dropServe,
	}, nil
}

func (s *testSourceSession) Call(ctx context.Context, op string, body []byte) (daemonkit.Reply, error) {
	if _, stated := ctx.Deadline(); !stated {
		return daemonkit.Reply{}, errors.New("test source session requires a call deadline")
	}
	if limit := daemonkit.MaxDetail(s.contract.MaxFrame); daemonkit.Bytes(len(body)) > limit {
		return daemonkit.Reply{}, daemonkit.ErrOversize
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return daemonkit.Reply{}, daemonkit.ErrLaneClosed
	}
	handlerCtx, dropHandler := context.WithCancel(ctx)
	defer dropHandler()
	defer context.AfterFunc(s.serveCtx, dropHandler)()
	reply, err := s.handle(handlerCtx, daemonkit.Request{Op: op, Body: append([]byte(nil), body...)})
	if err != nil {
		return daemonkit.Reply{}, err
	}
	return daemonkit.Reply{Body: append([]byte(nil), reply.Body...)}, nil
}

func (s *testSourceSession) Close(context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.dropServe()
	return nil
}

func (s *testSourceSession) settle() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.dropServe()
		s.closeErr = os.RemoveAll(s.dir)
	})
	return s.closeErr
}
