package sourcedriverservice

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/transportproto"
)

// testSourceDriverSession is one in-process spawned lane: the same unary
// contract a handoff socketpair carries, without a child process behind it.
type testSourceDriverSession struct {
	handle   daemonkit.Handler
	contract daemonkit.Contract
	serveCtx context.Context
	settle   context.CancelFunc

	mu     sync.Mutex
	closed bool
}

func startTestSourceDriverSession(t *testing.T, driver sourcedriver.Driver) *Client {
	t.Helper()
	client, _, _ := startTestSourceDriverLane(t, driver)
	return client
}

// startTestSourceDriverLane also exposes the raw session lane and the server,
// so a test can drive exact wire messages and inspect the pinned state beside
// the client that shares them.
func startTestSourceDriverLane(t *testing.T, driver sourcedriver.Driver) (*Client, SessionClient, *Server) {
	t.Helper()
	service := newServer(driver)
	mux, err := transportproto.NewMux(spawnedDeadlines(spawnedServerDeadline), service.handlerSpecs()...)
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	serveCtx, settle := context.WithCancel(context.Background())
	session := &testSourceDriverSession{
		handle: mux.Handle, contract: SpawnedContract(), serveCtx: serveCtx, settle: settle,
	}
	t.Cleanup(func() {
		settle()
		if err := service.release(); err != nil {
			t.Errorf("release service: %v", err)
		}
	})
	client, err := NewClientOn(session)
	if err != nil {
		t.Fatalf("NewClientOn: %v", err)
	}
	return client, session, service
}

func (s *testSourceDriverSession) Call(ctx context.Context, op string, body []byte) (daemonkit.Reply, error) {
	if _, stated := ctx.Deadline(); !stated {
		return daemonkit.Reply{}, errors.New("test source driver session requires a call deadline")
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
	// A caller whose own context expired never learns the reply: delivery is
	// unknown, exactly as it is across a real session.
	if err := ctx.Err(); err != nil {
		return daemonkit.Reply{}, err
	}
	return daemonkit.Reply{Body: append([]byte(nil), reply.Body...)}, nil
}

func (s *testSourceDriverSession) Close(context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.settle()
	return nil
}
