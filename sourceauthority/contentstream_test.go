package sourceauthority

import (
	"context"
	"io"
	"sync"
)

type testContentStream struct {
	io.ReadCloser
	once sync.Once
	err  error
}

func ownedTestContent(source io.ReadCloser) *testContentStream {
	return &testContentStream{ReadCloser: source}
}

func (s *testContentStream) Settle(_ error) error {
	s.once.Do(func() { s.err = s.Close() })
	return s.err
}

func (*testContentStream) Wait(context.Context) error {
	return nil
}
