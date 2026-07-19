//go:build !darwin || !cgo

package sourceauthority

import (
	"context"
	"errors"
)

type platformFSEventsEngine struct{}

func (platformFSEventsEngine) Open(
	_ context.Context,
	roots []RootSpec,
	resume []StreamCheckpoint,
	_ DurableEventSink,
) (EventStream, error) {
	if _, err := validateFSEventsOpen(roots, resume); err != nil {
		return nil, err
	}
	return nil, errors.New("sourceauthority: FSEvents unavailable")
}

func newPlatformFSEventsEngine() EventBackend { return platformFSEventsEngine{} }
