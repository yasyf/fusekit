package catalogservice

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogproto"
)

func TestBrokerCommandWindowBoundsProducerWithoutDropping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commands := make(chan catalogproto.BrokerCommand)
	input := make(chan wire.Chunk)
	output := make(chan []byte, maxOutstandingBrokerCommands+1)
	session := &windowBrokerSession{commands: commands}
	terminal := new(json.RawMessage)
	done := make(chan struct{})
	go func() {
		serveBroker(ctx, func() {}, input, session, output, terminal)
		close(done)
	}()

	produced := make(chan uint64, maxOutstandingBrokerCommands+2)
	go func() {
		for id := uint64(1); id <= maxOutstandingBrokerCommands+2; id++ {
			command := catalogproto.BrokerCommand{
				Protocol: catalogproto.Version, CommandID: id, Kind: catalogproto.BrokerCommandKindListDomains,
			}
			select {
			case commands <- command:
			case <-ctx.Done():
				return
			}
			select {
			case produced <- id:
			case <-ctx.Done():
				return
			}
		}
	}()
	for id := uint64(1); id <= maxOutstandingBrokerCommands; id++ {
		select {
		case got := <-produced:
			if got != id {
				t.Fatalf("produced command = %d, want %d", got, id)
			}
		case <-time.After(time.Second):
			t.Fatalf("producer stalled before window at %d", id)
		}
	}
	select {
	case id := <-produced:
		t.Fatalf("producer exceeded outstanding window with command %d", id)
	case <-time.After(20 * time.Millisecond):
	}

	domains := []catalogproto.RegisteredDomain{}
	result := catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: 1, Kind: catalogproto.BrokerCommandKindListDomains, Domains: &domains,
	}
	payload, err := catalogproto.Encode(result)
	if err != nil {
		t.Fatalf("Encode(result): %v", err)
	}
	input <- wire.Chunk{Payload: payload}
	select {
	case id := <-produced:
		if id != maxOutstandingBrokerCommands+1 {
			t.Fatalf("resumed command = %d", id)
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not resume after matched result")
	}
	session.mu.Lock()
	accepted := append([]uint64(nil), session.accepted...)
	session.mu.Unlock()
	if len(accepted) != 1 || accepted[0] != 1 {
		t.Fatalf("accepted results = %v", accepted)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("broker did not stop after cancellation")
	}
}

func TestBrokerCommandIDCannotBeReusedAfterSettlement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commands := make(chan catalogproto.BrokerCommand)
	input := make(chan wire.Chunk)
	output := make(chan []byte)
	session := &windowBrokerSession{commands: commands}
	terminal := new(json.RawMessage)
	done := make(chan struct{})
	go func() {
		serveBroker(ctx, func() {}, input, session, output, terminal)
		close(done)
	}()
	command := catalogproto.BrokerCommand{
		Protocol: catalogproto.Version, CommandID: 1, Kind: catalogproto.BrokerCommandKindListDomains,
	}
	commands <- command
	<-output
	domains := []catalogproto.RegisteredDomain{}
	result := catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: 1, Kind: catalogproto.BrokerCommandKindListDomains, Domains: &domains,
	}
	payload, err := catalogproto.Encode(result)
	if err != nil {
		t.Fatalf("Encode(result): %v", err)
	}
	input <- wire.Chunk{Payload: payload}
	deadline := time.Now().Add(time.Second)
	for {
		session.mu.Lock()
		accepted := len(session.accepted)
		session.mu.Unlock()
		if accepted == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first result was not accepted")
		}
		time.Sleep(time.Millisecond)
	}
	commands <- command
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reused command id did not close broker session")
	}
	var response catalogproto.BrokerOpenResponse
	if err := catalogproto.Decode(*terminal, &response); err != nil {
		t.Fatalf("Decode(terminal): %v", err)
	}
	if response.Code != catalogproto.ErrorCodeIntegrity {
		t.Fatalf("terminal code = %q, want integrity", response.Code)
	}
}

type windowBrokerSession struct {
	commands <-chan catalogproto.BrokerCommand
	mu       sync.Mutex
	accepted []uint64
}

func (s *windowBrokerSession) Commands() <-chan catalogproto.BrokerCommand { return s.commands }

func (s *windowBrokerSession) AcceptResult(_ context.Context, result catalogproto.BrokerResult) error {
	s.mu.Lock()
	s.accepted = append(s.accepted, result.CommandID)
	s.mu.Unlock()
	return nil
}

func (*windowBrokerSession) Close(error) {}
