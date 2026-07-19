package sourceauthority

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/yasyf/daemonkit/wire"
)

const (
	observerOpenPageItems       = 128
	observerOpenPageLimit       = maxObserverRoots + maxObserverCheckpoints
	observerOpenPageBytes       = 128 << 10
	observerOpenTotalBytes      = 32 << 20
	observerOpenChunk      byte = 1
)

type observerOpenManifest struct {
	Pages        uint32      `json:"pages"`
	EncodedBytes uint64      `json:"encoded_bytes"`
	Digest       Fingerprint `json:"digest"`
	Roots        uint32      `json:"roots"`
	Resume       uint32      `json:"resume"`
}

type observerOpenPage struct {
	Protocol uint16             `json:"protocol"`
	Cursor   uint32             `json:"cursor"`
	Previous Fingerprint        `json:"previous"`
	Digest   Fingerprint        `json:"digest"`
	Roots    []RootSpec         `json:"roots,omitempty"`
	Resume   []StreamCheckpoint `json:"resume,omitempty"`
}

func planObserverOpenPages(roots []RootSpec, resume []StreamCheckpoint) (observerOpenManifest, error) {
	var manifest observerOpenManifest
	var previous Fingerprint
	err := emitObserverOpenBodies(roots, resume, func(page observerOpenPage) error {
		page.Cursor, page.Previous = manifest.Pages, previous
		encoded, err := encodeObserverOpenPage(&page)
		if err != nil {
			return err
		}
		manifest.Pages++
		manifest.EncodedBytes += uint64(len(encoded))
		manifest.Roots += uint32(len(page.Roots))
		manifest.Resume += uint32(len(page.Resume))
		previous = page.Digest
		if manifest.EncodedBytes > observerOpenTotalBytes {
			return errors.New("sourceauthority: observer open pages exceed their byte limit")
		}
		return nil
	})
	manifest.Digest = previous
	if err != nil {
		return observerOpenManifest{}, err
	}
	if err := validateObserverOpenManifest(manifest); err != nil {
		return observerOpenManifest{}, err
	}
	return manifest, nil
}

func sendObserverOpenPages(
	ctx context.Context,
	call *wire.ClientCall,
	roots []RootSpec,
	resume []StreamCheckpoint,
	manifest observerOpenManifest,
) error {
	var actual observerOpenManifest
	var previous Fingerprint
	err := emitObserverOpenBodies(roots, resume, func(page observerOpenPage) error {
		page.Cursor, page.Previous = actual.Pages, previous
		encoded, err := encodeObserverOpenPage(&page)
		if err != nil {
			return err
		}
		if err := call.SendChunk(ctx, encodeStreamChunk(observerOpenChunk, page.Cursor, encoded)); err != nil {
			return err
		}
		actual.Pages++
		actual.EncodedBytes += uint64(len(encoded))
		actual.Roots += uint32(len(page.Roots))
		actual.Resume += uint32(len(page.Resume))
		previous = page.Digest
		return nil
	})
	actual.Digest = previous
	if err != nil {
		return err
	}
	if actual != manifest {
		return errors.New("sourceauthority: observer open inputs changed while streaming")
	}
	return call.CloseSend(ctx)
}

func receiveObserverOpenPages(
	ctx context.Context,
	chunks <-chan wire.Chunk,
	manifest observerOpenManifest,
) ([]RootSpec, []StreamCheckpoint, error) {
	if err := validateObserverOpenManifest(manifest); err != nil {
		return nil, nil, err
	}
	roots := make([]RootSpec, 0, manifest.Roots)
	resume := make([]StreamCheckpoint, 0, manifest.Resume)
	var actual observerOpenManifest
	var previous Fingerprint
	resumePhase := false
	for actual.Pages < manifest.Pages {
		var chunk wire.Chunk
		var ok bool
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case chunk, ok = <-chunks:
		}
		if !ok || chunk.End || len(chunk.Payload) < 5 || chunk.Payload[0] != observerOpenChunk ||
			binary.BigEndian.Uint32(chunk.Payload[1:5]) != actual.Pages {
			return nil, nil, errors.New("sourceauthority: observer open page sequence is invalid")
		}
		payload := chunk.Payload[5:]
		if len(payload) == 0 || len(payload) > observerOpenPageBytes {
			return nil, nil, errors.New("sourceauthority: observer open page exceeds its byte limit")
		}
		var page observerOpenPage
		if err := decodeObserver(payload, &page); err != nil {
			return nil, nil, err
		}
		provided := page.Digest
		page.Digest = Fingerprint{}
		if page.Protocol != fseventsObserverProtocol || page.Cursor != actual.Pages || page.Previous != previous {
			return nil, nil, errors.New("sourceauthority: observer open page identity is invalid")
		}
		encoded, err := encodeObserverOpenPage(&page)
		if err != nil || page.Digest != provided {
			return nil, nil, errors.New("sourceauthority: observer open page digest is invalid")
		}
		actual.Pages++
		actual.EncodedBytes += uint64(len(encoded))
		actual.Roots += uint32(len(page.Roots))
		actual.Resume += uint32(len(page.Resume))
		previous = page.Digest
		if len(page.Resume) != 0 {
			resumePhase = true
		} else if resumePhase {
			return nil, nil, errors.New("sourceauthority: observer root page followed a resume page")
		}
		roots = append(roots, page.Roots...)
		resume = append(resume, page.Resume...)
	}
	actual.Digest = previous
	if actual != manifest {
		return nil, nil, errors.New("sourceauthority: observer open terminal proof is invalid")
	}
	if err := finishSourceTaskInput(chunks); err != nil {
		return nil, nil, err
	}
	return roots, resume, nil
}

func emitObserverOpenBodies(
	roots []RootSpec,
	resume []StreamCheckpoint,
	yield func(observerOpenPage) error,
) error {
	if err := emitObserverOpenPages(len(roots), func(start, end int) observerOpenPage {
		return observerOpenPage{Protocol: fseventsObserverProtocol, Roots: roots[start:end]}
	}, yield); err != nil {
		return err
	}
	return emitObserverOpenPages(len(resume), func(start, end int) observerOpenPage {
		return observerOpenPage{Protocol: fseventsObserverProtocol, Resume: resume[start:end]}
	}, yield)
}

func emitObserverOpenPages(
	count int,
	page func(int, int) observerOpenPage,
	yield func(observerOpenPage) error,
) error {
	for start := 0; start < count; {
		maximum, end := min(start+observerOpenPageItems, count), 0
		if fits, err := observerOpenPageFits(page(start, maximum)); err == nil && fits {
			end = maximum
		} else {
			low, high := start+1, maximum-1
			for low <= high {
				middle := low + (high-low)/2
				fits, err := observerOpenPageFits(page(start, middle))
				if err != nil {
					high = middle - 1
					continue
				}
				if fits {
					end = middle
					low = middle + 1
				} else {
					high = middle - 1
				}
			}
		}
		if end == 0 {
			candidate := page(start, start+1)
			if _, err := encodeObserverOpenPage(&candidate); err != nil {
				return err
			}
			return errors.New("sourceauthority: observer open item exceeds its page byte limit")
		}
		if err := yield(page(start, end)); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func observerOpenPageFits(page observerOpenPage) (bool, error) {
	if (len(page.Roots) == 0) == (len(page.Resume) == 0) ||
		len(page.Roots) > observerOpenPageItems || len(page.Resume) > observerOpenPageItems {
		return false, errors.New("sourceauthority: observer open page shape is invalid")
	}
	if err := validateSourceTaskStrings(reflect.ValueOf(page)); err != nil {
		return false, err
	}
	var largest Fingerprint
	for index := range largest {
		largest[index] = 0xff
	}
	page.Protocol, page.Cursor, page.Previous, page.Digest =
		^uint16(0), ^uint32(0), largest, largest
	payload, err := json.Marshal(page)
	if err != nil {
		return false, err
	}
	return len(payload) <= observerOpenPageBytes, nil
}

func encodeObserverOpenPage(page *observerOpenPage) ([]byte, error) {
	if (len(page.Roots) == 0) == (len(page.Resume) == 0) ||
		len(page.Roots) > observerOpenPageItems || len(page.Resume) > observerOpenPageItems {
		return nil, errors.New("sourceauthority: observer open page shape is invalid")
	}
	if err := validateSourceTaskStrings(reflect.ValueOf(page)); err != nil {
		return nil, err
	}
	page.Digest = Fingerprint{}
	body, err := json.Marshal(page)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	_, _ = hash.Write(page.Previous[:])
	var cursor [4]byte
	binary.BigEndian.PutUint32(cursor[:], page.Cursor)
	_, _ = hash.Write(cursor[:])
	_, _ = hash.Write(body)
	copy(page.Digest[:], hash.Sum(nil))
	encoded, err := json.Marshal(page)
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 || len(encoded) > observerOpenPageBytes {
		return nil, errors.New("sourceauthority: observer open page exceeds its byte limit")
	}
	return encoded, nil
}

func validateObserverOpenManifest(manifest observerOpenManifest) error {
	minimumPages := observerOpenPageCount(manifest.Roots) + observerOpenPageCount(manifest.Resume)
	maximumPages := manifest.Roots + manifest.Resume
	if manifest.Roots == 0 || manifest.Roots > maxObserverRoots || manifest.Resume > maxObserverCheckpoints ||
		manifest.EncodedBytes == 0 || manifest.EncodedBytes > observerOpenTotalBytes ||
		manifest.Pages == 0 || manifest.Pages > observerOpenPageLimit ||
		manifest.Pages < minimumPages || manifest.Pages > maximumPages ||
		manifest.Digest == (Fingerprint{}) {
		return errors.New("sourceauthority: observer open manifest is invalid")
	}
	return nil
}

func observerOpenPageCount(count uint32) uint32 {
	return (count + observerOpenPageItems - 1) / observerOpenPageItems
}
