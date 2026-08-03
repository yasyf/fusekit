package sourceauthority

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
)

const (
	observerOpenPageItems  = 128
	observerOpenPageLimit  = maxObserverRoots + maxObserverCheckpoints
	observerOpenPageBytes  = 128 << 10
	observerOpenTotalBytes = 32 << 20
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

type observerStageResponse struct {
	Protocol uint16 `json:"protocol"`
	Cursor   uint32 `json:"cursor"`
}

// observerOpenStage accumulates the exact staged open configuration one page at
// a time, proving the same digest chain the streamed carrier proved inline.
type observerOpenStage struct {
	mu          sync.Mutex
	actual      observerOpenManifest
	previous    Fingerprint
	resumePhase bool
	roots       []RootSpec
	resume      []StreamCheckpoint
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
	caller spawnedCaller,
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
		body, err := caller.call(ctx, fseventsOpStage, encoded)
		if err != nil {
			return err
		}
		var response observerStageResponse
		if err := decodeObserver(body, &response); err != nil {
			return err
		}
		if response.Protocol != fseventsObserverProtocol || response.Cursor != page.Cursor {
			return errors.New("sourceauthority: observer open page acknowledgement is invalid")
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
		return errors.New("sourceauthority: observer open inputs changed while staging")
	}
	return nil
}

func (s *observerOpenStage) accept(payload []byte) (observerStageResponse, error) {
	if len(payload) == 0 || len(payload) > observerOpenPageBytes {
		return observerStageResponse{}, errors.New("sourceauthority: observer open page exceeds its byte limit")
	}
	var page observerOpenPage
	if err := decodeObserver(payload, &page); err != nil {
		return observerStageResponse{}, err
	}
	provided := page.Digest
	page.Digest = Fingerprint{}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.actual.Pages >= observerOpenPageLimit {
		return observerStageResponse{}, errors.New("sourceauthority: observer open pages exceed their page limit")
	}
	if page.Protocol != fseventsObserverProtocol || page.Cursor != s.actual.Pages || page.Previous != s.previous {
		return observerStageResponse{}, errors.New("sourceauthority: observer open page identity is invalid")
	}
	encoded, err := encodeObserverOpenPage(&page)
	if err != nil || page.Digest != provided {
		return observerStageResponse{}, errors.New("sourceauthority: observer open page digest is invalid")
	}
	if len(page.Resume) != 0 {
		s.resumePhase = true
	} else if s.resumePhase {
		return observerStageResponse{}, errors.New("sourceauthority: observer root page followed a resume page")
	}
	s.actual.Pages++
	s.actual.EncodedBytes += uint64(len(encoded))
	s.actual.Roots += uint32(len(page.Roots))
	s.actual.Resume += uint32(len(page.Resume))
	if s.actual.EncodedBytes > observerOpenTotalBytes {
		return observerStageResponse{}, errors.New("sourceauthority: observer open pages exceed their byte limit")
	}
	s.previous = page.Digest
	s.roots = append(s.roots, page.Roots...)
	s.resume = append(s.resume, page.Resume...)
	return observerStageResponse{Protocol: fseventsObserverProtocol, Cursor: page.Cursor}, nil
}

func (s *observerOpenStage) settle(manifest observerOpenManifest) ([]RootSpec, []StreamCheckpoint, error) {
	if err := validateObserverOpenManifest(manifest); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	actual := s.actual
	actual.Digest = s.previous
	if actual != manifest {
		return nil, nil, errors.New("sourceauthority: observer open terminal proof is invalid")
	}
	return s.roots, s.resume, nil
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
