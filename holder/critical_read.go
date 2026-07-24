package holder

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/worker"
	"github.com/yasyf/fusekit/catalogproto"
)

const (
	criticalReadChildArgument = "--fusekit-critical-read-child"
	criticalReadInputLimit    = 256 << 10
	criticalReadTimeout       = 30 * time.Second
)

type criticalReadTask struct {
	Objects []criticalReadObject `json:"objects"`
}

type criticalReadObject struct {
	ObjectID        catalogproto.ObjectID `json:"object_id"`
	ObjectRevision  uint64                `json:"object_revision"`
	ContentRevision uint64                `json:"content_revision"`
	Size            uint64                `json:"size"`
	Hash            string                `json:"hash"`
	Path            string                `json:"path"`
}

type criticalReadResult struct {
	Objects []criticalReadObservation `json:"objects"`
}

type criticalReadObservation struct {
	ObjectID catalogproto.ObjectID `json:"object_id"`
	Size     uint64                `json:"size"`
	Hash     string                `json:"hash"`
}

func runCriticalReadChild(ctx context.Context, arguments []string, stdin io.Reader, stdout io.Writer) (bool, error) {
	if len(arguments) == 0 || arguments[0] != criticalReadChildArgument {
		return false, nil
	}
	if len(arguments) != 1 || stdin == nil || stdout == nil {
		return true, errors.New("FuseKit critical read child: invalid invocation")
	}
	decoder := json.NewDecoder(io.LimitReader(stdin, criticalReadInputLimit+1))
	decoder.DisallowUnknownFields()
	var task criticalReadTask
	if err := decoder.Decode(&task); err != nil {
		return true, fmt.Errorf("FuseKit critical read child: decode task: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return true, err
	}
	if err := validateCriticalReadTask(task); err != nil {
		return true, err
	}
	result := criticalReadResult{Objects: make([]criticalReadObservation, 0, len(task.Objects))}
	buffer := make([]byte, 64<<10)
	for _, object := range task.Objects {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		file, err := os.Open(object.Path)
		if err != nil {
			return true, fmt.Errorf("FuseKit critical read child: open %s: %w", object.ObjectID, err)
		}
		hash := sha256.New()
		read, readErr := io.CopyBuffer(hash, io.LimitReader(file, int64(object.Size)+1), buffer)
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return true, errors.Join(readErr, closeErr)
		}
		actual := hex.EncodeToString(hash.Sum(nil))
		if uint64(read) != object.Size || actual != object.Hash {
			return true, fmt.Errorf("FuseKit critical read child: object %s content changed", object.ObjectID)
		}
		result.Objects = append(result.Objects, criticalReadObservation{ObjectID: object.ObjectID, Size: uint64(read), Hash: actual})
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		return true, fmt.Errorf("FuseKit critical read child: encode result: %w", err)
	}
	return true, nil
}

func runCriticalPathReads(
	ctx context.Context,
	runner workerRunner,
	executable string,
	readiness catalogproto.CriticalReadinessProof,
	paths []catalogproto.CriticalMaterializationPath,
) ([sha256.Size]byte, error) {
	if runner == nil || !filepath.IsAbs(executable) || filepath.Clean(executable) != executable || strings.ContainsRune(executable, 0) {
		return [sha256.Size]byte{}, errors.New("FuseKit critical read: fixed runtime executable is invalid")
	}
	pathByID := make(map[catalogproto.ObjectID]string, len(paths))
	uniquePaths := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if _, exists := pathByID[path.ObjectID]; exists {
			return [sha256.Size]byte{}, errors.New("FuseKit critical read: duplicate materialization path")
		}
		if _, exists := uniquePaths[path.Path]; exists {
			return [sha256.Size]byte{}, errors.New("FuseKit critical read: duplicate user-visible path")
		}
		pathByID[path.ObjectID] = path.Path
		uniquePaths[path.Path] = struct{}{}
	}
	task := criticalReadTask{Objects: make([]criticalReadObject, 0, len(readiness.Objects))}
	for _, object := range readiness.Objects {
		path, ok := pathByID[object.ObjectID]
		if !ok {
			return [sha256.Size]byte{}, errors.New("FuseKit critical read: missing materialization path")
		}
		task.Objects = append(task.Objects, criticalReadObject{
			ObjectID: object.ObjectID, ObjectRevision: object.ObjectRevision,
			ContentRevision: object.ContentRevision, Size: object.Size, Hash: object.Hash, Path: path,
		})
	}
	sort.Slice(task.Objects, func(left, right int) bool { return task.Objects[left].ObjectID < task.Objects[right].ObjectID })
	if err := validateCriticalReadTask(task); err != nil {
		return [sha256.Size]byte{}, err
	}
	input, err := json.Marshal(task)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	if len(input) > criticalReadInputLimit {
		return [sha256.Size]byte{}, errors.New("FuseKit critical read: task exceeds bounded child input")
	}
	result, err := runner.Run(ctx, worker.CommandRequest{
		Path: executable, Dir: "/", Args: []string{criticalReadChildArgument},
		Env: workerChildEnvironment(os.Environ()), Stdin: input, TotalTimeout: criticalReadTimeout,
	})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("FuseKit critical read: run signed child: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(result.Stdout))
	decoder.DisallowUnknownFields()
	var observed criticalReadResult
	if err := decoder.Decode(&observed); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("FuseKit critical read: decode child result: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return [sha256.Size]byte{}, err
	}
	if len(observed.Objects) != len(task.Objects) {
		return [sha256.Size]byte{}, errors.New("FuseKit critical read: child result count changed")
	}
	digest := sha256.New()
	for index, object := range task.Objects {
		actual := observed.Objects[index]
		if actual.ObjectID != object.ObjectID || actual.Size != object.Size || actual.Hash != object.Hash {
			return [sha256.Size]byte{}, errors.New("FuseKit critical read: child result identity changed")
		}
		writeCriticalDigestString(digest, string(object.ObjectID))
		_ = binary.Write(digest, binary.BigEndian, object.ObjectRevision)
		_ = binary.Write(digest, binary.BigEndian, object.ContentRevision)
		_ = binary.Write(digest, binary.BigEndian, object.Size)
		writeCriticalDigestString(digest, actual.Hash)
	}
	var proof [sha256.Size]byte
	copy(proof[:], digest.Sum(nil))
	return proof, nil
}

func validateCriticalReadTask(task criticalReadTask) error {
	if len(task.Objects) == 0 || len(task.Objects) > 32 {
		return errors.New("FuseKit critical read child: object count is invalid")
	}
	prior := catalogproto.ObjectID("")
	for _, object := range task.Objects {
		if object.ObjectID == "" || object.ObjectRevision == 0 || object.ContentRevision == 0 ||
			object.Size > math.MaxInt64-1 || len(object.Hash) != sha256.Size*2 ||
			!filepath.IsAbs(object.Path) || filepath.Clean(object.Path) != object.Path ||
			strings.ContainsRune(object.Path, 0) || prior != "" && object.ObjectID <= prior {
			return errors.New("FuseKit critical read child: object identity is invalid")
		}
		if _, err := hex.DecodeString(object.Hash); err != nil {
			return errors.New("FuseKit critical read child: object hash is invalid")
		}
		prior = object.ObjectID
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("FuseKit critical read child: trailing input")
	}
	return nil
}

func writeCriticalDigestString(target io.Writer, value string) {
	_ = binary.Write(target, binary.BigEndian, uint32(len(value)))
	_, _ = io.WriteString(target, value)
}
