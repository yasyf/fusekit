package sourceauthority

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

// SourceTaskIdentity binds one physical child to an acknowledged topology declaration.
type SourceTaskIdentity struct {
	Owner               catalog.SourceAuthorityFleetOwnerID
	FleetGeneration     causal.Generation
	Authority           causal.SourceAuthorityID
	AuthorityGeneration causal.Generation
	DriverID            string
	DriverConfig        []byte
	DeclarationDigest   [32]byte
}

// SourceTaskChildConfig is one exact source-task child invocation.
type SourceTaskChildConfig struct {
	Socket           string
	JournalRoot      string
	Identity         SourceTaskIdentity
	InvocationDigest [32]byte
}

// SourceTaskChildArguments encodes the exact v1 topology-fenced physical-child invocation.
func SourceTaskChildArguments(socket, journalRoot string, identity SourceTaskIdentity) ([]string, error) {
	config := SourceTaskChildConfig{Socket: socket, JournalRoot: journalRoot, Identity: identity}
	if err := validateSourceTaskChildConfig(config, false); err != nil {
		return nil, err
	}
	config.InvocationDigest = sourceTaskInvocationDigest(config)
	return []string{
		sourceTaskChildArg, socket, journalRoot, string(identity.Owner),
		strconv.FormatUint(uint64(identity.FleetGeneration), 10), string(identity.Authority),
		strconv.FormatUint(uint64(identity.AuthorityGeneration), 10), identity.DriverID,
		base64.RawStdEncoding.EncodeToString(identity.DriverConfig),
		hex.EncodeToString(identity.DeclarationDigest[:]), hex.EncodeToString(config.InvocationDigest[:]),
	}, nil
}

// ParseSourceTaskChildArguments recognizes and decodes the exact v1 invocation.
func ParseSourceTaskChildArguments(arguments []string) (SourceTaskChildConfig, bool, error) {
	if len(arguments) == 0 || arguments[0] != sourceTaskChildArg {
		return SourceTaskChildConfig{}, false, nil
	}
	if len(arguments) != 11 {
		return SourceTaskChildConfig{}, true, errors.New("sourceauthority: invalid source task child invocation")
	}
	fleetGeneration, err := strconv.ParseUint(arguments[4], 10, 64)
	if err != nil {
		return SourceTaskChildConfig{}, true, errors.New("sourceauthority: invalid source task fleet generation")
	}
	authorityGeneration, err := strconv.ParseUint(arguments[6], 10, 64)
	if err != nil {
		return SourceTaskChildConfig{}, true, errors.New("sourceauthority: invalid source task authority generation")
	}
	driverConfig, err := base64.RawStdEncoding.DecodeString(arguments[8])
	if err != nil || len(driverConfig) > catalog.SourceDriverConfigMaxBytes {
		return SourceTaskChildConfig{}, true, errors.New("sourceauthority: invalid source task driver configuration")
	}
	declarationDigest, err := hex.DecodeString(arguments[9])
	if err != nil || len(declarationDigest) != sha256.Size {
		return SourceTaskChildConfig{}, true, errors.New("sourceauthority: invalid source task declaration digest")
	}
	invocationDigest, err := hex.DecodeString(arguments[10])
	if err != nil || len(invocationDigest) != sha256.Size {
		return SourceTaskChildConfig{}, true, errors.New("sourceauthority: invalid source task invocation digest")
	}
	config := SourceTaskChildConfig{
		Socket: arguments[1], JournalRoot: arguments[2],
		Identity: SourceTaskIdentity{
			Owner: catalog.SourceAuthorityFleetOwnerID(arguments[3]), FleetGeneration: causal.Generation(fleetGeneration),
			Authority: causal.SourceAuthorityID(arguments[5]), AuthorityGeneration: causal.Generation(authorityGeneration),
			DriverID: arguments[7], DriverConfig: driverConfig,
		},
	}
	copy(config.Identity.DeclarationDigest[:], declarationDigest)
	copy(config.InvocationDigest[:], invocationDigest)
	if err := validateSourceTaskChildConfig(config, true); err != nil {
		return SourceTaskChildConfig{}, true, err
	}
	return config, true, nil
}

func validateSourceTaskChildConfig(config SourceTaskChildConfig, requireDigest bool) error {
	socket, journalRoot := config.Socket, config.JournalRoot
	if !filepath.IsAbs(socket) || filepath.Clean(socket) != socket || len(socket) >= 100 {
		return errors.New("sourceauthority: source task child socket path is invalid")
	}
	if !filepath.IsAbs(journalRoot) || filepath.Clean(journalRoot) != journalRoot ||
		filepath.Dir(filepath.Dir(socket)) != journalRoot ||
		!strings.HasPrefix(filepath.Base(filepath.Dir(socket)), "source-task-") {
		return errors.New("sourceauthority: source task journal root is invalid")
	}
	identity := config.Identity
	if catalog.ValidateSourceAuthorityFleetOwnerID(identity.Owner) != nil ||
		identity.FleetGeneration == 0 || causal.ValidateSourceAuthorityID(identity.Authority) != nil ||
		identity.AuthorityGeneration == 0 || identity.AuthorityGeneration != identity.FleetGeneration ||
		catalog.ValidateSourceDriverID(identity.DriverID) != nil ||
		len(identity.DriverConfig) > catalog.SourceDriverConfigMaxBytes || identity.DeclarationDigest == ([32]byte{}) {
		return errors.New("sourceauthority: source task topology identity is invalid")
	}
	if requireDigest && (config.InvocationDigest == ([32]byte{}) || config.InvocationDigest != sourceTaskInvocationDigest(config)) {
		return errors.New("sourceauthority: source task invocation digest mismatch")
	}
	return nil
}

func sourceTaskInvocationDigest(config SourceTaskChildConfig) [32]byte {
	digest := sha256.New()
	writeSourceTaskDigest(digest, "fusekit.source-task.invocation.v1")
	writeSourceTaskDigest(digest, config.Socket)
	writeSourceTaskDigest(digest, config.JournalRoot)
	writeSourceTaskDigest(digest, string(config.Identity.Owner))
	writeSourceTaskDigest(digest, strconv.FormatUint(uint64(config.Identity.FleetGeneration), 10))
	writeSourceTaskDigest(digest, string(config.Identity.Authority))
	writeSourceTaskDigest(digest, strconv.FormatUint(uint64(config.Identity.AuthorityGeneration), 10))
	writeSourceTaskDigest(digest, config.Identity.DriverID)
	writeSourceTaskDigest(digest, string(config.Identity.DriverConfig))
	_, _ = digest.Write(config.Identity.DeclarationDigest[:])
	var result [32]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func writeSourceTaskDigest(digest hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write([]byte(value))
}
