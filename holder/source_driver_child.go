package holder

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/sourcedriverproto"
	"github.com/yasyf/fusekit/sourcedriverservice"
)

const sourceDriverChildMode = "--fusekit-source-driver-v1"

// SourceDriverInvocation is the immutable FuseKit-owned identity presented to a product driver registry.
type SourceDriverInvocation struct {
	DriverID            string
	Authority           causal.SourceAuthorityID
	FleetOwner          catalog.SourceAuthorityFleetOwnerID
	AuthorityGeneration causal.Generation
	DeclarationDigest   [32]byte
	DriverConfig        []byte
	TargetsDigest       [32]byte
}

// SourceDriverRegistry validates the exact invocation before resolving a product driver.
type SourceDriverRegistry interface {
	SourceDriver(context.Context, SourceDriverInvocation) (sourcedriver.Driver, error)
}

// DriverFactory resolves exactly one physical policy or semantic driver kind.
type DriverFactory struct {
	Physical func(context.Context, sourceauthority.SourceTaskIdentity) (sourceauthority.AuthorityPolicy, error)
	Semantic func(context.Context, SourceDriverInvocation) (sourcedriver.Driver, error)
}

// DriverFactories is one immutable product registry keyed by durable DriverID.
type DriverFactories struct {
	entries map[string]DriverFactory
}

// NewDriverFactories validates and freezes one product driver registry.
func NewDriverFactories(entries map[string]DriverFactory) (DriverFactories, error) {
	result := DriverFactories{entries: make(map[string]DriverFactory, len(entries))}
	for id, factory := range entries {
		if catalog.ValidateSourceDriverID(id) != nil || (factory.Physical == nil) == (factory.Semantic == nil) {
			return DriverFactories{}, errors.New("holder: each DriverID requires exactly one physical or semantic factory")
		}
		result.entries[id] = factory
	}
	return result, nil
}

func (r DriverFactories) physical(
	ctx context.Context,
	identity sourceauthority.SourceTaskIdentity,
) (sourceauthority.AuthorityPolicy, error) {
	identity.DriverConfig = append([]byte(nil), identity.DriverConfig...)
	factory, ok := r.entries[identity.DriverID]
	if !ok || factory.Physical == nil {
		return nil, fmt.Errorf("holder: unknown physical DriverID %q", identity.DriverID)
	}
	policy, err := factory.Physical(ctx, identity)
	if err != nil {
		return nil, err
	}
	if nilAuthorityPolicy(policy) {
		return nil, errors.New("holder: physical DriverID returned no policy")
	}
	return policy, nil
}

// SourceDriver resolves one exact semantic invocation.
func (r DriverFactories) SourceDriver(
	ctx context.Context,
	invocation SourceDriverInvocation,
) (sourcedriver.Driver, error) {
	invocation.DriverConfig = append([]byte(nil), invocation.DriverConfig...)
	factory, ok := r.entries[invocation.DriverID]
	if !ok || factory.Semantic == nil {
		return nil, fmt.Errorf("holder: unknown semantic DriverID %q", invocation.DriverID)
	}
	driver, err := factory.Semantic(ctx, invocation)
	if err != nil {
		return nil, err
	}
	if driver == nil {
		return nil, errors.New("holder: semantic DriverID returned no driver")
	}
	return driver, nil
}

func (r DriverFactories) sourceFleet(
	ctx context.Context,
	topology desiredTopology,
) (SourceAuthorityFleet, error) {
	fleet := SourceAuthorityFleet{Owner: topology.Head.Owner}
	if topology.Head.Fleet == nil {
		return fleet, nil
	}
	fleet.Generation = topology.Head.Fleet.Generation
	fleet.Authorities = make([]SourceAuthoritySpec, 0, len(topology.Authorities))
	for _, declaration := range topology.Authorities {
		factory, ok := r.entries[declaration.DriverID]
		if !ok {
			return SourceAuthorityFleet{}, fmt.Errorf(
				"holder: unknown DriverID %q for authority %q",
				declaration.DriverID, declaration.Authority,
			)
		}
		if factory.Physical != nil {
			identity := sourceauthority.SourceTaskIdentity{
				Owner: topology.Head.Owner, FleetGeneration: declaration.FleetGeneration,
				Authority: declaration.Authority, AuthorityGeneration: declaration.FleetGeneration,
				DriverID: declaration.DriverID, DeclarationDigest: declaration.DeclarationDigest,
				DriverConfig: append([]byte(nil), declaration.DriverConfig...),
			}
			policy, err := r.physical(ctx, identity)
			if err != nil {
				return SourceAuthorityFleet{}, fmt.Errorf(
					"holder: resolve authority %q: %w", declaration.Authority, err,
				)
			}
			fleet.Authorities = append(fleet.Authorities, PhysicalSourceSpec{
				Authority: declaration.Authority, DeclarationDigest: declaration.DeclarationDigest,
				DriverID: declaration.DriverID, DriverConfig: append([]byte(nil), declaration.DriverConfig...),
				Policy: policy,
			})
			continue
		}
		fleet.Authorities = append(fleet.Authorities, SemanticDriverSpec{
			Authority: declaration.Authority, DeclarationDigest: declaration.DeclarationDigest,
			DriverID: declaration.DriverID, DriverConfig: append([]byte(nil), declaration.DriverConfig...),
		})
	}
	return fleet, nil
}

type sourceDriverChildInvocation struct {
	SourceDriverInvocation
}

func sourceDriverChildArguments(
	fleet SourceAuthorityFleet,
	spec SemanticDriverSpec,
	targets []catalog.SourceDriverTarget,
) ([]string, error) {
	targetsDigest, err := catalog.SourceDriverTargetsDigest(targets)
	if err != nil {
		return nil, err
	}
	invocation := sourceDriverChildInvocation{
		SourceDriverInvocation: SourceDriverInvocation{
			DriverID: spec.DriverID, Authority: spec.Authority, FleetOwner: fleet.Owner,
			AuthorityGeneration: fleet.Generation, DeclarationDigest: spec.DeclarationDigest,
			DriverConfig:  append([]byte(nil), spec.DriverConfig...),
			TargetsDigest: targetsDigest,
		},
	}
	if err := validateSourceDriverChildInvocation(invocation); err != nil {
		return nil, err
	}
	return []string{
		sourceDriverChildMode, string(invocation.FleetOwner),
		strconv.FormatUint(uint64(invocation.AuthorityGeneration), 10), string(invocation.Authority),
		hex.EncodeToString(invocation.DeclarationDigest[:]), hex.EncodeToString(invocation.TargetsDigest[:]),
		invocation.DriverID, base64.RawStdEncoding.EncodeToString(invocation.DriverConfig),
	}, nil
}

func validateSourceDriverChildInvocation(invocation sourceDriverChildInvocation) error {
	if catalog.ValidateSourceAuthorityFleetOwnerID(invocation.FleetOwner) != nil || invocation.AuthorityGeneration == 0 ||
		causal.ValidateSourceAuthorityID(invocation.Authority) != nil || invocation.DeclarationDigest == ([32]byte{}) ||
		invocation.TargetsDigest == ([32]byte{}) || catalog.ValidateSourceDriverID(invocation.DriverID) != nil ||
		len(invocation.DriverConfig) > catalog.SourceDriverConfigMaxBytes {
		return errors.New("holder: invalid source driver child invocation")
	}
	return nil
}

func parseSourceDriverChildArguments(arguments []string) (sourceDriverChildInvocation, bool, error) {
	if len(arguments) == 0 || arguments[0] != sourceDriverChildMode {
		return sourceDriverChildInvocation{}, false, nil
	}
	if len(arguments) != 8 {
		return sourceDriverChildInvocation{}, true, errors.New("holder: malformed source driver child invocation")
	}
	generation, err := strconv.ParseUint(arguments[2], 10, 64)
	if err != nil {
		return sourceDriverChildInvocation{}, true, errors.New("holder: malformed source driver child generation")
	}
	rawDeclarationDigest, err := hex.DecodeString(arguments[4])
	if err != nil || len(rawDeclarationDigest) != 32 {
		return sourceDriverChildInvocation{}, true, errors.New("holder: malformed source driver declaration digest")
	}
	rawTargetsDigest, err := hex.DecodeString(arguments[5])
	if err != nil || len(rawTargetsDigest) != 32 {
		return sourceDriverChildInvocation{}, true, errors.New("holder: malformed source driver targets digest")
	}
	driverConfig, err := base64.RawStdEncoding.DecodeString(arguments[7])
	if err != nil || len(driverConfig) > catalog.SourceDriverConfigMaxBytes {
		return sourceDriverChildInvocation{}, true, errors.New("holder: malformed source driver configuration")
	}
	invocation := sourceDriverChildInvocation{
		SourceDriverInvocation: SourceDriverInvocation{
			FleetOwner:          catalog.SourceAuthorityFleetOwnerID(arguments[1]),
			AuthorityGeneration: causal.Generation(generation), Authority: causal.SourceAuthorityID(arguments[3]),
			DriverID: arguments[6], DriverConfig: driverConfig,
		},
	}
	copy(invocation.DeclarationDigest[:], rawDeclarationDigest)
	copy(invocation.TargetsDigest[:], rawTargetsDigest)
	if err := validateSourceDriverChildInvocation(invocation); err != nil {
		return sourceDriverChildInvocation{}, true, err
	}
	return invocation, true, nil
}

func runSourceDriverChild(
	ctx context.Context,
	arguments []string,
	registry SourceDriverRegistry,
) (bool, error) {
	invocation, recognized, err := parseSourceDriverChildArguments(arguments)
	if !recognized || err != nil {
		return recognized, err
	}
	if registry == nil {
		return true, errors.New("holder: source driver child registry is required")
	}
	driver, err := registry.SourceDriver(ctx, invocation.SourceDriverInvocation)
	if err != nil {
		return true, fmt.Errorf("holder: resolve source driver %q: %w", invocation.DriverID, err)
	}
	if driver == nil {
		return true, fmt.Errorf("holder: source driver %q is nil", invocation.DriverID)
	}
	conn, err := wire.NewDuplexConn(os.Stdin, os.Stdout)
	if err != nil {
		return true, fmt.Errorf("holder: open source driver session: %w", err)
	}
	parent, err := wire.SpawnedParentSessionIdentity()
	if err != nil {
		_ = conn.Close()
		return true, err
	}
	server := &wire.Server{
		WireBuild: sourcedriverproto.Build, Workers: 1, Backlog: 1, MaxSessions: 1,
		InboundQueue: 4, OutboundQueue: 4, StreamQueue: 2,
	}
	if _, err := sourcedriverservice.Register(server, driver); err != nil {
		return true, err
	}
	admit := func() (func(), error) { return func() {}, nil }
	return true, server.ServeSession(ctx, conn, parent, func() error { return nil }, admit, admit)
}
