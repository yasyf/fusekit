package tenant

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/yasyf/fusekit/catalog"
)

const (
	materializationWorkerMode    = "fusekit-materialize-v1"
	materializationWorkerVersion = 1
)

// SourceMutationPlanner supplies the only product-specific worker planning in StandardPlanner.
type SourceMutationPlanner interface {
	PrepareSourceMutation(context.Context, SourceMutationStep) (SourceMutationWorker, error)
}

// StandardPlanner runs generic catalog materialization and delegates external source mutation planning.
type StandardPlanner struct {
	Executable     string
	CatalogPath    string
	SourceMutation SourceMutationPlanner
}

// PrepareSourceMutation delegates the external source operation without exposing catalog state.
func (p StandardPlanner) PrepareSourceMutation(ctx context.Context, step SourceMutationStep) (SourceMutationWorker, error) {
	if p.SourceMutation == nil {
		return SourceMutationWorker{}, errors.New("tenant: source mutation planner is required")
	}
	return p.SourceMutation.PrepareSourceMutation(ctx, step)
}

// PrepareMaterialization returns one exact read-only catalog verification worker.
func (p StandardPlanner) PrepareMaterialization(_ context.Context, _ Catalog, step MaterializationStep) (WorkerSpec, error) {
	if err := p.validate(); err != nil {
		return WorkerSpec{}, err
	}
	arguments, err := MaterializationWorkerArguments(MaterializationWorkerConfig{
		CatalogPath: p.CatalogPath, Tenant: step.Tenant.ID,
		Generation: step.Tenant.Generation, Revision: step.Revision,
	})
	if err != nil {
		return WorkerSpec{}, err
	}
	return WorkerSpec{Path: p.Executable, Args: arguments}, nil
}

// PrepareMountLifecycle returns no worker because mountmux owns presentation activation.
func (p StandardPlanner) PrepareMountLifecycle(context.Context, Catalog, MountLifecycleStep) (*WorkerSpec, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (p StandardPlanner) validate() error {
	if !exactAbsolutePath(p.Executable) || !exactAbsolutePath(p.CatalogPath) {
		return errors.New("tenant: standard planner paths must be exact and absolute")
	}
	return nil
}

// MaterializationWorkerConfig identifies one exact read-only catalog proof.
type MaterializationWorkerConfig struct {
	CatalogPath string             `json:"catalog_path"`
	Tenant      catalog.TenantID   `json:"tenant"`
	Generation  catalog.Generation `json:"generation"`
	Revision    catalog.Revision   `json:"revision"`
}

type materializationWorkerEnvelope struct {
	Protocol int                         `json:"protocol"`
	Config   MaterializationWorkerConfig `json:"config"`
}

// MaterializationWorkerArguments encodes one exact disposable child invocation.
func MaterializationWorkerArguments(config MaterializationWorkerConfig) ([]string, error) {
	if err := validateMaterializationWorkerConfig(config); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(materializationWorkerEnvelope{Protocol: materializationWorkerVersion, Config: config})
	if err != nil {
		return nil, fmt.Errorf("tenant: encode materialization worker arguments: %w", err)
	}
	return []string{materializationWorkerMode, base64.RawURLEncoding.EncodeToString(payload)}, nil
}

// ParseMaterializationWorkerArguments recognizes one exact disposable child invocation.
func ParseMaterializationWorkerArguments(arguments []string) (MaterializationWorkerConfig, bool, error) {
	if len(arguments) == 0 || arguments[0] != materializationWorkerMode {
		return MaterializationWorkerConfig{}, false, nil
	}
	if len(arguments) != 2 {
		return MaterializationWorkerConfig{}, true, errors.New("tenant: materialization worker arguments are invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(arguments[1])
	if err != nil {
		return MaterializationWorkerConfig{}, true, fmt.Errorf("tenant: decode materialization worker arguments: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var envelope materializationWorkerEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return MaterializationWorkerConfig{}, true, fmt.Errorf("tenant: decode materialization worker contract: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return MaterializationWorkerConfig{}, true, errors.New("tenant: materialization worker contract has trailing data")
	}
	if envelope.Protocol != materializationWorkerVersion {
		return MaterializationWorkerConfig{}, true, fmt.Errorf("tenant: materialization worker protocol %d is unsupported", envelope.Protocol)
	}
	if err := validateMaterializationWorkerConfig(envelope.Config); err != nil {
		return MaterializationWorkerConfig{}, true, err
	}
	return envelope.Config, true, nil
}

// RunMaterializationWorker verifies interested catalog content and writes its exact proof.
func RunMaterializationWorker(ctx context.Context, config MaterializationWorkerConfig, output io.Writer) error {
	if err := validateMaterializationWorkerConfig(config); err != nil {
		return err
	}
	if output == nil {
		return errors.New("tenant: materialization worker proof output is required")
	}
	if err := catalog.VerifyMaterialization(ctx, config.CatalogPath, config.Tenant, config.Generation, config.Revision); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(workerProof{
		Tenant: config.Tenant, Generation: config.Generation,
		Revision: config.Revision, Lane: LaneMaterialization,
	})
}

func validateMaterializationWorkerConfig(config MaterializationWorkerConfig) error {
	if !exactAbsolutePath(config.CatalogPath) || config.Tenant == "" || config.Generation == 0 || config.Revision == 0 ||
		strings.ContainsRune(string(config.Tenant), 0) {
		return errors.New("tenant: materialization worker contract is incomplete")
	}
	return nil
}

var _ Planner = StandardPlanner{}
