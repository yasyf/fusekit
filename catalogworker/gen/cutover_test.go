package main

import "testing"

func TestCatalogV1ActivationOutboxOperationCutover(t *testing.T) {
	names := make(map[string]struct{}, len(operations))
	for _, operation := range operations {
		if _, found := names[operation.name]; found {
			t.Fatalf("duplicate operation %q", operation.name)
		}
		names[operation.name] = struct{}{}
	}
	for _, name := range []string{
		"EnsureTenantNamespace", "SetTenantPresent", "SetTenantAbsent", "StageApplication",
		"RecordPresentation", "ActivateTenant", "RecoverTenantPreparations", "TenantTargetingRevision",
		"RetirePresentation", "RetireApplication", "ClearTenantActivation", "TenantLifecycle",
		"RecoverDeliveries", "ClaimDelivery", "RecordDelivery", "AcknowledgeDelivery",
		"QuarantineExpired", "ActivationPresentationTarget",
	} {
		if _, found := names[name]; !found {
			t.Errorf("current operation %q is absent", name)
		}
		if !generatedUnaryOperations[name] {
			t.Errorf("current operation %q is not generated", name)
		}
	}
	for _, name := range []string{
		"FileProviderSignalPlan", "CurrentConvergenceTarget", "ClaimConvergenceOutbox", "PageConvergenceOutbox",
		"SettleConvergenceOutbox", "ConvergenceEngineHead", "PageConvergenceEngine",
		"StageConvergenceEngineMutation", "PublishConvergenceEngineMutation",
		"DiscardUnpublishedConvergenceEngineMutations",
	} {
		if _, found := names[name]; found {
			t.Errorf("removed operation %q remains", name)
		}
		if mutatingOperations[name] || generatedUnaryOperations[name] {
			t.Errorf("removed operation %q remains in generator maps", name)
		}
	}
}
