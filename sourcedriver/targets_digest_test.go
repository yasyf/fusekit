package sourcedriver

import (
	"fmt"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func TestTargetsDigestStreamsMaximumDeclarationDeterministically(t *testing.T) {
	targets := make([]TargetDeclaration, MaxTargets)
	for index := range targets {
		targets[index] = TargetDeclaration{
			Tenant:     catalog.TenantID(fmt.Sprintf("target-%05d", index)),
			Generation: causal.Generation(index + 1),
		}
	}
	first, err := TargetsDigest(targets)
	if err != nil {
		t.Fatalf("TargetsDigest(10k): %v", err)
	}
	second, err := TargetsDigest(targets)
	if err != nil || second != first {
		t.Fatalf("TargetsDigest replay = %x, %v, want %x", second, err, first)
	}
	changed := append([]TargetDeclaration(nil), targets...)
	changed[len(changed)-1].Generation++
	changedDigest, err := TargetsDigest(changed)
	if err != nil || changedDigest == first {
		t.Fatalf("changed digest = %x, %v, want distinct from %x", changedDigest, err, first)
	}
}

func TestTargetsDigestRejectsOrderAndCountBounds(t *testing.T) {
	unordered := []TargetDeclaration{
		{Tenant: "b", Generation: 1},
		{Tenant: "a", Generation: 1},
	}
	if _, err := TargetsDigest(unordered); err == nil {
		t.Fatal("unordered targets accepted")
	}
	over := make([]TargetDeclaration, MaxTargets+1)
	for index := range over {
		over[index] = TargetDeclaration{
			Tenant: catalog.TenantID(fmt.Sprintf("target-%05d", index)), Generation: 1,
		}
	}
	if _, err := TargetsDigest(over); err == nil {
		t.Fatal("over-limit targets accepted")
	}
	left, err := TargetsDigest([]TargetDeclaration{{Tenant: "a", Generation: 1}, {Tenant: "bc", Generation: 2}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := TargetsDigest([]TargetDeclaration{{Tenant: "ab", Generation: 1}, {Tenant: "c", Generation: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatal("length-delimited target identities collided")
	}
}
