package catalog_test

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourcedriver"
)

func TestSourceDriverTargetsDigestPortableVectors(t *testing.T) {
	t.Parallel()
	vectors := []struct {
		name     string
		targets  []catalog.SourceDriverTarget
		expected string
	}{
		{name: "one", targets: digestTargets(1), expected: "a8db1dc00716f660d121eb5c05e5d58f50fcc8300252691cbde0910eeb064a83"},
		{name: "page", targets: digestTargets(128), expected: "fb2107bd8894dd9d35f9fdda84e1c6aa8e8f866468cacd73b73d215912365828"},
		{name: "maximum", targets: digestTargets(10_000), expected: "8228bd4dc654503664688b33453f2ba02f9f130c3695244c34d4165c38568287"},
		{name: "ambiguous-a-bc", targets: namedDigestTargets("a", "bc"), expected: "75b4a9fdf8d147e891c2eb0faa279bb281cff78ab9c81ebd04f6757037412241"},
		{name: "ambiguous-ab-c", targets: namedDigestTargets("ab", "c"), expected: "c8c37edc87137b6aa7a0f63e858c2c0b2885ff4dae603d8552bb02cb4ec2e114"},
	}
	var ambiguous [2][32]byte
	for index, vector := range vectors {
		vector := vector
		t.Run(vector.name, func(t *testing.T) {
			catalogDigest, err := catalog.SourceDriverTargetsDigest(vector.targets)
			if err != nil {
				t.Fatal(err)
			}
			driverDigest, err := sourcedriver.TargetsDigest(driverDigestTargets(vector.targets))
			if err != nil {
				t.Fatal(err)
			}
			if catalogDigest != driverDigest {
				t.Fatalf("catalog digest %x differs from source driver digest %x", catalogDigest, driverDigest)
			}
			if got := hex.EncodeToString(catalogDigest[:]); got != vector.expected {
				t.Fatalf("portable v1 digest = %s, want %s", got, vector.expected)
			}
			if index >= 3 {
				ambiguous[index-3] = catalogDigest
			}
		})
	}
	if ambiguous[0] == ambiguous[1] {
		t.Fatal("length-delimited ambiguous tenant splits collided")
	}
}

func digestTargets(count int) []catalog.SourceDriverTarget {
	targets := make([]catalog.SourceDriverTarget, count)
	for index := range targets {
		targets[index] = catalog.SourceDriverTarget{
			Tenant:     catalog.TenantID(fmt.Sprintf("tenant-%05d", index)),
			Generation: catalog.Generation(index + 1),
		}
	}
	return targets
}

func namedDigestTargets(names ...string) []catalog.SourceDriverTarget {
	targets := make([]catalog.SourceDriverTarget, len(names))
	for index, name := range names {
		targets[index] = catalog.SourceDriverTarget{
			Tenant: catalog.TenantID(name), Generation: catalog.Generation(index + 1),
		}
	}
	return targets
}

func driverDigestTargets(targets []catalog.SourceDriverTarget) []sourcedriver.TargetDeclaration {
	values := make([]sourcedriver.TargetDeclaration, len(targets))
	for index, target := range targets {
		values[index] = sourcedriver.TargetDeclaration{
			Tenant: target.Tenant, Generation: causal.Generation(target.Generation),
		}
	}
	return values
}
