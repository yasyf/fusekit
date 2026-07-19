package catalog

import (
	"crypto/sha256"
	"testing"
)

func TestProjectionFingerprintsSeparateMountOnlyFromFileProviderIdentity(t *testing.T) {
	content := sha256.Sum256([]byte("same bytes"))
	sourceObjects := []sourceFingerprintObject{
		{
			key: "file-a", parent: "root", name: "settings.json", kind: KindFile,
			mode: 0o600, size: 10, hash: content,
			visibility: Visibility{FileProvider: true},
		},
		{
			key: "mount-only", parent: "root", name: "notes.txt", kind: KindFile,
			mode: 0o600, size: 10, hash: content,
			visibility: Visibility{Mount: true},
		},
	}
	initialCatalog, err := sourceCatalogProjectionFingerprint(1, "root", sourceObjects)
	if err != nil {
		t.Fatal(err)
	}
	root := ObjectID{1}
	file := ObjectID{2}
	providerObjects := []fileProviderFingerprintObject{{
		id: file, parent: root, name: "settings.json", kind: KindFile,
		mode: 0o600, size: 10, hash: content,
	}}
	initialProvider, err := fileProviderProjectionFingerprint(root, providerObjects)
	if err != nil {
		t.Fatal(err)
	}

	mountChanged := append([]sourceFingerprintObject(nil), sourceObjects...)
	mountChanged[1].name = "renamed-notes.txt"
	mountCatalog, err := sourceCatalogProjectionFingerprint(1, "root", mountChanged)
	if err != nil {
		t.Fatal(err)
	}
	mountProvider, err := fileProviderProjectionFingerprint(root, providerObjects)
	if err != nil {
		t.Fatal(err)
	}
	if mountCatalog == initialCatalog || mountProvider != initialProvider {
		t.Fatalf("mount-only change proofs catalog=%x/%x file-provider=%x/%x",
			initialCatalog, mountCatalog, initialProvider, mountProvider)
	}

	renamed := append([]fileProviderFingerprintObject(nil), providerObjects...)
	renamed[0].name = "renamed-settings.json"
	renamedFileProvider, err := fileProviderProjectionFingerprint(root, renamed)
	if err != nil {
		t.Fatal(err)
	}
	if renamedFileProvider == initialProvider {
		t.Fatal("File Provider structural rename retained its old proof")
	}

	reidentified := append([]fileProviderFingerprintObject(nil), providerObjects...)
	reidentified[0].id = ObjectID{3}
	reidentifiedFileProvider, err := fileProviderProjectionFingerprint(root, reidentified)
	if err != nil {
		t.Fatal(err)
	}
	if reidentifiedFileProvider == initialProvider {
		t.Fatal("File Provider object identity change retained its old proof")
	}
}
