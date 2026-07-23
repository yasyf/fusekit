package tenant

import (
	"testing"

	"github.com/yasyf/fusekit/catalog"
)

func TestTenantSpecMountMetadataMatchesPresentationSet(t *testing.T) {
	base := TenantSpec{
		OwnerID: "owner", ID: "tenant", Backing: BackingSpec{Root: "/backing"},
		Content: ContentSource{ID: "source"},
		Traits: TenantTraits{
			Access: ReadWrite, CaseSensitivity: catalog.CaseSensitive,
			Presentations: catalog.PresentFileProvider,
		},
		FileProvider: FileProviderSpec{
			Enabled: true, PresentationInstanceID: "instance", DisplayName: "Account",
		},
		Generation: 1,
	}
	if err := base.validate(); err != nil {
		t.Fatalf("pathless File Provider spec: %v", err)
	}
	mountWithoutMetadata := base
	mountWithoutMetadata.Traits.Presentations = catalog.PresentMount
	mountWithoutMetadata.FileProvider = FileProviderSpec{}
	if err := mountWithoutMetadata.validate(); err == nil {
		t.Fatal("mount spec without Mount metadata validated")
	}
	fileProviderWithMount := base
	fileProviderWithMount.Mount = MountSpec{PresentationRoot: "/mount"}
	if err := fileProviderWithMount.validate(); err == nil {
		t.Fatal("File Provider-only spec with Mount metadata validated")
	}
}
