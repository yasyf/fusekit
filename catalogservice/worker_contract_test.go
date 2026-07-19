package catalogservice_test

import (
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/catalogworker"
)

var (
	_ catalogservice.CatalogReadStore     = (*catalogworker.Manager)(nil)
	_ catalogservice.CatalogMutationStore = (*catalogworker.Manager)(nil)
)
