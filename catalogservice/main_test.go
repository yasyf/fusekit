package catalogservice

import (
	"fmt"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/trust"
)

func TestMain(m *testing.M) {
	if recognized, err := trust.RunVerifierChild(os.Args[1:], os.Stdout); recognized {
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
