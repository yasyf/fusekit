package catalogservice

import "testing"

// TestCloseBudgetsItsOwnTeardown proves the owned lane's teardown states its
// own deadline: daemonkit's Close refuses a context carrying none, and Close
// has no caller context to inherit one from.
func TestCloseBudgetsItsOwnTeardown(t *testing.T) {
	_, d := startCatalogServer(t, newFakeReader(1), &fakeMutations{})
	client, err := NewClient(d)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close owned catalog client: %v", err)
	}
}
