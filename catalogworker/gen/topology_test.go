package main

import (
	"strings"
	"testing"
)

func TestTopologyOperationsGenerateRequestAndResponseValidation(t *testing.T) {
	source := render()
	for _, required := range []string{
		"catalog.ValidateSourceAuthorityFleetOwnerID(input.Owner)",
		"input.Request.Validate()",
		"response.Head.Validate(input.Owner)",
		"response.Page.Validate(input.Request)",
		"catalog.ValidateSourceAuthorityFleetOwnerID(owner)",
		"request.Validate()",
		"response.Head.Validate(owner)",
		"response.Page.Validate(request)",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("generated topology protocol is missing %q", required)
		}
	}
}
