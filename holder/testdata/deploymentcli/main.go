package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/fusekit/holder"
)

func main() {
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	digest := codeidentity.PolicyDigest{1}
	plan, err := holder.NewDeploymentPlan(holder.DeploymentPlanSpec{
		Application: holder.SignedApplication{
			AppPath: filepath.Join(home, "Applications", "ProductHelper.app"), BundleID: "com.example.product", TeamID: "ABCDE12345",
			Broker:  holder.SignedExecutable{ExecutableName: "ProductHelper", SigningIdentifier: "com.example.product"},
			Runtime: holder.SignedExecutable{ExecutableName: "ProductHelper", SigningIdentifier: "com.example.product"},
		},
		RuntimeDirectory:    filepath.Join(home, "Library", "Application Support", "Product", "FuseKit"),
		PresentationRoot:    filepath.Join(home, "Library", "Application Support", "Product", "Files"),
		BuildID:             "example-build",
		Readiness:           holder.StandardReadinessContract(),
		SourceCapable:       true,
		BrokerPolicyDigest:  digest,
		RuntimePolicyDigest: digest,
	})
	broker, _ := plan.Broker()
	fmt.Println(broker.CodeIdentity, broker.PolicyDigest, err)
}
