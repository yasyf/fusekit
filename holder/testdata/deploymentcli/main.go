package main

import (
	"fmt"

	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/fusekit/holder"
)

func main() {
	digest := codeidentity.PolicyDigest{1}
	plan, err := holder.NewDeploymentPlan(holder.DeploymentPlanSpec{
		Application: holder.SignedApplication{
			AppPath: "/Applications/Example.app", BundleID: "com.example.product", TeamID: "ABCDE12345",
			Broker:  holder.SignedExecutable{ExecutableName: "Example", SigningIdentifier: "com.example.product"},
			Runtime: holder.SignedExecutable{ExecutableName: "Example", SigningIdentifier: "com.example.product"},
		},
		RuntimeDirectory:    "/Users/example/Library/Application Support/Example/FuseKit",
		PresentationRoot:    "/Users/example/Library/Application Support/Example/Files",
		BuildID:             "example-build",
		Readiness:           holder.StandardReadinessContract(),
		SourceCapable:       true,
		BrokerPolicyDigest:  digest,
		RuntimePolicyDigest: digest,
	})
	broker, _ := plan.Broker()
	fmt.Println(broker.CodeIdentity, broker.PolicyDigest, err)
}
