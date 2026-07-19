package holder

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/fusekit/catalogworker"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/sourceauthority"
)

// ChildConfig supplies fixed executable child-mode dependencies.
type ChildConfig struct {
	Stdout  io.Writer
	Drivers DriverFactories
}

// RunChild recognizes and runs one exact FuseKit child mode in the fixed signed executable.
func RunChild(ctx context.Context, arguments []string, config ChildConfig) (bool, error) {
	if recognized, err := trust.RunVerifierChild(arguments, config.Stdout); recognized {
		return true, err
	}
	if recognized, err := catalogworker.RunChild(ctx, arguments); recognized {
		return true, err
	}
	if config, recognized, err := mountmux.ParseNativeChildArguments(arguments); recognized {
		if err != nil {
			return true, err
		}
		return true, mountmux.RunNativeChild(ctx, config)
	}
	if recognized, err := sourceauthority.RunFSEventsObserverChild(ctx, arguments); recognized {
		return true, err
	}
	if recognized, err := runSourceDriverChild(ctx, arguments, config.Drivers); recognized {
		return true, err
	}
	if child, recognized, err := sourceauthority.ParseSourceTaskChildArguments(arguments); recognized {
		if err != nil {
			return true, err
		}
		policy, err := config.Drivers.physical(ctx, child.Identity)
		if err != nil {
			return true, fmt.Errorf("holder: resolve physical source DriverID: %w", err)
		}
		materializers := sourceauthority.SourceTaskMaterializers{child.Identity.Authority: policy}
		_, err = sourceauthority.RunSourceTaskChild(ctx, arguments, materializers)
		return true, err
	}
	return false, nil
}

func nilAuthorityPolicy(policy sourceauthority.AuthorityPolicy) bool {
	if policy == nil {
		return true
	}
	value := reflect.ValueOf(policy)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
