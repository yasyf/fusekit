//go:build !darwin && !linux

package sourceauthority

import "errors"

func mutationRenameNoReplace(int, string, int, string) error {
	return errors.New("sourceauthority: no-replace rename is unsupported on this platform")
}
