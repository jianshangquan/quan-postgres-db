package store

import (
	"fmt"
	"regexp"
)

var idRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

// ValidateID rejects unsafe tenant/timeline path segments.
func ValidateID(id string) error {
	if !idRe.MatchString(id) {
		return fmt.Errorf("invalid id %q", id)
	}
	return nil
}
