package api

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// bucketRE is the accepted bucket name shape: 3–63 lowercase letters,
// digits and hyphens, starting and ending with a letter or digit.
var bucketRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

// maxKeyBytes is the longest key accepted, in bytes.
const maxKeyBytes = 1024

func validateBucket(name string) error {
	if !bucketRE.MatchString(name) {
		return fmt.Errorf("%w: %q must match %s", ErrInvalidBucket, name, bucketRE)
	}
	return nil
}

// validateKey accepts 1–1024 bytes of valid UTF-8 with no leading '/' and
// no ".." path segment, so a key can never escape its bucket when
// mapped onto a path.
func validateKey(key string) error {
	switch {
	case key == "":
		return fmt.Errorf("%w: empty", ErrInvalidKey)
	case len(key) > maxKeyBytes:
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrInvalidKey, len(key), maxKeyBytes)
	case !utf8.ValidString(key):
		return fmt.Errorf("%w: not valid UTF-8", ErrInvalidKey)
	case key[0] == '/':
		return fmt.Errorf("%w: leading '/'", ErrInvalidKey)
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == ".." {
			return fmt.Errorf("%w: %q segment", ErrInvalidKey, seg)
		}
	}
	return nil
}
