package api

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// bucketRE is the accepted bucket name shape: 3–63 lowercase letters,
// digits and hyphens, starting and ending with a letter or digit.
var bucketRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

// maxKeyBytes is the longest key accepted, in bytes.
const maxKeyBytes = 1024

// maxListLimit is both the default and the ceiling for a listing's limit.
const maxListLimit = 1000

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

// listQuery is the parsed query string of an object listing.
type listQuery struct {
	Prefix     string
	StartAfter string
	Limit      int
}

// parseListQuery reads prefix, start_after and limit. Prefix and
// start_after may be empty; when set they obey the key rules. Limit
// defaults to maxListLimit and must be an integer in [1, maxListLimit].
func parseListQuery(q url.Values) (listQuery, error) {
	lq := listQuery{Prefix: q.Get("prefix"), StartAfter: q.Get("start_after"), Limit: maxListLimit}
	if lq.Prefix != "" {
		if err := validateKey(lq.Prefix); err != nil {
			return listQuery{}, fmt.Errorf("%w: prefix: %v", ErrInvalidArgument, err)
		}
	}
	if lq.StartAfter != "" {
		if err := validateKey(lq.StartAfter); err != nil {
			return listQuery{}, fmt.Errorf("%w: start_after: %v", ErrInvalidArgument, err)
		}
	}
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return listQuery{}, fmt.Errorf("%w: limit %q is not an integer", ErrInvalidArgument, raw)
		}
		if n < 1 || n > maxListLimit {
			return listQuery{}, fmt.Errorf("%w: limit %d outside [1, %d]", ErrInvalidArgument, n, maxListLimit)
		}
		lq.Limit = n
	}
	return lq, nil
}
