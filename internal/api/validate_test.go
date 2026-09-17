package api

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateKeyTable(t *testing.T) {
	for _, tc := range []struct {
		key string
		ok  bool
	}{
		{"k", true},
		{"a/b/c", true},
		{"a//b", true},
		{"./a", true},
		{"a/./b", true},
		{"a/.../b", true},
		{"..a", true},
		{"a..", true},
		{"trailing/", true},
		{"ünïcode/🙂", true},
		{strings.Repeat("k", 1024), true},
		{"", false},
		{"/abs", false},
		{"..", false},
		{"../a", false},
		{"a/..", false},
		{"a/../b", false},
		{"\xff", false},
		{"a\xffb", false},
		{strings.Repeat("k", 1025), false},
	} {
		err := validateKey(tc.key)
		if (err == nil) != tc.ok {
			t.Errorf("validateKey(%q) = %v, want ok=%v", tc.key, err, tc.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalidKey) {
			t.Errorf("validateKey(%q) = %v, not ErrInvalidKey", tc.key, err)
		}
	}
	for _, tc := range []struct {
		name string
		ok   bool
	}{
		{"abc", true},
		{"a-1", true},
		{"123", true},
		{strings.Repeat("a", 63), true},
		{"ab", false},
		{strings.Repeat("a", 64), false},
		{"-ab", false},
		{"ab-", false},
		{"Abc", false},
		{"a_b", false},
		{"a.b", false},
		{"a/b", false},
		{"", false},
	} {
		err := validateBucket(tc.name)
		if (err == nil) != tc.ok {
			t.Errorf("validateBucket(%q) = %v, want ok=%v", tc.name, err, tc.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalidBucket) {
			t.Errorf("validateBucket(%q) = %v, not ErrInvalidBucket", tc.name, err)
		}
	}
}
