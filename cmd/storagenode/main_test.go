package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	if version == "" {
		t.Fatal("version is empty")
	}

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	stdout := os.Stdout
	os.Stdout = w
	main()
	os.Stdout = stdout
	if err := w.Close(); err != nil {
		t.Fatalf("close pipe: %v", err)
	}

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}
	got := strings.TrimSpace(string(out))
	if want := "storagenode " + version; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
}
