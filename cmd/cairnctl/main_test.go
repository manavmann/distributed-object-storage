package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/manavmann/distributed-object-storage/internal/testcluster"
)

func TestParse(t *testing.T) {
	cases := []struct {
		name string
		args []string
		env  string
		want command
		err  error
	}{
		{"default url", []string{"status"}, "", command{URL: "http://localhost:8080", Name: "status", Args: []string{}}, nil},
		{"env url", []string{"status"}, "http://env:1/", command{URL: "http://env:1", Name: "status", Args: []string{}}, nil},
		{"flag beats env", []string{"-url", "http://flag:2", "mb", "b"}, "http://env:1", command{URL: "http://flag:2", Name: "mb", Args: []string{"b"}}, nil},
		{"ls prefix", []string{"ls", "b", "p/"}, "", command{URL: "http://localhost:8080", Name: "ls", Args: []string{"b", "p/"}}, nil},
		{"put", []string{"put", "b", "k", "f"}, "", command{URL: "http://localhost:8080", Name: "put", Args: []string{"b", "k", "f"}}, nil},
		{"get to stdout", []string{"get", "b", "k"}, "", command{URL: "http://localhost:8080", Name: "get", Args: []string{"b", "k"}}, nil},
		{"locate", []string{"locate", "b", "k"}, "", command{URL: "http://localhost:8080", Name: "locate", Args: []string{"b", "k"}}, nil},
		{"no subcommand", nil, "", command{}, ErrUsage},
		{"unknown", []string{"cp", "a", "b"}, "", command{}, ErrUsage},
		{"too few", []string{"put", "b", "k"}, "", command{}, ErrUsage},
		{"too many", []string{"rm", "b", "k", "x"}, "", command{}, ErrUsage},
		{"bad flag", []string{"-nope", "status"}, "", command{}, ErrUsage},
		{"flag after subcommand", []string{"status", "-url", "x"}, "", command{}, ErrUsage},
	}
	for _, c := range cases {
		got, err := parse(c.args, c.env)
		if !errors.Is(err, c.err) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.err)
			continue
		}
		if err != nil {
			continue
		}
		if got.URL != c.want.URL || got.Name != c.want.Name || strings.Join(got.Args, " ") != strings.Join(c.want.Args, " ") {
			t.Errorf("%s: parse = %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	cl := testcluster.New(t, testcluster.Opts{Nodes: 1, RF: 1, W: 1})
	base := cl.Client().URL()
	ctx := context.Background()
	do := func(out *bytes.Buffer, args ...string) error {
		cmd, err := parse(append([]string{"-url", base}, args...), "")
		if err != nil {
			t.Fatalf("parse %v: %v", args, err)
		}
		return run(ctx, cmd, out)
	}
	payload := make([]byte, 3<<20+17)
	rand.Read(payload)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	if err := os.WriteFile(src, payload, 0o644); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := do(&out, "mb", "ctl"); err != nil {
		t.Fatalf("mb: %v", err)
	}
	if err := do(&out, "mb", "ctl"); !errors.Is(err, ErrAPI) || !strings.Contains(err.Error(), "BucketAlreadyExists") {
		t.Fatalf("second mb err = %v, want ErrAPI BucketAlreadyExists", err)
	}
	out.Reset()
	if err := do(&out, "put", "ctl", "dir/k 1", src); err != nil {
		t.Fatalf("put: %v", err)
	}
	if !strings.Contains(out.String(), `"key":"dir/k 1"`) {
		t.Fatalf("put output %q lacks the key", out.String())
	}
	out.Reset()
	if err := do(&out, "get", "ctl", "dir/k 1"); err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(out.Bytes(), payload) {
		t.Fatalf("get to stdout: %d bytes, want %d", out.Len(), len(payload))
	}
	dst := filepath.Join(dir, "dst.bin")
	if err := do(&out, "get", "ctl", "dir/k 1", dst); err != nil {
		t.Fatalf("get to file: %v", err)
	}
	if got, err := os.ReadFile(dst); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("get to file: %d bytes, err %v; want %d", len(got), err, len(payload))
	}
	out.Reset()
	if err := do(&out, "ls", "ctl", "dir/"); err != nil {
		t.Fatalf("ls: %v", err)
	}
	if want := "3145745\tdir/k 1\n"; out.String() != want {
		t.Fatalf("ls = %q, want %q", out.String(), want)
	}
	out.Reset()
	if err := do(&out, "locate", "ctl", "dir/k 1"); err != nil {
		t.Fatalf("locate: %v", err)
	}
	if !strings.Contains(out.String(), cl.NodeID(0)) {
		t.Fatalf("locate %q lacks node %s", out.String(), cl.NodeID(0))
	}
	out.Reset()
	if err := do(&out, "status"); err != nil || !strings.Contains(out.String(), `"nodes"`) {
		t.Fatalf("status = %q, %v", out.String(), err)
	}
	if err := do(&out, "rm", "ctl", "dir/k 1"); err != nil {
		t.Fatalf("rm: %v", err)
	}
	if err := do(&out, "get", "ctl", "dir/k 1"); !errors.Is(err, ErrAPI) || !strings.Contains(err.Error(), "NoSuchKey") {
		t.Fatalf("get after rm err = %v, want ErrAPI NoSuchKey", err)
	}
}
