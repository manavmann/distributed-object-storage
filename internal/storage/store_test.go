package storage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/manavmann/distributed-object-storage/internal/blob"
)

func openStore(t *testing.T) (*Store, string) {
	t.Helper()
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s, root
}

func mustWrite(t *testing.T, s *Store, id string, payload []byte) blob.Header {
	t.Helper()
	h, err := s.Write(id, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Write %s: %v", id, err)
	}
	return h
}

func readAll(t *testing.T, s *Store, id string) ([]byte, blob.Header) {
	t.Helper()
	b, err := s.Read(id)
	if err != nil {
		t.Fatalf("Read %s: %v", id, err)
	}
	defer b.Close()
	got, err := io.ReadAll(b)
	if err != nil {
		t.Fatalf("ReadAll %s: %v", id, err)
	}
	return got, b.Header
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// corruptOnDisk applies mutate to the raw bytes of blobs/<id>.
func corruptOnDisk(t *testing.T, root, id string, mutate func([]byte) []byte) {
	t.Helper()
	path := filepath.Join(root, blobsDir, id)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if err := os.WriteFile(path, mutate(raw), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

// assertQuarantined checks that blobs/<id> is gone, Read reports ErrCorrupt
// wrapping want, and quarantine/ holds exactly one file.
func assertQuarantined(t *testing.T, s *Store, root, id string, want error) {
	t.Helper()
	b, err := s.Read(id)
	if err == nil {
		b.Close()
		t.Fatalf("Read %s succeeded, want ErrCorrupt", id)
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Read %s = %v, want ErrCorrupt", id, err)
	}
	if !errors.Is(err, want) {
		t.Fatalf("Read %s = %v, want it to wrap %v", id, err, want)
	}
	if got := dirNames(t, filepath.Join(root, blobsDir)); len(got) != 0 {
		t.Fatalf("blobs/ still has %v after quarantine", got)
	}
	if got := dirNames(t, filepath.Join(root, quarantineDir)); len(got) != 1 || got[0] != id {
		t.Fatalf("quarantine/ = %v, want [%s]", got, id)
	}
	if _, err := s.Read(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Read %s = %v, want ErrNotFound", id, err)
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	s, root := openStore(t)
	payload := bytes.Repeat([]byte("cairn "), 10_000)

	h := mustWrite(t, s, "obj-1", payload)
	if h.Length != uint64(len(payload)) || h.SHA256 != sha256.Sum256(payload) {
		t.Fatalf("Write header = %+v, want length %d and payload digest", h, len(payload))
	}

	got, gotH := readAll(t, s, "obj-1")
	if !bytes.Equal(got, payload) {
		t.Fatalf("Read returned %d bytes that differ from the %d written", len(got), len(payload))
	}
	if gotH != h {
		t.Fatalf("Read header = %+v, want %+v", gotH, h)
	}

	raw, err := os.ReadFile(filepath.Join(root, blobsDir, "obj-1"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(raw) != blob.HeaderSize+len(payload) {
		t.Fatalf("file is %d bytes, want %d", len(raw), blob.HeaderSize+len(payload))
	}
	if got := dirNames(t, filepath.Join(root, blobsDir)); len(got) != 1 {
		t.Fatalf("blobs/ = %v, want only obj-1", got)
	}
}

func TestEmptyPayloadRoundTrip(t *testing.T) {
	s, _ := openStore(t)
	mustWrite(t, s, "empty", nil)
	got, h := readAll(t, s, "empty")
	if len(got) != 0 || h.Length != 0 {
		t.Fatalf("Read empty = %d bytes, header %+v", len(got), h)
	}
}

func TestReadNonexistent(t *testing.T) {
	s, _ := openStore(t)
	if _, err := s.Read("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Read = %v, want ErrNotFound", err)
	}
	if err := s.Delete("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete = %v, want ErrNotFound", err)
	}
}

func TestInvalidID(t *testing.T) {
	s, _ := openStore(t)
	for _, id := range []string{"", ".", "..", ".hidden", tmpPrefix + "x", "a/b", `a\b`, "a b", "é"} {
		if _, err := s.Write(id, strings.NewReader("x")); !errors.Is(err, ErrInvalidID) {
			t.Errorf("Write(%q) = %v, want ErrInvalidID", id, err)
		}
		if _, err := s.Read(id); !errors.Is(err, ErrInvalidID) {
			t.Errorf("Read(%q) = %v, want ErrInvalidID", id, err)
		}
		if err := s.Delete(id); !errors.Is(err, ErrInvalidID) {
			t.Errorf("Delete(%q) = %v, want ErrInvalidID", id, err)
		}
	}
}

func TestCorruptHeaderQuarantined(t *testing.T) {
	s, root := openStore(t)
	mustWrite(t, s, "obj", []byte("payload"))
	corruptOnDisk(t, root, "obj", func(raw []byte) []byte {
		copy(raw[:4], "XXXX")
		return raw
	})
	assertQuarantined(t, s, root, "obj", blob.ErrBadMagic)
}

func TestShortHeaderQuarantined(t *testing.T) {
	s, root := openStore(t)
	mustWrite(t, s, "obj", []byte("payload"))
	corruptOnDisk(t, root, "obj", func(raw []byte) []byte {
		return raw[:blob.HeaderSize/2]
	})
	assertQuarantined(t, s, root, "obj", blob.ErrTruncated)
}

func TestTruncatedPayloadQuarantined(t *testing.T) {
	s, root := openStore(t)
	mustWrite(t, s, "obj", bytes.Repeat([]byte("x"), 4096))
	corruptOnDisk(t, root, "obj", func(raw []byte) []byte {
		return raw[:len(raw)-1]
	})
	assertQuarantined(t, s, root, "obj", blob.ErrTruncated)
}

func TestFlippedPayloadByteQuarantined(t *testing.T) {
	s, root := openStore(t)
	mustWrite(t, s, "obj", bytes.Repeat([]byte("x"), 4096))
	corruptOnDisk(t, root, "obj", func(raw []byte) []byte {
		raw[len(raw)-1] ^= 0x01
		return raw
	})
	assertQuarantined(t, s, root, "obj", blob.ErrChecksum)
}

func TestDeleteMovesToQuarantine(t *testing.T) {
	s, root := openStore(t)
	mustWrite(t, s, "obj", []byte("payload"))
	if err := s.Delete("obj"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Read("obj"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Read after Delete = %v, want ErrNotFound", err)
	}
	if got := dirNames(t, filepath.Join(root, quarantineDir)); len(got) != 1 || got[0] != "obj" {
		t.Fatalf("quarantine/ = %v, want [obj]", got)
	}

	// A second blob under the same id must not overwrite the quarantined one.
	mustWrite(t, s, "obj", []byte("payload 2"))
	if err := s.Delete("obj"); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if got := dirNames(t, filepath.Join(root, quarantineDir)); len(got) != 2 {
		t.Fatalf("quarantine/ = %v, want two files", got)
	}
}

func TestConcurrentWritesDifferentKeys(t *testing.T) {
	s, root := openStore(t)
	const n = 32
	payloads := make([][]byte, n)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte{byte(i)}, 1024*(i+1))
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.Write(fmt.Sprintf("obj-%d", i), bytes.NewReader(payloads[i]))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Write obj-%d: %v", i, err)
		}
	}

	for i := range payloads {
		got, _ := readAll(t, s, fmt.Sprintf("obj-%d", i))
		if !bytes.Equal(got, payloads[i]) {
			t.Fatalf("obj-%d: read %d bytes that differ from the %d written", i, len(got), len(payloads[i]))
		}
	}
	if got := dirNames(t, filepath.Join(root, blobsDir)); len(got) != n {
		t.Fatalf("blobs/ has %d entries, want %d: %v", len(got), n, got)
	}
}

func TestWriteFailureLeavesNoPartialFile(t *testing.T) {
	s, root := openStore(t)
	mustWrite(t, s, "good", []byte("keep me"))

	boom := errors.New("boom")
	r := io.MultiReader(strings.NewReader("partial"), failingReader{boom})
	if _, err := s.Write("bad", r); !errors.Is(err, boom) {
		t.Fatalf("Write = %v, want it to wrap the reader error", err)
	}
	if got := dirNames(t, filepath.Join(root, blobsDir)); len(got) != 1 || got[0] != "good" {
		t.Fatalf("blobs/ = %v, want [good]", got)
	}
	if _, err := s.Read("bad"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Read bad = %v, want ErrNotFound", err)
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestReopenDiscardsPartialWrite(t *testing.T) {
	s, root := openStore(t)
	payload := []byte("committed before the crash")
	mustWrite(t, s, "good", payload)

	// Simulate a crash mid-write: a spooled temp file exists in blobs/ but
	// was never renamed into place.
	partial := filepath.Join(root, blobsDir, tmpPrefix+"crashed")
	if err := os.WriteFile(partial, []byte("half a blob"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	s2, err := Open(root)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	if got := dirNames(t, filepath.Join(root, blobsDir)); len(got) != 1 || got[0] != "good" {
		t.Fatalf("blobs/ after re-Open = %v, want [good]", got)
	}
	got, _ := readAll(t, s2, "good")
	if !bytes.Equal(got, payload) {
		t.Fatalf("good after re-Open = %q, want %q", got, payload)
	}
}
