// Package storage is the on-disk store of a storage node.
//
// A Store owns a root directory with two subdirectories:
//
//	blobs/       committed blobs, one file per blob_id, in the blob package format
//	quarantine/  files that failed verification or were deleted; never served,
//	             never removed by this package
//
// Nothing under blobs/ is ever partial. A write spools to a temp file in
// blobs/, fsyncs it, renames it into place and fsyncs the directory, so a
// crash leaves either the complete blob or a temp file that the next Open
// discards. A read verifies the whole payload before serving a byte.
package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/manavmann/distributed-object-storage/internal/blob"
)

const (
	blobsDir      = "blobs"
	quarantineDir = "quarantine"
	// tmpPrefix marks an in-progress write. Blob ids may not start with '.',
	// so a temp file can never be mistaken for a committed blob.
	tmpPrefix = ".tmp-"
)

var (
	// ErrNotFound means no committed blob has that id.
	ErrNotFound = errors.New("storage: blob not found")
	// ErrCorrupt means the blob failed header or checksum verification and
	// has been moved to quarantine/. It wraps the blob package error.
	ErrCorrupt = errors.New("storage: blob corrupt")
	// ErrInvalidID means the id cannot name a file under blobs/.
	ErrInvalidID = errors.New("storage: invalid blob id")
)

// Store is a node's blob directory. It is safe for concurrent use.
type Store struct {
	blobs      string
	quarantine string
	// mu serializes moves into quarantine/ so two moves of the same id pick
	// distinct destination names.
	mu sync.Mutex
}

// Open prepares root for use, creating blobs/ and quarantine/ if missing,
// and discards temp files left in blobs/ by writes that never committed.
// Committed blobs need no recovery: every file in blobs/ that is not a temp
// file was fully written and fsynced before it got its name.
func Open(root string) (*Store, error) {
	s := &Store{
		blobs:      filepath.Join(root, blobsDir),
		quarantine: filepath.Join(root, quarantineDir),
	}
	for _, dir := range []string{s.blobs, s.quarantine} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("storage: open: %w", err)
		}
	}
	entries, err := os.ReadDir(s.blobs)
	if err != nil {
		return nil, fmt.Errorf("storage: open: %w", err)
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), tmpPrefix) {
			continue
		}
		if err := os.Remove(filepath.Join(s.blobs, e.Name())); err != nil {
			return nil, fmt.Errorf("storage: open: discard partial write: %w", err)
		}
	}
	if err := syncDir(s.blobs); err != nil {
		return nil, fmt.Errorf("storage: open: %w", err)
	}
	return s, nil
}

// Write stores the whole of r as blob id and returns the header it was
// committed under. The payload is hashed while it is spooled, so the header
// is written once the length and digest are known. The blob is visible under
// blobs/<id> only after it has been fsynced. An existing blob with the same
// id is replaced.
func (s *Store) Write(id string, r io.Reader) (blob.Header, error) {
	if err := checkID(id); err != nil {
		return blob.Header{}, err
	}
	f, err := os.CreateTemp(s.blobs, tmpPrefix+"*")
	if err != nil {
		return blob.Header{}, fmt.Errorf("storage: write %s: %w", id, err)
	}
	h, err := spool(f, r)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), filepath.Join(s.blobs, id))
	}
	if err == nil {
		err = syncDir(s.blobs)
	}
	if err != nil {
		os.Remove(f.Name())
		return blob.Header{}, fmt.Errorf("storage: write %s: %w", id, err)
	}
	return h, nil
}

// spool writes a zero header, streams r through SHA-256 into f, then seeks
// back and writes the real header.
func spool(f *os.File, r io.Reader) (blob.Header, error) {
	var placeholder [blob.HeaderSize]byte
	if _, err := f.Write(placeholder[:]); err != nil {
		return blob.Header{}, err
	}
	digest := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, digest), r)
	if err != nil {
		return blob.Header{}, err
	}
	h := blob.Header{Length: uint64(n)}
	digest.Sum(h.SHA256[:0])
	enc := blob.EncodeHeader(h)
	if _, err := f.WriteAt(enc[:], 0); err != nil {
		return blob.Header{}, err
	}
	return h, nil
}

// Blob is an open, fully verified blob. Reads, seeks and ReadAt address the
// payload only; the header is exposed as Header.
type Blob struct {
	Header blob.Header
	*io.SectionReader
	f *os.File
}

// Close releases the underlying file.
func (b *Blob) Close() error {
	return b.f.Close()
}

// Read opens blob id and verifies its header and full payload checksum
// before returning. It returns ErrNotFound if there is no such blob and
// ErrCorrupt if verification fails, in which case the file has been moved
// to quarantine/ and nothing of it is served.
func (s *Store) Read(id string) (*Blob, error) {
	if err := checkID(id); err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(s.blobs, id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		return nil, fmt.Errorf("storage: read %s: %w", id, err)
	}
	h, err := blob.ReadHeader(f)
	if err == nil {
		err = blob.Verify(io.NewSectionReader(f, blob.HeaderSize, int64(h.Length)), h)
	}
	if err != nil {
		if isFormatError(err) {
			f.Close()
			if qerr := s.quarantineBlob(id); qerr != nil {
				return nil, fmt.Errorf("storage: read %s: %w", id, qerr)
			}
			return nil, fmt.Errorf("%w: %s: %w", ErrCorrupt, id, err)
		}
		f.Close()
		return nil, fmt.Errorf("storage: read %s: %w", id, err)
	}
	return &Blob{
		Header:        h,
		SectionReader: io.NewSectionReader(f, blob.HeaderSize, int64(h.Length)),
		f:             f,
	}, nil
}

// Delete moves blob id to quarantine/ so it stops being served. The bytes
// stay on disk for GC to reclaim. It returns ErrNotFound if there is no
// such blob.
func (s *Store) Delete(id string) error {
	if err := checkID(id); err != nil {
		return err
	}
	if err := s.quarantineBlob(id); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		return fmt.Errorf("storage: delete %s: %w", id, err)
	}
	return nil
}

// BlobCount returns the number of committed blobs in blobs/.
func (s *Store) BlobCount() (int, error) {
	entries, err := os.ReadDir(s.blobs)
	if err != nil {
		return 0, fmt.Errorf("storage: count: %w", err)
	}
	n := 0
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), tmpPrefix) {
			n++
		}
	}
	return n, nil
}

// FreeBytes returns the free space on the filesystem holding blobs/.
func (s *Store) FreeBytes() (uint64, error) {
	n, err := freeBytes(s.blobs)
	if err != nil {
		return 0, fmt.Errorf("storage: free bytes: %w", err)
	}
	return n, nil
}

// quarantineBlob moves blobs/<id> to quarantine/. If quarantine/<id> is
// already taken the file gets a numeric suffix, so nothing in quarantine/
// is ever overwritten.
func (s *Store) quarantineBlob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dst := filepath.Join(s.quarantine, id)
	for n := 1; ; n++ {
		_, err := os.Lstat(dst)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return err
		}
		dst = filepath.Join(s.quarantine, id+"."+strconv.Itoa(n))
	}
	if err := os.Rename(filepath.Join(s.blobs, id), dst); err != nil {
		return err
	}
	if err := syncDir(s.blobs); err != nil {
		return err
	}
	return syncDir(s.quarantine)
}

// isFormatError reports whether err is a verdict about the bytes on disk
// rather than an I/O failure.
func isFormatError(err error) bool {
	return errors.Is(err, blob.ErrBadMagic) ||
		errors.Is(err, blob.ErrBadVersion) ||
		errors.Is(err, blob.ErrTruncated) ||
		errors.Is(err, blob.ErrChecksum)
}

// checkID accepts ids made of ASCII letters, digits, '-', '_' and '.', not
// starting with '.', so an id is always a single, non-hidden path element
// and can never collide with a temp file.
func checkID(id string) error {
	if id == "" || id[0] == '.' {
		return fmt.Errorf("%w: %q", ErrInvalidID, id)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9', c == '-', c == '_', c == '.':
		default:
			return fmt.Errorf("%w: %q", ErrInvalidID, id)
		}
	}
	return nil
}

// syncDir fsyncs a directory so a rename or unlink in it is durable. Windows
// has no directory fsync (FlushFileBuffers on a directory handle is denied);
// NTFS journals directory metadata itself, so it is a no-op there.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
