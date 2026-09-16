// Package blob defines the on-disk format of a stored blob: a fixed-size
// header followed by the payload it describes.
//
// A blob is exactly HeaderSize bytes of header followed by Length bytes of
// payload. All integers are big-endian.
//
//	offset  size  field
//	     0     4  magic, the ASCII bytes "CRNB"
//	     4     4  format version, uint32 (currently 1)
//	     8     8  payload length in bytes, uint64
//	    16    32  SHA-256 of the payload
//	    48    16  reserved: written as zero, ignored when read
//	    64     -  payload, exactly Length bytes
//
// The header carries no checksum of its own. A corrupted length or digest
// surfaces from Verify as ErrTruncated or ErrChecksum, and Verify is the only
// way a payload is ever declared good.
package blob

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	// HeaderSize is the fixed size of an encoded Header in bytes.
	HeaderSize = 64
	// Magic is the byte sequence every blob starts with.
	Magic = "CRNB"
	// Version is the only format version this package reads or writes.
	Version = 1
)

var (
	// ErrBadMagic means the bytes do not start with Magic.
	ErrBadMagic = errors.New("blob: bad magic")
	// ErrBadVersion means the header declares a version other than Version.
	ErrBadVersion = errors.New("blob: unsupported format version")
	// ErrTruncated means the input ended before the header or payload did.
	ErrTruncated = errors.New("blob: truncated")
	// ErrChecksum means the payload's SHA-256 does not match the header.
	ErrChecksum = errors.New("blob: checksum mismatch")
)

// Header describes the payload that follows it.
type Header struct {
	Length uint64
	SHA256 [sha256.Size]byte
}

// EncodeHeader serializes h in the layout documented for the package.
func EncodeHeader(h Header) [HeaderSize]byte {
	var b [HeaderSize]byte
	copy(b[0:4], Magic)
	binary.BigEndian.PutUint32(b[4:8], Version)
	binary.BigEndian.PutUint64(b[8:16], h.Length)
	copy(b[16:48], h.SHA256[:])
	return b
}

// DecodeHeader parses the header at the start of b. Bytes beyond HeaderSize
// are ignored, so b may be a whole blob. It returns ErrTruncated if b is
// shorter than HeaderSize, ErrBadMagic or ErrBadVersion if the prefix is not
// a version 1 header.
func DecodeHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, fmt.Errorf("%w: header has %d of %d bytes", ErrTruncated, len(b), HeaderSize)
	}
	if string(b[0:4]) != Magic {
		return Header{}, fmt.Errorf("%w: %q", ErrBadMagic, b[0:4])
	}
	if v := binary.BigEndian.Uint32(b[4:8]); v != Version {
		return Header{}, fmt.Errorf("%w: %d", ErrBadVersion, v)
	}
	h := Header{Length: binary.BigEndian.Uint64(b[8:16])}
	copy(h.SHA256[:], b[16:48])
	return h, nil
}

// ReadHeader reads exactly HeaderSize bytes from r and decodes them. It
// returns ErrTruncated if r ends first and any other read error unchanged.
func ReadHeader(r io.Reader) (Header, error) {
	var b [HeaderSize]byte
	n, err := io.ReadFull(r, b[:])
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Header{}, fmt.Errorf("%w: header has %d of %d bytes", ErrTruncated, n, HeaderSize)
		}
		return Header{}, fmt.Errorf("blob: read header: %w", err)
	}
	return DecodeHeader(b[:])
}

// Verify streams exactly h.Length bytes from r through SHA-256 and compares
// the digest with h.SHA256. It never reads past the payload, so r may hold
// trailing data. It returns ErrTruncated if r ends early, ErrChecksum if the
// digest differs, and any other read error unchanged.
func Verify(r io.Reader, h Header) error {
	if h.Length > math.MaxInt64 {
		return fmt.Errorf("%w: payload length %d exceeds what any reader can hold", ErrTruncated, h.Length)
	}
	digest := sha256.New()
	n, err := io.CopyN(digest, r, int64(h.Length))
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("%w: payload has %d of %d bytes", ErrTruncated, n, h.Length)
		}
		return fmt.Errorf("blob: read payload: %w", err)
	}
	var sum [sha256.Size]byte
	digest.Sum(sum[:0])
	if sum != h.SHA256 {
		return ErrChecksum
	}
	return nil
}
