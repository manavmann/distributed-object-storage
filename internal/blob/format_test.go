package blob

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"
	"testing/iotest"
)

func headerFor(payload []byte) Header {
	return Header{Length: uint64(len(payload)), SHA256: sha256.Sum256(payload)}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	want := headerFor([]byte("hello, cairn"))
	b := EncodeHeader(want)

	got, err := DecodeHeader(b[:])
	if err != nil {
		t.Fatalf("DecodeHeader: %v", err)
	}
	if got != want {
		t.Fatalf("DecodeHeader = %+v, want %+v", got, want)
	}

	got, err = ReadHeader(bytes.NewReader(b[:]))
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if got != want {
		t.Fatalf("ReadHeader = %+v, want %+v", got, want)
	}
}

func TestHeaderIsExactly64Bytes(t *testing.T) {
	if HeaderSize != 64 {
		t.Fatalf("HeaderSize = %d, want 64", HeaderSize)
	}
	h := Header{Length: 0x0102030405060708}
	for i := range h.SHA256 {
		h.SHA256[i] = byte(i)
	}
	b := EncodeHeader(h)

	want := []byte("CRNB\x00\x00\x00\x01\x01\x02\x03\x04\x05\x06\x07\x08")
	want = append(want, h.SHA256[:]...)
	want = append(want, make([]byte, 16)...)
	if len(want) != 64 {
		t.Fatalf("golden header is %d bytes, want 64", len(want))
	}
	if !bytes.Equal(b[:], want) {
		t.Fatalf("EncodeHeader =\n% x\nwant\n% x", b[:], want)
	}
}

func TestDecodeHeaderBadMagic(t *testing.T) {
	b := EncodeHeader(headerFor(nil))
	copy(b[:4], "XXXX")
	if _, err := DecodeHeader(b[:]); !errors.Is(err, ErrBadMagic) {
		t.Fatalf("DecodeHeader = %v, want ErrBadMagic", err)
	}
}

func TestDecodeHeaderUnknownVersion(t *testing.T) {
	for _, v := range []uint32{0, 2, math.MaxUint32} {
		b := EncodeHeader(headerFor(nil))
		binary.BigEndian.PutUint32(b[4:8], v)
		if _, err := DecodeHeader(b[:]); !errors.Is(err, ErrBadVersion) {
			t.Fatalf("version %d: DecodeHeader = %v, want ErrBadVersion", v, err)
		}
	}
}

func TestShortHeader(t *testing.T) {
	b := EncodeHeader(headerFor(nil))
	for _, n := range []int{0, 1, 4, HeaderSize - 1} {
		if _, err := DecodeHeader(b[:n]); !errors.Is(err, ErrTruncated) {
			t.Fatalf("DecodeHeader(%d bytes) = %v, want ErrTruncated", n, err)
		}
		if _, err := ReadHeader(bytes.NewReader(b[:n])); !errors.Is(err, ErrTruncated) {
			t.Fatalf("ReadHeader(%d bytes) = %v, want ErrTruncated", n, err)
		}
	}
}

func TestVerifyGoodPayload(t *testing.T) {
	payload := bytes.Repeat([]byte("cairn "), 20000)
	h := headerFor(payload)

	blob := append(append([]byte(nil), payload...), "trailer"...)
	r := bytes.NewReader(blob)
	if err := Verify(r, h); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(rest) != "trailer" {
		t.Fatalf("Verify left %q unread, want %q: it must read exactly Length bytes", rest, "trailer")
	}
}

func TestVerifyTruncatedPayload(t *testing.T) {
	payload := []byte("the payload is longer than what the reader holds")
	h := headerFor(payload)
	for _, n := range []int{0, 1, len(payload) - 1} {
		err := Verify(bytes.NewReader(payload[:n]), h)
		if !errors.Is(err, ErrTruncated) {
			t.Fatalf("Verify(%d of %d bytes) = %v, want ErrTruncated", n, len(payload), err)
		}
	}

	huge := Header{Length: math.MaxUint64, SHA256: sha256.Sum256(nil)}
	if err := Verify(bytes.NewReader(nil), huge); !errors.Is(err, ErrTruncated) {
		t.Fatalf("Verify(Length = MaxUint64) = %v, want ErrTruncated", err)
	}
}

func TestVerifyFlippedByte(t *testing.T) {
	payload := bytes.Repeat([]byte{0xAB}, 4096)
	h := headerFor(payload)
	for _, i := range []int{0, len(payload) / 2, len(payload) - 1} {
		bad := append([]byte(nil), payload...)
		bad[i] ^= 0x01
		if err := Verify(bytes.NewReader(bad), h); !errors.Is(err, ErrChecksum) {
			t.Fatalf("Verify(byte %d flipped) = %v, want ErrChecksum", i, err)
		}
	}

	h.SHA256[0] ^= 0x80
	if err := Verify(bytes.NewReader(payload), h); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Verify(digest bit flipped) = %v, want ErrChecksum", err)
	}
}

func TestVerifyZeroLengthPayload(t *testing.T) {
	r := bytes.NewReader([]byte("untouched"))
	if err := Verify(r, headerFor(nil)); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if r.Len() != len("untouched") {
		t.Fatalf("Verify read %d bytes of a zero-length payload", len("untouched")-r.Len())
	}

	wrong := Header{Length: 0, SHA256: [sha256.Size]byte{1}}
	if err := Verify(bytes.NewReader(nil), wrong); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Verify(zero length, wrong digest) = %v, want ErrChecksum", err)
	}
}

func TestReadErrorsPassThrough(t *testing.T) {
	errBoom := errors.New("boom")
	sentinels := []error{ErrBadMagic, ErrBadVersion, ErrTruncated, ErrChecksum}

	_, err := ReadHeader(iotest.ErrReader(errBoom))
	if !errors.Is(err, errBoom) {
		t.Fatalf("ReadHeader = %v, want errBoom", err)
	}
	for _, s := range sentinels {
		if errors.Is(err, s) {
			t.Fatalf("ReadHeader = %v, must not match %v", err, s)
		}
	}

	err = Verify(iotest.ErrReader(errBoom), Header{Length: 1})
	if !errors.Is(err, errBoom) {
		t.Fatalf("Verify = %v, want errBoom", err)
	}
	for _, s := range sentinels {
		if errors.Is(err, s) {
			t.Fatalf("Verify = %v, must not match %v", err, s)
		}
	}
}

func FuzzDecodeHeader(f *testing.F) {
	good := EncodeHeader(headerFor([]byte("seed")))
	f.Add(good[:])

	badMagic := good
	copy(badMagic[:4], "NOPE")
	f.Add(badMagic[:])

	badVersion := good
	binary.BigEndian.PutUint32(badVersion[4:8], 2)
	f.Add(badVersion[:])

	f.Add([]byte{})
	f.Add(good[:HeaderSize-1])

	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := DecodeHeader(b)
		if err != nil {
			if !errors.Is(err, ErrTruncated) && !errors.Is(err, ErrBadMagic) && !errors.Is(err, ErrBadVersion) {
				t.Fatalf("DecodeHeader = %v, want a sentinel error", err)
			}
			return
		}
		enc := EncodeHeader(h)
		again, err := DecodeHeader(enc[:])
		if err != nil {
			t.Fatalf("DecodeHeader(EncodeHeader(h)) = %v", err)
		}
		if again != h {
			t.Fatalf("round trip = %+v, want %+v", again, h)
		}
		if !bytes.Equal(enc[:48], b[:48]) {
			t.Fatalf("re-encoded header differs from input in the first 48 bytes:\n% x\n% x", enc[:48], b[:48])
		}
	})
}
