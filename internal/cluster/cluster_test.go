package cluster

import (
	"errors"
	"testing"
)

func TestParseStatic(t *testing.T) {
	r, err := ParseStatic("n1=10.0.0.1:9100, n2=localhost:9101")
	if err != nil {
		t.Fatalf("ParseStatic: %v", err)
	}
	want := []Node{{ID: "n1", Addr: "http://10.0.0.1:9100"}, {ID: "n2", Addr: "http://localhost:9101"}}
	got := r.Healthy()
	if len(got) != len(want) {
		t.Fatalf("Healthy = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Healthy[%d] = %v, want %v", i, got[i], want[i])
		}
	}
	if n, ok := r.Get("n2"); !ok || n != want[1] {
		t.Fatalf("Get(n2) = %v, %v", n, ok)
	}
	if _, ok := r.Get("n3"); ok {
		t.Fatal("Get(n3) reported an unknown node")
	}
}

func TestHealthyIsACopy(t *testing.T) {
	r := NewStatic([]Node{{ID: "n1", Addr: "http://a:1"}})
	r.Healthy()[0].ID = "changed"
	if got := r.Healthy()[0].ID; got != "n1" {
		t.Fatalf("caller mutated registry: %q", got)
	}
}

func TestParseStaticErrors(t *testing.T) {
	for _, in := range []string{"", "  ", "n1", "=host:1", "n1=", "n1=http://host:1", "n1=a:1,n1=b:2"} {
		if _, err := ParseStatic(in); !errors.Is(err, ErrInvalidNodes) {
			t.Errorf("ParseStatic(%q) = %v, want ErrInvalidNodes", in, err)
		}
	}
}
