package bootstrap

import (
	"errors"
	"sync"
	"testing"
)

type recorder struct {
	mu       sync.Mutex
	order    []string
	errByCmp map[string]error
}

func newRecorder() *recorder {
	return &recorder{errByCmp: map[string]error{}}
}

func (r *recorder) closer(name string) Closer {
	return closerFunc(func() error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.order = append(r.order, name)
		return r.errByCmp[name]
	})
}

func (r *recorder) withErr(name string, err error) *recorder {
	r.errByCmp[name] = err
	return r
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]string, len(r.order))
	copy(cp, r.order)
	return cp
}

func TestResourcesCloseIsLIFO(t *testing.T) {
	rec := newRecorder()
	res := &Resources{}

	res.add("first", rec.closer("first"))
	res.add("second", rec.closer("second"))
	res.add("third", rec.closer("third"))

	if err := res.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	got := rec.snapshot()
	want := []string{"third", "second", "first"}
	if len(got) != len(want) {
		t.Fatalf("close order %v, want %v", got, want)
	}
	for i, name := range want {
		if got[i] != name {
			t.Errorf("close[%d] = %q, want %q", i, got[i], name)
		}
	}
}

func TestResourcesCloseCollectsAllErrors(t *testing.T) {
	rec := newRecorder().
		withErr("first", errors.New("first err")).
		withErr("third", errors.New("third err"))

	res := &Resources{}
	res.add("first", rec.closer("first"))
	res.add("second", rec.closer("second"))
	res.add("third", rec.closer("third"))

	err := res.Close()
	if err == nil {
		t.Fatal("expected joined error from multiple failed closes")
	}
	msg := err.Error()
	if !contains(msg, "first err") {
		t.Errorf("error %q missing first err", msg)
	}
	if !contains(msg, "third err") {
		t.Errorf("error %q missing third err", msg)
	}
	got := rec.snapshot()
	if len(got) != 3 {
		t.Fatalf("expected all 3 closed even with errors, got %d", len(got))
	}
}

func TestResourcesCloseNilSafeAndIdempotent(t *testing.T) {
	res := &Resources{}
	if err := res.Close(); err != nil {
		t.Errorf("Close on empty resources returned %v", err)
	}
	if err := res.Close(); err != nil {
		t.Errorf("second Close returned %v", err)
	}
}

func TestFinalizeBootstrapClosesAndJoins(t *testing.T) {
	rec := newRecorder()
	res := &Resources{}
	res.add("opened", rec.closer("opened"))

	cause := errors.New("bootstrap failed at next step")
	err := finalizeBootstrap(res, cause)

	if !errors.Is(err, cause) {
		t.Errorf("finalizeBootstrap did not preserve cause: %v", err)
	}
	got := rec.snapshot()
	if len(got) != 1 || got[0] != "opened" {
		t.Errorf("finalizeBootstrap did not close opened resource in reverse order: %v", got)
	}
}

func TestFinalizeBootstrapNilResources(t *testing.T) {
	cause := errors.New("boom")
	err := finalizeBootstrap(nil, cause)
	if !errors.Is(err, cause) {
		t.Errorf("nil resources should return cause unchanged, got %v", err)
	}
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
