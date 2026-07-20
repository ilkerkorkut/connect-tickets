package signal_test

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/ilkerkorkut/connect-tickets/pkg/signal"
)

func TestRootContextCancelsOnSIGTERM(t *testing.T) {
	t.Parallel()

	ctx, cancel := signal.RootContext(context.Background())
	t.Cleanup(cancel)

	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(done)
	}()

	if err := sendSelf(syscall.SIGTERM); err != nil {
		t.Fatalf("send signal: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("context was not cancelled within 2s of SIGTERM")
	}
}

func TestRootContextCancelsOnSIGINT(t *testing.T) {
	t.Parallel()

	ctx, cancel := signal.RootContext(context.Background())
	t.Cleanup(cancel)

	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(done)
	}()

	if err := sendSelf(syscall.SIGINT); err != nil {
		t.Fatalf("send signal: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("context was not cancelled within 2s of SIGINT")
	}
}

func TestRootContextParentCancelPropagates(t *testing.T) {
	t.Parallel()

	parent, parentCancel := context.WithCancel(context.Background())
	ctx, cancel := signal.RootContext(parent)
	defer cancel()

	parentCancel()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("child context did not observe parent cancellation")
	}
}

func sendSelf(sig os.Signal) error {
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}
	return p.Signal(sig)
}
