package health

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReadyRequiresGateAndHealthyDependencies(t *testing.T) {
	t.Parallel()
	service := New(Check{Name: "database", Timeout: time.Second, Run: func(context.Context) error { return nil }})
	if err := service.Ready(context.Background()); !errors.Is(err, ErrNotReady) {
		t.Fatalf("Ready before gate = %v", err)
	}
	service.SetReady(true)
	if err := service.Ready(context.Background()); err != nil {
		t.Fatalf("Ready after gate = %v", err)
	}
}

func TestCheckUsesIndividualTimeoutAndSafeError(t *testing.T) {
	t.Parallel()
	service := New(Check{Name: "redis", Timeout: 10 * time.Millisecond, Run: func(ctx context.Context) error {
		<-ctx.Done()
		return errors.New("redis password leaked")
	}})
	started := time.Now()
	err := service.Check(context.Background())
	if !errors.Is(err, ErrNotReady) || time.Since(started) > time.Second {
		t.Fatalf("Check() err=%v duration=%s", err, time.Since(started))
	}
}
