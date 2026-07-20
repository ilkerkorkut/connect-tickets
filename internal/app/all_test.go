package app

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
)

func TestRunAllComposesAllThreeModes(t *testing.T) {
	origAPI, origWorker, origPublisher := runAPIFn, runWorkerFn, runPublisherFn
	t.Cleanup(func() {
		runAPIFn = origAPI
		runWorkerFn = origWorker
		runPublisherFn = origPublisher
	})

	var apiCalls, workerCalls, publisherCalls atomic.Int32
	runAPIFn = func(ctx context.Context, _ *Resources, _ config.Config) error {
		apiCalls.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}
	runWorkerFn = func(ctx context.Context, _ *Resources, _ config.Config) error {
		workerCalls.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}
	runPublisherFn = func(ctx context.Context, _ *Resources, _ config.Config) error {
		publisherCalls.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	res := newNoopResources()

	err := runAllWith(ctx, res, config.Config{})
	cancel()

	if err != nil {
		t.Errorf("runAllWith with context expiry should return nil, got %v", err)
	}
	if apiCalls.Load() != 1 {
		t.Errorf("api invoked %d times, want 1", apiCalls.Load())
	}
	if workerCalls.Load() != 1 {
		t.Errorf("worker invoked %d times, want 1", workerCalls.Load())
	}
	if publisherCalls.Load() != 1 {
		t.Errorf("publisher invoked %d times, want 1", publisherCalls.Load())
	}
}

func TestRunAllReturnsFirstMaterialFailure(t *testing.T) {
	origAPI, origWorker, origPublisher := runAPIFn, runWorkerFn, runPublisherFn
	t.Cleanup(func() {
		runAPIFn = origAPI
		runWorkerFn = origWorker
		runPublisherFn = origPublisher
	})

	sentinel := errors.New("worker exploded")
	runAPIFn = func(ctx context.Context, _ *Resources, _ config.Config) error {
		<-ctx.Done()
		return ctx.Err()
	}
	runWorkerFn = func(ctx context.Context, _ *Resources, _ config.Config) error {
		return sentinel
	}
	runPublisherFn = func(ctx context.Context, _ *Resources, _ config.Config) error {
		<-ctx.Done()
		return ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res := newNoopResources()

	err := runAllWith(ctx, res, config.Config{})
	if !errors.Is(err, sentinel) {
		t.Errorf("runAllWith = %v, want sentinel %v", err, sentinel)
	}
}

func TestRunAllCancelsSiblingsOnFailure(t *testing.T) {
	origAPI, origWorker, origPublisher := runAPIFn, runWorkerFn, runPublisherFn
	t.Cleanup(func() {
		runAPIFn = origAPI
		runWorkerFn = origWorker
		runPublisherFn = origPublisher
	})

	var apiDone, publisherDone atomic.Bool
	runAPIFn = func(ctx context.Context, _ *Resources, _ config.Config) error {
		<-ctx.Done()
		apiDone.Store(true)
		return ctx.Err()
	}
	runWorkerFn = func(ctx context.Context, _ *Resources, _ config.Config) error {
		return errors.New("boom")
	}
	runPublisherFn = func(ctx context.Context, _ *Resources, _ config.Config) error {
		<-ctx.Done()
		publisherDone.Store(true)
		return ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res := newNoopResources()

	_ = runAllWith(ctx, res, config.Config{})

	done := make(chan struct{})
	go func() {
		for !apiDone.Load() || !publisherDone.Load() {
			time.Sleep(time.Millisecond)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("siblings not cancelled: apiDone=%v publisherDone=%v", apiDone.Load(), publisherDone.Load())
	}
}
