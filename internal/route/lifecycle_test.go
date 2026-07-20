package route

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

type readinessFake struct {
	mu     sync.Mutex
	values []bool
	falseC chan struct{}
}

func (r *readinessFake) SetReady(value bool) {
	r.mu.Lock()
	r.values = append(r.values, value)
	r.mu.Unlock()
	if !value {
		select {
		case <-r.falseC:
		default:
			close(r.falseC)
		}
	}
}

func TestServeListenerMarksUnreadyAndDrainsRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		close(started)
		<-release
		writer.WriteHeader(http.StatusOK)
	})}
	readiness := &readinessFake{falseC: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ServeListener(ctx, server, listener, readiness, time.Second) }()

	responseDone := make(chan error, 1)
	go func() {
		response, err := http.Get("http://" + listener.Addr().String())
		if err == nil {
			err = response.Body.Close()
		}
		responseDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case <-readiness.falseC:
	case <-time.After(time.Second):
		t.Fatal("readiness did not turn false")
	}
	if response, err := (&http.Client{Timeout: 100 * time.Millisecond}).Get("http://" + listener.Addr().String()); err == nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Fatalf("close response body: %v", closeErr)
		}
		t.Fatal("new request was accepted during shutdown")
	}
	close(release)
	if err := <-responseDone; err != nil {
		t.Fatalf("in-flight request failed: %v", err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("ServeListener() error = %v", err)
	}
}
