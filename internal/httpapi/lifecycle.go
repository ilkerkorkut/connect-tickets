package httpapi

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

type Readiness interface {
	SetReady(bool)
}

func Serve(ctx context.Context, server *http.Server, readiness Readiness, timeout time.Duration) error {
	if server == nil || timeout <= 0 {
		return ErrInvalidServer
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}
	return ServeListener(ctx, server, listener, readiness, timeout)
}

func ServeListener(ctx context.Context, server *http.Server, listener net.Listener, readiness Readiness, timeout time.Duration) error {
	if server == nil || listener == nil || timeout <= 0 {
		return ErrInvalidServer
	}
	if readiness != nil {
		readiness.SetReady(true)
	}
	errs := make(chan error, 1)
	go func() { errs <- server.Serve(listener) }()
	select {
	case err := <-errs:
		if readiness != nil {
			readiness.SetReady(false)
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		if readiness != nil {
			readiness.SetReady(false)
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
		shutdownErr := server.Shutdown(shutdownCtx)
		cancel()
		serveErr := <-errs
		if shutdownErr != nil {
			return shutdownErr
		}
		if !errors.Is(serveErr, http.ErrServerClosed) {
			return serveErr
		}
		return ctx.Err()
	}
}
