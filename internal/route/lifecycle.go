package route

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

type Readiness interface {
	SetReady(bool)
}

func Serve(ctx context.Context, server *http.Server, readiness Readiness, timeout time.Duration, logger *slog.Logger) error {
	if server == nil || timeout <= 0 {
		return ErrInvalidServer
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}
	return ServeListener(ctx, server, listener, readiness, timeout, logger)
}

func ServeListener(ctx context.Context, server *http.Server, listener net.Listener, readiness Readiness, timeout time.Duration, logger *slog.Logger) error {
	if server == nil || listener == nil || timeout <= 0 {
		return ErrInvalidServer
	}
	if logger != nil {
		logger.Info("http server listening", "addr", listener.Addr().String())
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
			if logger != nil {
				logger.Info("http server stopped")
			}
			return nil
		}
		if logger != nil {
			logger.Error("http server failed", "err", err)
		}
		return err
	case <-ctx.Done():
		if readiness != nil {
			readiness.SetReady(false)
		}
		if logger != nil {
			logger.Info("http server shutdown initiated", "shutdown_timeout", timeout)
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
		shutdownErr := server.Shutdown(shutdownCtx)
		cancel()
		serveErr := <-errs
		if logger != nil {
			logger.Info("http server shutdown complete", "shutdown_err", shutdownErr, "serve_err", serveErr)
		}
		if shutdownErr != nil {
			return shutdownErr
		}
		if !errors.Is(serveErr, http.ErrServerClosed) {
			return serveErr
		}
		return ctx.Err()
	}
}
