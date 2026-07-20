package httpapi

import "errors"

var ErrInvalidServer = errors.New("invalid http server")

type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}
