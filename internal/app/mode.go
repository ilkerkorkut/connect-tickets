package app

import (
	"errors"
	"fmt"
	"strings"
)

type Mode string

const (
	ModeAPI         Mode = "api"
	ModeWorker      Mode = "worker"
	ModePublisher   Mode = "publisher"
	ModeAll         Mode = "all"
	ModeMigrate     Mode = "migrate"
	ModeHealthcheck Mode = "healthcheck"
)

var ErrUnknownMode = errors.New("unknown runtime mode")

func Modes() []Mode {
	return []Mode{
		ModeAPI,
		ModeWorker,
		ModePublisher,
		ModeAll,
		ModeMigrate,
		ModeHealthcheck,
	}
}

func ParseMode(args []string) (Mode, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("%w: usage: github-tracker <%s>", ErrUnknownMode, joinModes())
	}
	mode := Mode(strings.TrimSpace(args[0]))
	for _, known := range Modes() {
		if mode == known {
			return mode, nil
		}
	}
	return "", fmt.Errorf("%w: %q (expected one of %s)", ErrUnknownMode, mode, joinModes())
}

func joinModes() string {
	names := make([]string, 0, len(Modes()))
	for _, m := range Modes() {
		names = append(names, string(m))
	}
	return strings.Join(names, "|")
}
