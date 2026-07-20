package app_test

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/ilkerkorkut/connect-tickets/internal/app"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func TestBootstrapDependencyGraph(t *testing.T) {
	t.Run("gin engine constructs", func(t *testing.T) {
		engine := gin.New()
		if engine == nil {
			t.Fatal("expected gin engine")
		}
	})

	t.Run("sarama config constructs", func(t *testing.T) {
		cfg := sarama.NewConfig()
		if err := cfg.Validate(); err != nil {
			t.Fatalf("invalid sarama config: %v", err)
		}
	})

	t.Run("pgx pool config parses without connecting", func(t *testing.T) {
		cfg, err := pgxpool.ParseConfig("postgres://tracker:tracker@localhost:5432/tracker?sslmode=disable")
		if err != nil {
			t.Fatalf("parse pgx config: %v", err)
		}
		if cfg == nil {
			t.Fatal("expected pgx pool config")
		}
	})

	t.Run("redis client constructs from options without dialing", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		if client == nil {
			t.Fatal("expected redis client")
		}
	})
}

func TestRuntimeModesAreSelectable(t *testing.T) {
	expected := []app.Mode{
		app.ModeAPI,
		app.ModeWorker,
		app.ModePublisher,
		app.ModeAll,
		app.ModeMigrate,
		app.ModeHealthcheck,
	}
	if got := app.Modes(); len(got) != len(expected) {
		t.Fatalf("expected %d modes, got %d (%v)", len(expected), len(got), got)
	}
	for _, m := range expected {
		found := false
		for _, g := range app.Modes() {
			if g == m {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing required mode %q", m)
		}
	}
}

func TestParseMode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		args    []string
		want    app.Mode
		wantErr bool
	}{
		{"api mode", []string{"api"}, app.ModeAPI, false},
		{"worker mode", []string{"worker"}, app.ModeWorker, false},
		{"publisher mode", []string{"publisher"}, app.ModePublisher, false},
		{"all mode", []string{"all"}, app.ModeAll, false},
		{"migrate mode", []string{"migrate"}, app.ModeMigrate, false},
		{"healthcheck mode", []string{"healthcheck"}, app.ModeHealthcheck, false},
		{"missing mode", nil, "", true},
		{"empty mode", []string{""}, "", true},
		{"unknown mode", []string{"bogus"}, "", true},
		{"trimmed mode", []string{" api "}, app.ModeAPI, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := app.ParseMode(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for args=%v, got %q", tc.args, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
