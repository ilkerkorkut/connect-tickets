package migrations

import (
	"strings"
	"testing"
)

func TestLoadMigrations(t *testing.T) {
	t.Parallel()

	migrations, err := load()
	if err != nil {
		t.Fatalf("load() error = %v", err)
	}
	if len(migrations) != 2 {
		t.Fatalf("len(load()) = %d, want 2", len(migrations))
	}
	migration := migrations[0]
	if migration.version != "001" {
		t.Fatalf("version = %q, want 001", migration.version)
	}
	for _, fragment := range []string{
		"CREATE TABLE repositories",
		"CREATE TABLE batch_jobs",
		"CREATE TABLE job_attempts",
		"CREATE TABLE outbox_events",
		"CREATE TABLE repository_changes",
		"INSERT INTO change_sequences",
		"UNIQUE (batch_id, repo_id)",
		"ON DELETE RESTRICT",
	} {
		if !strings.Contains(migration.sql, fragment) {
			t.Errorf("migration does not contain %q", fragment)
		}
	}
	if migrations[1].version != "002" || !strings.Contains(migrations[1].sql, "CREATE TABLE invalid_refresh_messages") {
		t.Fatalf("worker-message migration is missing")
	}
}

func TestSplitStatements(t *testing.T) {
	t.Parallel()

	got := splitStatements("  SELECT 1;\n; SELECT 2 ; ")
	if len(got) != 2 || got[0] != "SELECT 1" || got[1] != "SELECT 2" {
		t.Fatalf("splitStatements() = %#v", got)
	}
}
