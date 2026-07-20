package migrations

import (
	"context"
	"crypto/sha256"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var files embed.FS

var ErrChecksumMismatch = errors.New("migration checksum mismatch")

type migration struct {
	version  string
	filename string
	sql      string
	checksum [sha256.Size]byte
}

func Apply(ctx context.Context, db *pgxpool.Pool) error {
	if db == nil {
		return errors.New("migration database connection is required")
	}
	conn, err := db.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()
	return apply(ctx, conn)
}

type transactionStarter interface {
	Begin(context.Context) (pgx.Tx, error)
}

func apply(ctx context.Context, db transactionStarter) error {
	migrations, err := load()
	if err != nil {
		return err
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migrations transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(849170211)"); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations (version varchar(255) PRIMARY KEY, checksum bytea NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())"); err != nil {
		return fmt.Errorf("create migration ledger: %w", err)
	}

	for _, migration := range migrations {
		var checksum []byte
		err := tx.QueryRow(ctx, "SELECT checksum FROM schema_migrations WHERE version = $1", migration.version).Scan(&checksum)
		switch {
		case err == nil:
			if string(checksum) != string(migration.checksum[:]) {
				return fmt.Errorf("%w: %s", ErrChecksumMismatch, migration.filename)
			}
			continue
		case errors.Is(err, pgx.ErrNoRows):
		default:
			return fmt.Errorf("read migration ledger: %w", err)
		}

		for _, statement := range splitStatements(migration.sql) {
			if _, err := tx.Exec(ctx, statement); err != nil {
				return fmt.Errorf("apply %s: %w", migration.filename, err)
			}
		}
		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)", migration.version, migration.checksum[:]); err != nil {
			return fmt.Errorf("record %s: %w", migration.filename, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

func load() ([]migration, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	migrations := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		version, _, ok := strings.Cut(entry.Name(), "_")
		if !ok || version == "" {
			return nil, fmt.Errorf("migration %q must begin with a version followed by an underscore", entry.Name())
		}
		contents, err := files.ReadFile(entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		migrations = append(migrations, migration{version: version, filename: entry.Name(), sql: string(contents), checksum: sha256.Sum256(contents)})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })
	if len(migrations) == 0 {
		return nil, errors.New("no embedded migrations")
	}
	for i := 1; i < len(migrations); i++ {
		if migrations[i-1].version == migrations[i].version {
			return nil, fmt.Errorf("duplicate migration version %q", migrations[i].version)
		}
	}
	return migrations, nil
}

func splitStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		if statement := strings.TrimSpace(part); statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}
