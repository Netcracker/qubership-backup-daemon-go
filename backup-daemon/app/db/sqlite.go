package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

type Db struct {
	WriterDB *sqlx.DB
	ReaderDB *sqlx.DB
}

// sqlitePragmas are applied per connection via the modernc.org/sqlite DSN.
//   - journal_mode=WAL: allow concurrent reads while a write is in progress.
//   - busy_timeout=5000: wait up to 5s on a locked db instead of failing.
//   - cache_size=-2048: cap the page cache at 2 MiB (negative = KiB) to keep
//     the memory footprint small in constrained containers.
const sqlitePragmas = "_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=cache_size(-2048)"

// openDB opens a pooled SQLite connection with the shared pragmas applied.
func openDB(dbPath string, maxOpen, maxIdle int) (*sqlx.DB, error) {
	dsn := fmt.Sprintf("%s?%s", dbPath, sqlitePragmas)
	db, err := sqlx.Connect("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	return db, nil
}

func NewConnection(dbPath string) (*Db, error) {
	if dbPath == "" {
		dbPath = "./database.db"
	}
	if dir := filepath.Dir(dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create db dir %s: %w", dir, err)
		}
	}

	// SQLite allows only one writer at a time, so the writer pool is capped at 1
	// to avoid "database is locked" errors. Readers can use a small pool.
	writerDB, err := openDB(dbPath, 1, 1)
	if err != nil {
		return nil, err
	}
	readerDB, err := openDB(dbPath, 4, 2)
	if err != nil {
		return nil, err
	}

	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		task_id      TEXT PRIMARY KEY,
		type         TEXT,
		status       TEXT,
		vault        TEXT,
		err          TEXT
	);`
	if _, err := writerDB.Exec(schema); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	if err := MigrateSchema(writerDB); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	if err := writerDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	if err := readerDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Db{
		WriterDB: writerDB,
		ReaderDB: readerDB,
	}, nil
}

func (db *Db) Close() error {
	var errs []error
	err := db.WriterDB.Close()
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to close writer: %v", err))
	}
	err = db.ReaderDB.Close()
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to close reader: %v", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}

func MigrateSchema(db1 *sqlx.DB) error {
	blob_path_column := `
	ALTER TABLE jobs ADD COLUMN blob_path TEXT;
	`
	if _, err := db1.Exec(blob_path_column); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("failed to add blob_path column: %v", err)
		}
	}

	creation_time_column := `
	ALTER TABLE jobs ADD COLUMN creation_time TEXT;
	`

	if _, err := db1.Exec(creation_time_column); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("failed to add creation_time column: %v", err)
		}
	}

	storage_name_column := `
	ALTER TABLE jobs ADD COLUMN storage_name TEXT;
	`
	if _, err := db1.Exec(storage_name_column); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("failed to add storage_name column: %v", err)
		}
	}

	databases_column := `
	ALTER TABLE jobs ADD COLUMN databases TEXT;
	`
	if _, err := db1.Exec(databases_column); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("failed to add databases column: %v", err)
		}
	}

	restore_databases_column := `
	ALTER TABLE jobs ADD COLUMN restore_databases TEXT;
	`
	if _, err := db1.Exec(restore_databases_column); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("failed to add restore_databases column: %v", err)
		}
	}

	completion_time_column := `
	ALTER TABLE jobs ADD COLUMN completion_time TEXT;
	`
	if _, err := db1.Exec(completion_time_column); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("failed to add completion_time column: %v", err)
		}
	}
	return nil
}
