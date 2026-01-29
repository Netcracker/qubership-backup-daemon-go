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

func NewConnection(dbPath string) (*Db, error) {
	if dbPath == "" {
		dbPath = "./database.db"
	}
	if dir := filepath.Dir(dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create db dir %s: %w", dir, err)
		}
	}
	db1, err := sqlx.Connect("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	db2, err := sqlx.Connect("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	_, err = db1.Exec("PRAGMA journal_mode = WAL;")
	if err != nil {
		return nil, fmt.Errorf("failed to enable WAL mode: %v", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		task_id      TEXT PRIMARY KEY,
		type         TEXT,
		status       TEXT,
		vault        TEXT,
		err          TEXT
	);`

	if _, err := db1.Exec(schema); err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	// migration
	if err := MigrateSchema(db1); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %v", err)
	}

	if err := db1.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}
	if err := db2.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}
	return &Db{
		WriterDB: db1,
		ReaderDB: db2,
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
	return nil
}
