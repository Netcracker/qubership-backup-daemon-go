package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/db"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
)

type DBRepository interface {
	UpdateJob(ctx context.Context, job entity.Job) error
	RemoveJob(ctx context.Context, taskID string) error
	RemoveVault(ctx context.Context, vault string) error
	SelectEverything(ctx context.Context, taskID string) (entity.Job, error)
	ListVaultNames(ctx context.Context) ([]string, error)
}

var ErrNotFound = errors.New("sql: no rows in result set")
var ErrNoVaults = errors.New("no vaults found")

type DBRepo struct {
	db *db.Db
}

func NewDBRepo(db *db.Db) DBRepository {
	return &DBRepo{
		db: db,
	}
}

func (d *DBRepo) UpdateJob(ctx context.Context, job entity.Job) error {
	upsertQuery := `
		insert into jobs (task_id, type, status, vault, err, storage_name, blob_path, databases, creation_time, restore_databases, completion_time)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		on conflict(task_id) do update set
			type         = excluded.type,
			status       = excluded.status,
			vault        = excluded.vault,
			err          = excluded.err,
			storage_name = excluded.storage_name,
			blob_path    = excluded.blob_path,
			databases    = COALESCE(NULLIF(excluded.databases, ''), jobs.databases),
			creation_time = COALESCE(NULLIF(excluded.creation_time, ''), jobs.creation_time),
			restore_databases = COALESCE(NULLIF(excluded.restore_databases, ''), jobs.restore_databases),
			completion_time = COALESCE(NULLIF(excluded.completion_time, ''), jobs.completion_time);
	`

	_, err := d.db.WriterDB.ExecContext(
		ctx, upsertQuery,
		job.TaskID, job.Type, job.Status, job.Vault, job.Err,
		job.StorageName, job.BlobPath, job.Databases, job.CreationTime,
		job.RestoreDatabases, job.CompletionTime,
	)
	if err != nil {
		return fmt.Errorf("error updating job status: %w", err)
	}
	return nil
}

func (d *DBRepo) RemoveJob(ctx context.Context, taskID string) error {
	deleteWithTaskID := `delete from jobs where task_id = $1`
	res, err := d.db.WriterDB.ExecContext(ctx, deleteWithTaskID, taskID)
	if err != nil {
		return fmt.Errorf("unable to delete job %s from jobs database: %v", taskID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("unable to delete job %s from jobs database: %v", taskID, err)
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (d *DBRepo) RemoveVault(ctx context.Context, vault string) error {
	deleteWithVault := `delete from jobs where vault = $1`

	res, err := d.db.WriterDB.ExecContext(ctx, deleteWithVault, vault)
	if err != nil {
		return fmt.Errorf("unable to delete vault %s from jobs database: %v", vault, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("unable to delete vault %s from jobs database: %v", vault, err)
	}
	if rows == 0 {
		return ErrNoVaults
	}
	return nil
}

func (d *DBRepo) ListVaultNames(ctx context.Context) ([]string, error) {
	query := `select distinct vault from jobs where vault != ''`
	var vaults []string
	err := d.db.ReaderDB.SelectContext(ctx, &vaults, query)
	if err != nil {
		return nil, fmt.Errorf("error listing vault names: %w", err)
	}
	return vaults, nil
}

func (d *DBRepo) SelectEverything(ctx context.Context, taskID string) (entity.Job, error) {
	var job entity.Job
	query := `select * from jobs where task_id = $1`

	err := d.db.ReaderDB.QueryRowxContext(ctx, query, taskID).StructScan(&job)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return entity.Job{}, fmt.Errorf("no job found with task_id %s: %w", taskID, ErrNotFound)
		}
		return entity.Job{}, fmt.Errorf("error getting job: %w", err)
	}
	return job, nil
}
