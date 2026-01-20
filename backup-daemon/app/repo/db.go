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
	RemoveVault(ctx context.Context, vault string) error
	SelectEverything(ctx context.Context, taskID string) (entity.Job, error)
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
		insert into jobs (task_id, type, status, vault, err, storage_name, blob_path, databases)
		values ($1, $2, $3, $4, $5, $6, $7, $8)
		on conflict(task_id) do update set
			type         = excluded.type,
			status       = excluded.status,
			vault        = excluded.vault,
			err          = excluded.err,
			storage_name = excluded.storage_name,
			blob_path    = excluded.blob_path,
			databases    = COALESCE(NULLIF(excluded.databases, ''), jobs.databases);
	`

	_, err := d.db.WriterDB.ExecContext(
		ctx, upsertQuery,
		job.TaskID, job.Type, job.Status, job.Vault, job.Err,
		job.StorageName, job.BlobPath, job.Databases,
	)
	if err != nil {
		return fmt.Errorf("error updating job status: %w", err)
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
