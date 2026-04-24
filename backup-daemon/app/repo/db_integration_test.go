package repo

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/db"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
)

func newTestDB(t *testing.T) *db.Db {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "database.db")

	conn, err := db.NewConnection(dbPath)
	if err != nil {
		t.Fatalf("Failed to connect to DB: %v", err)
	}
	return conn
}

func TestUpdateJob_Integration(t *testing.T) {
	testCases := []struct {
		name        string
		job         entity.Job
		expectedErr error
	}{
		{
			name: "success",
			job: entity.Job{
				TaskID: "task-1",
				Type:   "backup",
				Status: "pending",
				Vault:  "vault1",
				Err:    "",
			},
			expectedErr: nil,
		},
		{
			name: "update",
			job: entity.Job{
				TaskID: "task-1",
				Type:   "backup2",
				Status: "success",
				Vault:  "vault2",
				Err:    "",
			},
			expectedErr: nil,
		},
	}

	dbConn := newTestDB(t)
	defer func() {
		_ = dbConn.Close()
	}()

	repo := NewDBRepo(dbConn)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := repo.UpdateJob(context.Background(), tc.job)
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("update job err: expected: %v, got: %v", tc.expectedErr, err)
			}
		})
	}
}

func TestSelectEverything_Integration(t *testing.T) {
	dbConn := newTestDB(t)
	defer func() {
		_ = dbConn.Close()
	}()

	repo := NewDBRepo(dbConn)
	seed := entity.Job{
		TaskID: "task-1",
		Type:   "backup2",
		Status: "success",
		Vault:  "vault2",
		Err:    "",
	}
	if err := repo.UpdateJob(context.Background(), seed); err != nil {
		t.Fatalf("seed UpdateJob failed: %v", err)
	}

	testCases := []struct {
		name        string
		taskID      string
		job         entity.Job
		expectedErr error
	}{
		{
			name:        "success",
			taskID:      "task-1",
			job:         seed,
			expectedErr: nil,
		},
		{
			name:        "job not found",
			taskID:      "task-2",
			job:         entity.Job{},
			expectedErr: ErrNotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			job, err := repo.SelectEverything(context.Background(), tc.taskID)
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected %v, got: %v", tc.expectedErr, err)
			}
			if job != tc.job {
				t.Fatalf("expected job: %v, got: %v", tc.job, job)
			}
		})
	}
}

func TestRemoveVault_Integration(t *testing.T) {
	dbConn := newTestDB(t)
	defer func() {
		_ = dbConn.Close()
	}()

	repo := NewDBRepo(dbConn)

	seed := entity.Job{
		TaskID: "task-1",
		Type:   "backup2",
		Status: "success",
		Vault:  "vault2",
		Err:    "",
	}
	if err := repo.UpdateJob(context.Background(), seed); err != nil {
		t.Fatalf("seed UpdateJob failed: %v", err)
	}

	testCases := []struct {
		name        string
		vault       string
		expectedErr error
	}{
		{
			name:        "success",
			vault:       "vault2",
			expectedErr: nil,
		},
		{
			name:        "vault not found",
			vault:       "vault1",
			expectedErr: ErrNoVaults,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := repo.RemoveVault(context.Background(), tc.vault)
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected %v, got: %v", tc.expectedErr, err)
			}
		})
	}
}
