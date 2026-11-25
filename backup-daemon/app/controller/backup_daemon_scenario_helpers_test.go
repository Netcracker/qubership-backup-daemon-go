package controller

import (
	"context"
	"testing"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
)

// Shared helpers for the full-lifecycle scenario tests (both the MinIO-backed
// variant in backup_daemon_scenario_integration_test.go and the local
// filesystem variant in backup_daemon_scenario_localfs_test.go). Kept in an
// untagged file so both build configurations can use them without duplication.

func safeName(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		if r == '/' || r == ' ' {
			out = append(out, '_')
			continue
		}
		out = append(out, r)
	}
	return string(out)
}

func containsStr(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// waitForJob polls GetJobStatus until the job leaves "Queued"/"Processing", or fails the test on timeout.
func waitForJob(t *testing.T, daemon BackupDaemonUseCase, taskID string) entity.JobStatusResponse {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		status, err := daemon.GetJobStatus(context.Background(), entity.JobStatusRequest{TaskID: taskID})
		if err != nil {
			t.Fatalf("GetJobStatus(%s): %v", taskID, err)
		}
		switch status.Status {
		case "Successful", "Failed":
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for job %s to finish, last status=%q", taskID, status.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
