package tasks

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

// TestTaskExecutor_resolveS3Client_EmptyStorageNameUsesDefaultAlias mirrors
// the real app.go wiring when S3 aliases are configured (see app/app/app.go):
// the registry holds only alias-name keys such as "default" -- it never gets
// an explicit "" entry -- while te.s3Client stands in for the credential-less
// top-level client that exists whenever real credentials live solely in the
// alias config. A task whose CustomVars carry no storageName (e.g. an
// internally-scheduled granular backup, as opposed to a REST-driven one where
// rest/helperV2.go's normalizeStorageName fills in "default") must still
// resolve through the registry via "default", not silently fall back to the
// credential-less client.
func TestTaskExecutor_resolveS3Client_EmptyStorageNameUsesDefaultAlias(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	defaultClient := utils.NewMockS3ClientRepository(ctrl)
	fallbackClient := utils.NewMockS3ClientRepository(ctrl)

	registry := utils.NewS3AliasRegistry(map[string]utils.S3ClientRepository{
		"default": defaultClient,
	})

	te := &TaskExecutor{
		s3Client:   fallbackClient,
		s3Registry: registry,
		logger:     zap.NewNop().Sugar(),
	}

	got := te.resolveS3Client("")
	if got != defaultClient {
		t.Fatalf("resolveS3Client(\"\") did not return the registry's %q alias client", "default")
	}
}

// TestTaskExecutor_resolveS3Client_UnknownAliasFallsBackToDefaultClient
// documents the intended behavior for a genuinely unknown storageName: it
// should still fall back to te.s3Client (with a warning), unlike the empty
// case above which is now normalized to "default" instead of being treated
// as "skip the registry".
func TestTaskExecutor_resolveS3Client_UnknownAliasFallsBackToDefaultClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fallbackClient := utils.NewMockS3ClientRepository(ctrl)
	registry := utils.NewS3AliasRegistry(map[string]utils.S3ClientRepository{
		"default": utils.NewMockS3ClientRepository(ctrl),
	})

	te := &TaskExecutor{
		s3Client:   fallbackClient,
		s3Registry: registry,
		logger:     zap.NewNop().Sugar(),
	}

	got := te.resolveS3Client("nonexistent")
	if got != fallbackClient {
		t.Fatalf("resolveS3Client(\"nonexistent\") should fall back to te.s3Client")
	}
}

// TestTaskExecutor_moveBackupToS3_GranularBackupUsesDefaultAlias reproduces
// the CPCAP-11911 production scenario directly against moveBackupToS3: a
// granular/scheduled backup task carries no storageName in CustomVars (that
// normalization only happens in the REST V2 handler layer), so it must still
// upload through the "default" alias client. Before the fix, resolveS3Client
// skipped the registry whenever storageName was "" and used te.s3Client
// instead -- which in an alias-only deployment carries empty credentials and
// fails with "static credentials are empty" on the real AWS SDK. Here the
// fallback client is a mock with no expectations set, so any call to it fails
// the test via gomock's unexpected-call panic.
func TestTaskExecutor_moveBackupToS3_GranularBackupUsesDefaultAlias(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	vaultDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(vaultDir, ".console"), []byte("log"), 0o644); err != nil {
		t.Fatalf("failed to seed vault dir: %v", err)
	}

	defaultClient := utils.NewMockS3ClientRepository(ctrl)
	defaultClient.EXPECT().UploadFolder(gomock.Any(), vaultDir).Return(nil)

	// fallbackClient stands in for the credential-less top-level client from
	// production; it has no EXPECT() calls set up, so it must never be used.
	fallbackClient := utils.NewMockS3ClientRepository(ctrl)

	registry := utils.NewS3AliasRegistry(map[string]utils.S3ClientRepository{
		"default": defaultClient,
	})

	te := &TaskExecutor{
		s3Client:   fallbackClient,
		s3Registry: registry,
		s3Enable:   true,
		logger:     zap.NewNop().Sugar(),
	}

	task := Task{
		Vault:      entity.Vault{Folder: vaultDir},
		CustomVars: map[string]string{}, // no storageName -- e.g. a scheduled granular backup
		Job:        entity.Job{Vault: "20260820T090114"},
	}

	if err := te.moveBackupToS3(context.Background(), task); err != nil {
		t.Fatalf("moveBackupToS3 returned unexpected error: %v", err)
	}
}
