package app

import (
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/config"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	"go.uber.org/mock/gomock"
)

// TestLegacyS3Client_DefaultAliasConfigured covers an alias-only deployment:
// legacy uploads go through the "default" alias, so vault lookup and eviction
// must use the same client and bucket instead of the credential-less top-level one.
func TestLegacyS3Client_DefaultAliasConfigured(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topLevelClient := utils.NewMockS3ClientRepository(ctrl)
	aliasClient := utils.NewMockS3ClientRepository(ctrl)
	registry := utils.NewS3AliasRegistry(map[string]utils.S3ClientRepository{
		defaultS3Alias: aliasClient,
	})
	cfg := &config.Config{
		BucketName: "",
		S3Aliases: map[string]config.Alias{
			defaultS3Alias: {Name: defaultS3Alias, BucketName: "alias-bucket"},
		},
	}

	client, bucket := legacyS3Client(cfg, topLevelClient, registry)
	if client != aliasClient {
		t.Fatalf("legacyS3Client did not return the %q alias client", defaultS3Alias)
	}
	if bucket != "alias-bucket" {
		t.Fatalf("legacyS3Client bucket = %q, want %q", bucket, "alias-bucket")
	}
}

// TestLegacyS3Client_NoAliases keeps the plain S3 deployment unchanged: the
// registry wraps the top-level client, and its bucket comes from the main config.
func TestLegacyS3Client_NoAliases(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topLevelClient := utils.NewMockS3ClientRepository(ctrl)
	registry := utils.NewS3AliasRegistry(map[string]utils.S3ClientRepository{
		defaultS3Alias: topLevelClient,
	})
	cfg := &config.Config{BucketName: "main-bucket"}

	client, bucket := legacyS3Client(cfg, topLevelClient, registry)
	if client != topLevelClient {
		t.Fatalf("legacyS3Client should return the top-level client when no aliases are configured")
	}
	if bucket != "main-bucket" {
		t.Fatalf("legacyS3Client bucket = %q, want %q", bucket, "main-bucket")
	}
}

// TestLegacyS3Client_NoDefaultAlias falls back to the top-level client when
// aliases exist but none is named "default".
func TestLegacyS3Client_NoDefaultAlias(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topLevelClient := utils.NewMockS3ClientRepository(ctrl)
	registry := utils.NewS3AliasRegistry(map[string]utils.S3ClientRepository{
		"archive": utils.NewMockS3ClientRepository(ctrl),
	})
	cfg := &config.Config{
		BucketName: "main-bucket",
		S3Aliases: map[string]config.Alias{
			"archive": {Name: "archive", BucketName: "archive-bucket"},
		},
	}

	client, bucket := legacyS3Client(cfg, topLevelClient, registry)
	if client != topLevelClient {
		t.Fatalf("legacyS3Client should return the top-level client when there is no %q alias", defaultS3Alias)
	}
	if bucket != "main-bucket" {
		t.Fatalf("legacyS3Client bucket = %q, want %q", bucket, "main-bucket")
	}
}
