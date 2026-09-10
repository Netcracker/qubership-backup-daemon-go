//go:build integration

package utils

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// minioClient builds an S3Client pointed at the MinIO instance configured via
// environment variables set by scripts/test-s3-integration.sh.
func minioClient(t *testing.T) S3ClientRepository {
	t.Helper()
	url := getenv("MINIO_URL", "http://localhost:9000")
	key := getenv("MINIO_ACCESS_KEY", "minioadmin")
	secret := getenv("MINIO_SECRET_KEY", "minioadmin")
	bucket := getenv("MINIO_BUCKET", "test-bucket")
	region := getenv("MINIO_REGION", "us-east-1")

	client, err := NewS3Client(context.Background(), url, key, secret, bucket, region, true, "")
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	return client
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// prefix returns a unique per-test S3 prefix so tests do not collide.
func prefix(t *testing.T, suffix string) string {
	t.Helper()
	safe := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return fmt.Sprintf("integration-tests/%s/%s", safe, suffix)
}

// TestIntegration_UploadAndListFiles uploads a folder with two files and verifies
// that ListFiles returns both keys under the expected prefix.
func TestIntegration_UploadAndListFiles(t *testing.T) {
	ctx := context.Background()
	client := minioClient(t)
	p := prefix(t, "vault1")

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "dump.tar.gz"), []byte("backup data"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(`{"ok":true}`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := client.UploadFolderWithPrefix(ctx, dir, p); err != nil {
		t.Fatalf("UploadFolderWithPrefix: %v", err)
	}
	t.Cleanup(func() { _ = client.DeletePrefix(ctx, p) })

	files, err := client.ListFiles(ctx, p)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d: %v", len(files), files)
	}
}

// TestIntegration_ListCommonPrefixes_GranularFallback reproduces the production
// scenario: a granular backup has DB sub-prefixes in S3 but the local folder is
// gone. ListCommonPrefixes must return the DB names so GetBackupStats can build
// the db_list without shelling out.
func TestIntegration_ListCommonPrefixes_GranularFallback(t *testing.T) {
	ctx := context.Background()
	client := minioClient(t)

	vaultPrefix := prefix(t, "20260730T061234")
	db1Prefix := vaultPrefix + "/db1"
	db2Prefix := vaultPrefix + "/db2"

	// Upload a sentinel file under each DB sub-prefix to create the "directory".
	for _, dbPfx := range []string{db1Prefix, db2Prefix} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "dump.sql"), []byte("-- sql"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := client.UploadFolderWithPrefix(ctx, dir, dbPfx); err != nil {
			t.Fatalf("UploadFolderWithPrefix(%s): %v", dbPfx, err)
		}
	}
	t.Cleanup(func() { _ = client.DeletePrefix(ctx, vaultPrefix) })

	// ListCommonPrefixes is what GetBackupStats calls when GetBackupDBs fails.
	prefixes, err := client.ListCommonPrefixes(ctx, vaultPrefix)
	if err != nil {
		t.Fatalf("ListCommonPrefixes: %v", err)
	}

	parent := strings.TrimRight(vaultPrefix, "/") + "/"
	var dbNames []string
	for _, p := range prefixes {
		name := strings.TrimSuffix(strings.TrimPrefix(p, parent), "/")
		if name != "" {
			dbNames = append(dbNames, name)
		}
	}

	if len(dbNames) != 2 {
		t.Fatalf("expected 2 DB prefixes, got %d: %v", len(dbNames), dbNames)
	}
	for _, want := range []string{"db1", "db2"} {
		found := false
		for _, got := range dbNames {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected DB %q in prefixes, got: %v", want, dbNames)
		}
	}
}

// TestIntegration_UploadDownloadRoundtrip uploads a directory and downloads it
// back to a different local directory, verifying content is preserved.
func TestIntegration_UploadDownloadRoundtrip(t *testing.T) {
	ctx := context.Background()
	client := minioClient(t)
	p := prefix(t, "roundtrip")

	const content = "hello from roundtrip test"
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "data.bin"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	if err := client.UploadFolderWithPrefix(ctx, dir, p); err != nil {
		t.Fatalf("UploadFolderWithPrefix: %v", err)
	}
	t.Cleanup(func() { _ = client.DeletePrefix(ctx, p) })

	dst := t.TempDir()
	if err := client.DownloadFolder(ctx, p, dst); err != nil {
		t.Fatalf("DownloadFolder: %v", err)
	}

	// DownloadFolder strips the S3 prefix from the key to get a relative path.
	// Walk dst to find any file with our content.
	found := false
	if err := filepath.Walk(dst, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if bytes.Contains(data, []byte(content)) {
			found = true
		}
		return nil
	}); err != nil {
		t.Fatalf("walking dst: %v", err)
	}
	if !found {
		t.Fatalf("downloaded content not found in %s", dst)
	}
}

// TestIntegration_DeletePrefix uploads files under a prefix then deletes the
// whole prefix, verifying that ListFiles returns empty afterwards.
func TestIntegration_DeletePrefix(t *testing.T) {
	ctx := context.Background()
	client := minioClient(t)
	p := prefix(t, "to-delete")

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := client.UploadFolderWithPrefix(ctx, dir, p); err != nil {
		t.Fatalf("UploadFolderWithPrefix: %v", err)
	}

	if err := client.DeletePrefix(ctx, p); err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}

	files, err := client.ListFiles(ctx, p)
	if err != nil {
		t.Fatalf("ListFiles after delete: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected 0 files after DeletePrefix, got %d: %v", len(files), files)
	}
}

// TestIntegration_PrefixExists checks that PrefixExists returns true after
// upload and false after deletion.
func TestIntegration_PrefixExists(t *testing.T) {
	ctx := context.Background()
	client := minioClient(t)
	p := prefix(t, "exists-check")

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "x"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := client.UploadFolderWithPrefix(ctx, dir, p); err != nil {
		t.Fatalf("UploadFolderWithPrefix: %v", err)
	}
	t.Cleanup(func() { _ = client.DeletePrefix(ctx, p) })

	exists, err := client.PrefixExists(ctx, p)
	if err != nil {
		t.Fatalf("PrefixExists: %v", err)
	}
	if !exists {
		t.Fatal("expected PrefixExists=true after upload, got false")
	}

	if err := client.DeletePrefix(ctx, p); err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}

	exists, err = client.PrefixExists(ctx, p)
	if err != nil {
		t.Fatalf("PrefixExists after delete: %v", err)
	}
	if exists {
		t.Fatal("expected PrefixExists=false after delete, got true")
	}
}
