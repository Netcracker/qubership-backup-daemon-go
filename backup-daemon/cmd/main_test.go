package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeAliasesFile(t *testing.T, dir string, contents string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, S3AliasesFile), []byte(contents), 0o644); err != nil {
		t.Fatalf("failed to write %s: %v", S3AliasesFile, err)
	}
}

func TestLoadS3Aliases_Success(t *testing.T) {
	dir := t.TempDir()
	writeAliasesFile(t, dir, `{
		"default": {"s3Url": "http://minio:9000", "accessKeyId": "id-1", "accessKeySecret": "secret-1", "bucketName": "bucket-1", "region": "us-east-1"},
		"archive": {"s3Url": "http://minio2:9000", "accessKeyId": "id-2", "accessKeySecret": "secret-2", "bucketName": "bucket-2", "region": "eu-west-1"}
	}`)

	aliases, err := loadS3Aliases(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(aliases) != 2 {
		t.Fatalf("expected 2 aliases, got %d: %+v", len(aliases), aliases)
	}

	def, ok := aliases["default"]
	if !ok {
		t.Fatalf("expected %q alias to be present, got %+v", "default", aliases)
	}
	if def.Name != "default" {
		t.Errorf("expected Name to be backfilled to the map key %q, got %q", "default", def.Name)
	}
	if def.AccessKeyID != "id-1" || def.AccessKeySecret != "secret-1" || def.BucketName != "bucket-1" {
		t.Errorf("unexpected default alias fields: %+v", def)
	}

	archive, ok := aliases["archive"]
	if !ok {
		t.Fatalf("expected %q alias to be present, got %+v", "archive", aliases)
	}
	if archive.Name != "archive" || archive.BucketName != "bucket-2" {
		t.Errorf("unexpected archive alias fields: %+v", archive)
	}
}

func TestLoadS3Aliases_MissingFile(t *testing.T) {
	dir := t.TempDir() // no s3_aliases.json written

	_, err := loadS3Aliases(dir)
	if err == nil {
		t.Fatal("expected error for missing aliases file, got nil")
	}
}

func TestLoadS3Aliases_MalformedJSON(t *testing.T) {
	dir := t.TempDir()
	writeAliasesFile(t, dir, `{"default": {"s3Url": "http://minio:9000",}`) // trailing comma, unclosed brace

	_, err := loadS3Aliases(dir)
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

func TestLoadS3Aliases_EmptyObject(t *testing.T) {
	dir := t.TempDir()
	writeAliasesFile(t, dir, `{}`)

	aliases, err := loadS3Aliases(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(aliases) != 0 {
		t.Fatalf("expected 0 aliases, got %d: %+v", len(aliases), aliases)
	}
}

// TestLoadS3Aliases_MissingCredentialsAreNotRejected documents the actual
// (risky) behavior behind CPCAP-11911: loadS3Aliases performs no validation
// at all. An alias entry with no accessKeyId/accessKeySecret/bucketName
// parses successfully with zero-value fields instead of failing fast at
// startup -- the resulting client only fails later, at first real S3 call
// (see utils.NewS3Client, which never validates its inputs either). If this
// test starts failing because loadS3Aliases gained validation, that's a
// deliberate improvement and this test (and the comment) should be updated
// to match the new, stricter contract.
func TestLoadS3Aliases_MissingCredentialsAreNotRejected(t *testing.T) {
	dir := t.TempDir()
	writeAliasesFile(t, dir, `{"default": {"s3Url": "http://minio:9000"}}`)

	aliases, err := loadS3Aliases(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	def, ok := aliases["default"]
	if !ok {
		t.Fatalf("expected %q alias to be present, got %+v", "default", aliases)
	}
	if def.AccessKeyID != "" || def.AccessKeySecret != "" || def.BucketName != "" {
		t.Fatalf("expected blank credentials/bucket to pass through unchanged, got %+v", def)
	}
}

// TestLoadS3Aliases_EmptyAliasName documents another gap in the same vein: a
// blank key in the JSON object (an alias with "no name") is accepted as-is
// and backfilled to Name == "" rather than being rejected. Such an alias is
// only reachable via storageName == "" -- which normalizeStorageName
// (rest/helperV2.go) rewrites to "default" before it would ever get here,
// but internal/scheduled callers that build CustomVars directly are not
// guaranteed to go through that normalization.
func TestLoadS3Aliases_EmptyAliasName(t *testing.T) {
	dir := t.TempDir()
	writeAliasesFile(t, dir, `{"": {"s3Url": "http://minio:9000", "bucketName": "bucket-1"}}`)

	aliases, err := loadS3Aliases(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	empty, ok := aliases[""]
	if !ok {
		t.Fatalf("expected an alias keyed by the empty string, got %+v", aliases)
	}
	if empty.Name != "" {
		t.Errorf("expected Name to be backfilled to the empty map key, got %q", empty.Name)
	}
}
