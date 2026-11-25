package repo

import (
	"context"
	"errors"
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	gomock "go.uber.org/mock/gomock"
)

// TestS3FileSystem_ListDir_UsesCommonPrefixes verifies that ListDir asks S3
// to group keys via ListCommonPrefixes instead of recursively enumerating
// every object under a vault, and correctly derives vault names from the
// returned common prefixes.
func TestS3FileSystem_ListDir_UsesCommonPrefixes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := utils.NewMockS3ClientRepository(ctrl)
	client.EXPECT().
		ListCommonPrefixes(gomock.Any(), "backup-storage/").
		Return([]string{
			"backup-storage/20260617T000000/",
			"backup-storage/granular/",
			"backup-storage/not-a-vault/",
		}, nil)

	fs := NewS3FileSystem(context.Background(), client, "test-bucket")

	dirs, err := fs.ListDir("backup-storage")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expected := []string{"20260617T000000"}
	if len(dirs) != len(expected) {
		t.Fatalf("expected %d dirs, got %d: %v", len(expected), len(dirs), dirs)
	}
	for i, d := range dirs {
		if d != expected[i] {
			t.Errorf("expected %s at index %d, got %s", expected[i], i, d)
		}
	}
}

func TestS3FileSystem_ListDir_GranularPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := utils.NewMockS3ClientRepository(ctrl)
	client.EXPECT().
		ListCommonPrefixes(gomock.Any(), "backup-storage/granular/").
		Return([]string{
			"backup-storage/granular/20260728T113920/",
			"backup-storage/granular/20260720T012805/",
		}, nil)

	fs := NewS3FileSystem(context.Background(), client, "test-bucket")

	dirs, err := fs.ListDir("backup-storage/granular")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expected := []string{"20260728T113920", "20260720T012805"}
	if len(dirs) != len(expected) {
		t.Fatalf("expected %d dirs, got %d: %v", len(expected), len(dirs), dirs)
	}
	for i, d := range dirs {
		if d != expected[i] {
			t.Errorf("expected %s at index %d, got %s", expected[i], i, d)
		}
	}
}

func TestS3FileSystem_ListDir_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := utils.NewMockS3ClientRepository(ctrl)
	client.EXPECT().
		ListCommonPrefixes(gomock.Any(), "backup-storage/").
		Return(nil, errors.New("s3 unreachable"))

	fs := NewS3FileSystem(context.Background(), client, "test-bucket")

	if _, err := fs.ListDir("backup-storage"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestS3FileSystem_Exists_UsesPrefixExists guards against Exists degrading
// into a full ListFiles-style listing (which used to make a root-level
// Exists check as expensive as scanning every object under every vault).
// It must delegate to the constant-cost PrefixExists instead.
func TestS3FileSystem_Exists(t *testing.T) {
	testCases := []struct {
		name     string
		exists   bool
		callErr  error
		expected bool
	}{
		{name: "exists", exists: true, expected: true},
		{name: "does not exist", exists: false, expected: false},
		{name: "s3 error treated as not existing", callErr: errors.New("s3 unreachable"), expected: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			client := utils.NewMockS3ClientRepository(ctrl)
			client.EXPECT().
				PrefixExists(gomock.Any(), "backup-storage").
				Return(tc.exists, tc.callErr)

			fs := NewS3FileSystem(context.Background(), client, "test-bucket")

			if got := fs.Exists("backup-storage"); got != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}
