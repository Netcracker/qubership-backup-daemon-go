package repo

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FileSystem abstracts local and S3 filesystem operations used by StorageRepo.
// This mirrors the Python FileSystem / S3FileSystem classes in storage.py.
//
//go:generate mockgen -source=filesystem.go -destination=filesystem_mock.go -package=repo
type FileSystem interface {
	// Exists returns true if the path (directory or object prefix) exists.
	Exists(path string) bool
	// ExistsFile returns true if the file (object key) exists.
	ExistsFile(path string) bool
	// MkdirAll creates directories (no-op for flat object-stores).
	MkdirAll(path string) error
	// ListDir returns the direct children (names only) of a directory / prefix.
	ListDir(path string) ([]string, error)
	// RemoveAll deletes a directory tree (or S3 prefix).
	RemoveAll(path string) error
	// Remove deletes a single file / object.
	Remove(path string) error
	// TouchFile creates an empty file / zero-byte object.
	TouchFile(path string) error
	// GetType returns "fs" or "s3".
	GetType() string
	// ReadFile reading file into []byte, only for meta file usage
	ReadFile(path string) ([]byte, error)
	WriteFile(path string, content []byte) error
	// Health checks that the storage backend is accessible.
	Health(path string) error
}

type LocalFileSystem struct{}

func NewLocalFileSystem() FileSystem {
	return &LocalFileSystem{}
}

func (l *LocalFileSystem) Exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func (l *LocalFileSystem) ExistsFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func (l *LocalFileSystem) MkdirAll(path string) error {
	return os.MkdirAll(path, 0755)
}

func (l *LocalFileSystem) ListDir(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

func (l *LocalFileSystem) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (l *LocalFileSystem) Remove(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (l *LocalFileSystem) TouchFile(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	return f.Close()
}

func (l *LocalFileSystem) ReadFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			fmt.Printf("Error closing file: %s\n", err)
		}
	}(f)
	return io.ReadAll(f)
}

func (l *LocalFileSystem) WriteFile(path string, content []byte) error {
	return os.WriteFile(path, content, 0644)
}

func (l *LocalFileSystem) GetType() string {
	return "fs"
}

func (l *LocalFileSystem) Health(path string) error {
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("storage path %q not accessible: %w", path, err)
	}
	return nil
}
