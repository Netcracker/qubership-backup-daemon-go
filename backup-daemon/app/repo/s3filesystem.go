package repo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3FileSystem struct {
	client     utils.S3ClientRepository
	bucketName string
	ctx        context.Context
	dirMatcher *regexp.Regexp
}

func NewS3FileSystem(ctx context.Context, client utils.S3ClientRepository, bucketName string) FileSystem {
	return &S3FileSystem{
		client:     client,
		bucketName: bucketName,
		ctx:        ctx,
		dirMatcher: regexp.MustCompile(`(?i)\d{8}T\d{4,6}`),
	}
}

func (s *S3FileSystem) Exists(path string) bool {
	exists, err := s.client.PrefixExists(s.ctx, path)
	if err != nil {
		return false
	}
	return exists
}

func (s *S3FileSystem) ExistsFile(path string) bool {
	key := strings.Trim(path, "/")
	_, err := s.client.RawClient().HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(key),
	})
	return err == nil
}

// MkdirAll is a no-op for flat object stores (no real directories).
func (s *S3FileSystem) MkdirAll(_ string) error {
	return nil
}

func (s *S3FileSystem) ListDir(path string) ([]string, error) {
	prefix := strings.Trim(path, "/") + "/"
	commonPrefixes, err := s.client.ListCommonPrefixes(s.ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("s3 list dir %s: %w", path, err)
	}

	var dirs []string
	for _, commonPrefix := range commonPrefixes {
		rel := strings.TrimPrefix(commonPrefix, prefix)
		child := strings.TrimSuffix(rel, "/")
		if child == "" {
			continue
		}
		childParts := strings.Split(child, "_")
		if s.dirMatcher.MatchString(childParts[len(childParts)-1]) {
			dirs = append(dirs, child)
		}
	}
	return dirs, nil
}

func (s *S3FileSystem) RemoveAll(path string) error {
	prefix := strings.Trim(path, "/")
	if prefix == "" {
		return fmt.Errorf("refusing to delete empty S3 prefix")
	}
	return s.client.DeletePrefix(s.ctx, prefix)
}

func (s *S3FileSystem) Remove(path string) error {
	return s.RemoveAll(path)
}

func (s *S3FileSystem) TouchFile(path string) error {
	key := strings.Trim(path, "/")
	_, err := s.client.RawClient().PutObject(s.ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucketName),
		Key:           aws.String(key),
		Body:          bytes.NewReader([]byte{}),
		ContentLength: aws.Int64(0),
	})
	if err != nil {
		return fmt.Errorf("s3 touch %s: %w", path, err)
	}
	return nil
}

func (s *S3FileSystem) ReadFile(path string) ([]byte, error) {
	key := strings.Trim(path, "/")
	resp, err := s.client.RawClient().GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 read file %s: %w", path, err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("Error closing file: %s\n", err)
		}
	}(resp.Body)
	return io.ReadAll(resp.Body)
}

func (s *S3FileSystem) WriteFile(path string, content []byte) error {
	key := strings.Trim(path, "/")
	_, err := s.client.RawClient().PutObject(s.ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucketName),
		Key:           aws.String(key),
		Body:          bytes.NewReader(content),
		ContentLength: aws.Int64(int64(len(content))),
	})
	if err != nil {
		return fmt.Errorf("s3 write file %s: %w", path, err)
	}
	return nil
}

func (s *S3FileSystem) GetType() string {
	return "s3"
}

// Health checks S3 accessibility via HeadBucket — verifies the bucket exists
// and credentials are valid without transferring any object data.
func (s *S3FileSystem) Health(_ string) error {
	_, err := s.client.RawClient().HeadBucket(s.ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.bucketName),
	})
	if err != nil {
		return fmt.Errorf("s3 bucket %q not accessible: %w", s.bucketName, err)
	}
	return nil
}
