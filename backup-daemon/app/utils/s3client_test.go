package utils

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	gomock "go.uber.org/mock/gomock"
)

func NewS3ClientWithInterfaces(client ClientInterface, presignClient PresignClientInterface,
	downloader DownloaderInterface, uploader UploaderInterface) *S3Client {
	return &S3Client{
		client:        client,
		PresignClient: presignClient,
		Downloader:    downloader,
		Uploader:      uploader,
	}
}

func TestCreatePresignedUrl(t *testing.T) {
	testCases := []struct {
		name               string
		objectName         string
		expiration         int
		expectedExpiration time.Duration
		expectedResponse   *v4.PresignedHTTPRequest
		expectedError      error
		expectedURL        string
	}{
		{
			name:               "success",
			objectName:         "objectName",
			expiration:         10,
			expectedExpiration: time.Duration(10) * time.Second,
			expectedResponse: &v4.PresignedHTTPRequest{
				URL: "url",
			},
			expectedError: nil,
			expectedURL:   "url",
		},
		{
			name:               "failure",
			objectName:         "objectName",
			expiration:         0,
			expectedExpiration: time.Duration(3600) * time.Second,
			expectedResponse:   &v4.PresignedHTTPRequest{},
			expectedError:      errors.New("s3 error"),
			expectedURL:        "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var capturedDuration time.Duration

			s3PresignClient := NewMockPresignClientInterface(ctrl)
			s3Client := NewMockClientInterface(ctrl)
			downloadClient := NewMockDownloaderInterface(ctrl)
			uploadClient := NewMockUploaderInterface(ctrl)

			s3PresignClient.EXPECT().PresignGetObject(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
					opts := &s3.PresignOptions{}
					for _, fn := range optFns {
						fn(opts)
					}
					capturedDuration = opts.Expires
					return tc.expectedResponse, tc.expectedError
				}).AnyTimes()

			s3clientRepository := NewS3ClientWithInterfaces(s3Client, s3PresignClient, downloadClient, uploadClient)

			response, err := s3clientRepository.CreatePresignedUrl(context.Background(), tc.objectName, tc.expiration)
			if !errors.Is(err, tc.expectedError) {
				t.Fatalf("expected err %v, got: %v", tc.expectedError, err)
			}
			if response != tc.expectedURL {
				t.Fatalf("expected url %v, got: %v", tc.expectedURL, response)
			}
			if capturedDuration != tc.expectedExpiration {
				t.Fatalf("expected duration %v, got: %v", tc.expiration, capturedDuration)
			}
		})
	}
}

func TestListFiles(t *testing.T) {
	testCases := []struct {
		name             string
		path             string
		expectedResponse *s3.ListObjectsV2Output
		expectedError    error
		expectedReturn   []string
	}{
		{
			name: "success",
			path: "objectName",
			expectedResponse: &s3.ListObjectsV2Output{
				Contents: []types.Object{
					{Key: aws.String("file1.txt")},
					{Key: aws.String("file2.txt")},
				},
			},
			expectedReturn: []string{"file1.txt", "file2.txt"},
			expectedError:  nil,
		},
		{
			name:             "failure",
			path:             "objectName",
			expectedResponse: nil,
			expectedReturn:   nil,
			expectedError:    errors.New("s3 error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			s3PresignClient := NewMockPresignClientInterface(ctrl)
			s3Client := NewMockClientInterface(ctrl)
			downloadClient := NewMockDownloaderInterface(ctrl)
			uploadClient := NewMockUploaderInterface(ctrl)

			s3Client.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()

			s3clientRepository := NewS3ClientWithInterfaces(s3Client, s3PresignClient, downloadClient, uploadClient)

			response, err := s3clientRepository.ListFiles(context.Background(), tc.path)
			if !errors.Is(err, tc.expectedError) {
				t.Fatalf("expected err %v, got: %v", tc.expectedError, err)
			}
			for i, f := range response {
				if f != tc.expectedReturn[i] {
					t.Errorf("expected %s, got %s", tc.expectedReturn[i], f)
				}
			}
		})
	}
}

func TestUploadFolder(t *testing.T) {
	// Create a temp dir with a file for the "failure" test case
	failureDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(failureDir, "test.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name                    string
		path                    string
		expectedHeadObjectError error
		expectedPutObjectError  error
		expectedError           error
		expectedUploadError     error
	}{
		{
			name:                    "success",
			path:                    "./",
			expectedHeadObjectError: nil,
			expectedError:           nil,
			expectedPutObjectError:  nil,
			expectedUploadError:     nil,
		},
		{
			name:                    "failure",
			path:                    failureDir,
			expectedPutObjectError:  errors.New("s3 error"),
			expectedHeadObjectError: nil,
			expectedError:           nil,
			expectedUploadError:     nil,
		},
		{
			name:                    "upload error",
			path:                    "./",
			expectedHeadObjectError: nil,
			expectedError: fmt.Errorf("error while uploading object to %s. The object is too large.\n"+
				"The maximum size for a multipart upload is 5TB", ""),
			expectedPutObjectError: nil,
			expectedUploadError: &smithy.GenericAPIError{
				Code:    "EntityTooLarge",
				Message: "Object too large",
				Fault:   smithy.FaultServer,
			},
		},
		{
			name:                    "wait error",
			path:                    "./",
			expectedHeadObjectError: nil,
			expectedError:           nil,
			expectedPutObjectError:  nil,
			expectedUploadError:     nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			s3PresignClient := NewMockPresignClientInterface(ctrl)
			s3Client := NewMockClientInterface(ctrl)
			downloadClient := NewMockDownloaderInterface(ctrl)
			uploadClient := NewMockUploaderInterface(ctrl)

			s3Client.EXPECT().HeadObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(&s3.HeadObjectOutput{}, tc.expectedHeadObjectError).AnyTimes()
			s3Client.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(&s3.PutObjectOutput{}, tc.expectedPutObjectError).AnyTimes()
			uploadClient.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
				if input != nil && input.Body != nil {
					_, _ = io.Copy(io.Discard, input.Body)
					if c, ok := input.Body.(io.Closer); ok {
						_ = c.Close()
					}
				}
				if tc.expectedPutObjectError != nil {
					return nil, tc.expectedPutObjectError
				}
				return nil, tc.expectedUploadError
			}).AnyTimes()

			s3clientRepository := NewS3ClientWithInterfaces(s3Client, s3PresignClient, downloadClient, uploadClient)

			err := s3clientRepository.UploadFolder(context.Background(), tc.path)

			if tc.expectedError != nil {
				if !strings.Contains(err.Error(), tc.expectedError.Error()) {
					t.Fatalf("expected err %v, got: %v", tc.expectedError, err)
				}
			} else if tc.expectedPutObjectError != nil && !errors.Is(err, tc.expectedPutObjectError) {
				t.Fatalf("expected put object error %v, got: %v", tc.expectedPutObjectError, err)
			} else if tc.expectedHeadObjectError != nil && !errors.Is(err, tc.expectedHeadObjectError) {
				t.Fatalf("expected head object error %v, got: %v", tc.expectedHeadObjectError, err)
			}
		})
	}
}

func TestDownloadFolder(t *testing.T) {
	testCases := []struct {
		name                       string
		s3Folder                   string
		localDir                   string
		expectedGetObjectError     error
		expectedGetObjectResponse  *s3.GetObjectOutput
		expectedListObjectResponse *s3.ListObjectsV2Output
		expectedListObjectError    error
		expectedError              error
		expectedDownloadError      error
		skipOnNonWindows           bool
	}{
		{
			name:                   "success",
			s3Folder:               "./",
			localDir:               "",
			expectedGetObjectError: nil,
			expectedGetObjectResponse: &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader([]byte("file content"))),
				ETag: aws.String("etag"),
			},
			expectedError: nil,
			expectedListObjectResponse: &s3.ListObjectsV2Output{
				Contents: []types.Object{
					{Key: aws.String("file1.txt")},
					{Key: aws.String("/file2.txt")},
				},
			},
			expectedListObjectError: nil,
			expectedDownloadError:   nil,
		},
		{
			name:                       "list objects error",
			s3Folder:                   "./",
			localDir:                   "",
			expectedGetObjectError:     nil,
			expectedGetObjectResponse:  nil,
			expectedListObjectError:    errors.New("s3 error"),
			expectedListObjectResponse: nil,
			expectedError:              errors.New("s3 error"),
			expectedDownloadError:      nil,
		},
		{
			name:                   "success 2",
			s3Folder:               "./",
			localDir:               "./",
			expectedGetObjectError: nil,
			expectedGetObjectResponse: &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader([]byte("file content"))),
				ETag: aws.String("etag"),
			},
			expectedError: nil,
			expectedListObjectResponse: &s3.ListObjectsV2Output{
				Contents: []types.Object{
					{Key: aws.String("file1.txt")},
					{Key: aws.String("./file2.txt")},
				},
			},
			expectedListObjectError: nil,
			expectedDownloadError:   nil,
		},
		{
			name:                      "fail to relative",
			s3Folder:                  `C:\\folder`,
			localDir:                  "./",
			expectedGetObjectError:    nil,
			expectedGetObjectResponse: nil,
			expectedError:             errors.New(`Rel: can't make file1.txt relative to C:\\folder`),
			expectedListObjectResponse: &s3.ListObjectsV2Output{
				Contents: []types.Object{
					{Key: aws.String("file1.txt")},
					{Key: aws.String("./file2.txt")},
				},
			},
			expectedListObjectError: nil,
			expectedDownloadError:   nil,
			skipOnNonWindows:        true,
		},
		{
			name:                   "downlaod error",
			s3Folder:               "./",
			localDir:               "./",
			expectedGetObjectError: nil,
			expectedGetObjectResponse: &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader([]byte("file content"))),
				ETag: aws.String("etag"),
			},
			expectedError: nil,
			expectedListObjectResponse: &s3.ListObjectsV2Output{
				Contents: []types.Object{
					{Key: aws.String("file1.txt")},
					{Key: aws.String("./file2.txt")},
				},
			},
			expectedListObjectError: nil,
			expectedDownloadError:   errors.New("s3 download error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipOnNonWindows && runtime.GOOS != "windows" {
				t.Skip("filepath.Rel with Windows paths only fails on Windows")
			}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			s3PresignClient := NewMockPresignClientInterface(ctrl)
			s3Client := NewMockClientInterface(ctrl)
			downloadClient := NewMockDownloaderInterface(ctrl)
			uploadClient := NewMockUploaderInterface(ctrl)

			s3Client.EXPECT().
				ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(tc.expectedListObjectResponse, tc.expectedListObjectError).
				Times(1)

			s3Client.EXPECT().
				GetObject(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(tc.expectedGetObjectResponse, tc.expectedGetObjectError).
				AnyTimes()

			downloadClient.EXPECT().
				Download(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*manager.Downloader)) (int64, error) {
					n, _ := w.WriteAt([]byte("file content"), 0)
					return int64(n), tc.expectedDownloadError
				}).AnyTimes()

			s3clientRepository := NewS3ClientWithInterfaces(s3Client, s3PresignClient, downloadClient, uploadClient)

			err := s3clientRepository.DownloadFolder(context.Background(), tc.s3Folder, tc.localDir)

			if tc.expectedError != nil {
				if err == nil {
					t.Fatalf("expected err %v, got nil", tc.expectedError)
				}
				if !strings.Contains(err.Error(), tc.expectedError.Error()) {
					t.Fatalf("expected err %v, got: %v", tc.expectedError, err)
				}
			} else if tc.expectedListObjectError != nil && !errors.Is(err, tc.expectedListObjectError) {
				t.Fatalf("expected list object error %v, got: %v", tc.expectedListObjectError, err)
			} else if tc.expectedGetObjectError != nil && !errors.Is(err, tc.expectedGetObjectError) {
				t.Fatalf("expected get object error %v, got: %v", tc.expectedGetObjectError, err)
			} else if tc.expectedDownloadError != nil && !errors.Is(err, tc.expectedDownloadError) {
				t.Fatalf("expected download error %v, got: %v", tc.expectedDownloadError, err)
			}
		})
	}
}

func TestLoadCACerts_SingleFile(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(certFile, generateSelfSignedCertPEM(t), 0644); err != nil {
		t.Fatal(err)
	}

	pool, err := loadCACerts(certFile)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil cert pool")
	}
}

func TestLoadCACerts_Directory(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"ca1.crt", "ca2.crt"} {
		if err := os.WriteFile(filepath.Join(dir, name), generateSelfSignedCertPEM(t), 0644); err != nil {
			t.Fatal(err)
		}
	}

	pool, err := loadCACerts(dir)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil cert pool")
	}
}

func TestLoadCACerts_DirectoryWithKubernetesSymlinkDirectory(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(certFile, generateSelfSignedCertPEM(t), 0644); err != nil {
		t.Fatal(err)
	}
	// Simulate the Kubernetes ..data symlink which points to a directory
	symlinkTarget := t.TempDir()
	if err := os.Symlink(symlinkTarget, filepath.Join(dir, "..data")); err != nil {
		t.Skip("symlinks not supported on this platform")
	}

	pool, err := loadCACerts(dir)
	if err != nil {
		t.Fatalf("expected no error (symlink-to-dir should be skipped), got: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil cert pool")
	}
}

func TestLoadCACerts_DirectorySkipsSubdirectories(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), generateSelfSignedCertPEM(t), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0755); err != nil {
		t.Fatal(err)
	}

	pool, err := loadCACerts(dir)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil cert pool")
	}
}

func TestLoadCACerts_PathDoesNotExist(t *testing.T) {
	_, err := loadCACerts("/nonexistent/path/ca.crt")
	if err == nil {
		t.Fatal("expected error for nonexistent path, got nil")
	}
}

func TestLoadCACerts_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()

	pool, err := loadCACerts(dir)
	if err != nil {
		t.Fatalf("expected no error for empty dir, got: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil cert pool even when empty")
	}
}

func TestLoadCACerts_TrailingSlashStripped(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), generateSelfSignedCertPEM(t), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := loadCACerts(dir + "/")
	if err != nil {
		t.Fatalf("expected no error with trailing slash, got: %v", err)
	}
}

func TestDeletePrefix_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s3Client := NewMockClientInterface(ctrl)
	client := NewS3ClientWithInterfaces(s3Client, nil, nil, nil)

	s3Client.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("backups/20240101/file.tar")},
			},
			IsTruncated: aws.Bool(false),
		}, nil).
		Times(1)

	s3Client.EXPECT().
		DeleteObjects(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&s3.DeleteObjectsOutput{}, nil).
		Times(1)

	if err := client.DeletePrefix(context.Background(), "backups/20240101"); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestDeletePrefix_PaginatesUntilNotTruncated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s3Client := NewMockClientInterface(ctrl)
	client := NewS3ClientWithInterfaces(s3Client, nil, nil, nil)

	token := aws.String("next-token")
	gomock.InOrder(
		s3Client.EXPECT().
			ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(&s3.ListObjectsV2Output{
				Contents:              []types.Object{{Key: aws.String("prefix/a")}},
				IsTruncated:           aws.Bool(true),
				NextContinuationToken: token,
			}, nil),
		s3Client.EXPECT().
			ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(&s3.ListObjectsV2Output{
				Contents:    []types.Object{{Key: aws.String("prefix/b")}},
				IsTruncated: aws.Bool(false),
			}, nil),
	)

	s3Client.EXPECT().
		DeleteObjects(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&s3.DeleteObjectsOutput{}, nil).
		Times(2)

	if err := client.DeletePrefix(context.Background(), "prefix"); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestDeletePrefix_EmptyPrefixReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := NewS3ClientWithInterfaces(NewMockClientInterface(ctrl), nil, nil, nil)

	err := client.DeletePrefix(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty prefix, got nil")
	}
	if !strings.Contains(err.Error(), "prefix is empty") {
		t.Fatalf("expected 'prefix is empty' error, got: %v", err)
	}
}

func TestDeletePrefix_SlashOnlyPrefixReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := NewS3ClientWithInterfaces(NewMockClientInterface(ctrl), nil, nil, nil)

	err := client.DeletePrefix(context.Background(), "/")
	if err == nil {
		t.Fatal("expected error for slash-only prefix, got nil")
	}
}

func TestDeletePrefix_ListObjectsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s3Client := NewMockClientInterface(ctrl)
	client := NewS3ClientWithInterfaces(s3Client, nil, nil, nil)

	s3Client.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("connection reset")).
		Times(1)

	err := client.DeletePrefix(context.Background(), "backups/20240101")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("expected connection error, got: %v", err)
	}
}

func TestDeletePrefix_DeleteObjectsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s3Client := NewMockClientInterface(ctrl)
	client := NewS3ClientWithInterfaces(s3Client, nil, nil, nil)

	s3Client.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&s3.ListObjectsV2Output{
			Contents:    []types.Object{{Key: aws.String("backups/file")}},
			IsTruncated: aws.Bool(false),
		}, nil)

	s3Client.EXPECT().
		DeleteObjects(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("access denied"))

	err := client.DeletePrefix(context.Background(), "backups")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "access denied") {
		t.Fatalf("expected access denied error, got: %v", err)
	}
}

func TestDeletePrefix_EmptyListSkipsDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s3Client := NewMockClientInterface(ctrl)
	client := NewS3ClientWithInterfaces(s3Client, nil, nil, nil)

	s3Client.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&s3.ListObjectsV2Output{
			Contents:    []types.Object{},
			IsTruncated: aws.Bool(false),
		}, nil)

	// DeleteObjects must NOT be called when there are no objects
	if err := client.DeletePrefix(context.Background(), "empty-prefix"); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestUploadFolderWithPrefix_KeysArePrefixed(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "data.bin"), []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s3Client := NewMockClientInterface(ctrl)
	uploadClient := NewMockUploaderInterface(ctrl)
	client := NewS3ClientWithInterfaces(s3Client, nil, nil, uploadClient)

	var capturedKey string
	uploadClient.EXPECT().
		Upload(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			capturedKey = aws.ToString(input.Key)
			if input.Body != nil {
				_, _ = io.Copy(io.Discard, input.Body)
			}
			return &manager.UploadOutput{}, nil
		})

	s3Client.EXPECT().HeadObject(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&s3.HeadObjectOutput{}, nil).AnyTimes()

	if err := client.UploadFolderWithPrefix(context.Background(), dir, "my/prefix"); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if !strings.HasPrefix(capturedKey, "my/prefix/") {
		t.Fatalf("expected key to start with 'my/prefix/', got: %s", capturedKey)
	}
}

// generateSelfSignedCertPEM returns a minimal PEM-encoded self-signed certificate for testing.
func generateSelfSignedCertPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
