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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
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

// TestListFiles_Paginates guards against a bug where ListFiles issued a single
// unpaginated ListObjectsV2 call. Since S3 truncates responses at MaxKeys
// (1000 by default) and returns keys in lexicographic order, and vault names
// are sortable timestamps, an unpaginated listing would silently drop the
// newest vaults once a prefix accumulated more than a page of objects --
// making just-created backups invisible to restore's vault lookup.
func TestListFiles_Paginates(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s3PresignClient := NewMockPresignClientInterface(ctrl)
	s3Client := NewMockClientInterface(ctrl)
	downloadClient := NewMockDownloaderInterface(ctrl)
	uploadClient := NewMockUploaderInterface(ctrl)

	firstPage := &s3.ListObjectsV2Output{
		Contents: []types.Object{
			{Key: aws.String("prefix/20260601T000000")},
		},
		IsTruncated:           aws.Bool(true),
		NextContinuationToken: aws.String("token-1"),
	}
	secondPage := &s3.ListObjectsV2Output{
		Contents: []types.Object{
			{Key: aws.String("prefix/20260728T113920")},
		},
		IsTruncated: aws.Bool(false),
	}

	gomock.InOrder(
		s3Client.EXPECT().
			ListObjectsV2(gomock.Any(), gomock.Cond(func(in *s3.ListObjectsV2Input) bool {
				return in.ContinuationToken == nil
			}), gomock.Any()).
			Return(firstPage, nil),
		s3Client.EXPECT().
			ListObjectsV2(gomock.Any(), gomock.Cond(func(in *s3.ListObjectsV2Input) bool {
				return in.ContinuationToken != nil && *in.ContinuationToken == "token-1"
			}), gomock.Any()).
			Return(secondPage, nil),
	)

	s3clientRepository := NewS3ClientWithInterfaces(s3Client, s3PresignClient, downloadClient, uploadClient)

	files, err := s3clientRepository.ListFiles(context.Background(), "prefix")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expected := []string{"prefix/20260601T000000", "prefix/20260728T113920"}
	if len(files) != len(expected) {
		t.Fatalf("expected %d files across both pages, got %d: %v", len(expected), len(files), files)
	}
	for i, f := range files {
		if f != expected[i] {
			t.Errorf("expected %s at index %d, got %s", expected[i], i, f)
		}
	}
}

// TestListCommonPrefixes verifies that directory-style listing asks S3 to
// group keys via Delimiter and reads CommonPrefixes rather than walking
// every object under the prefix.
func TestListCommonPrefixes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s3PresignClient := NewMockPresignClientInterface(ctrl)
	s3Client := NewMockClientInterface(ctrl)
	downloadClient := NewMockDownloaderInterface(ctrl)
	uploadClient := NewMockUploaderInterface(ctrl)

	s3Client.EXPECT().
		ListObjectsV2(gomock.Any(), gomock.Cond(func(in *s3.ListObjectsV2Input) bool {
			return in.Delimiter != nil && *in.Delimiter == "/" && *in.Prefix == "backup-storage/"
		}), gomock.Any()).
		Return(&s3.ListObjectsV2Output{
			CommonPrefixes: []types.CommonPrefix{
				{Prefix: aws.String("backup-storage/20260617T000000/")},
				{Prefix: aws.String("backup-storage/granular/")},
			},
			IsTruncated: aws.Bool(false),
		}, nil)

	s3clientRepository := NewS3ClientWithInterfaces(s3Client, s3PresignClient, downloadClient, uploadClient)

	prefixes, err := s3clientRepository.ListCommonPrefixes(context.Background(), "backup-storage")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expected := []string{"backup-storage/20260617T000000/", "backup-storage/granular/"}
	if len(prefixes) != len(expected) {
		t.Fatalf("expected %d prefixes, got %d: %v", len(expected), len(prefixes), prefixes)
	}
	for i, p := range prefixes {
		if p != expected[i] {
			t.Errorf("expected %s at index %d, got %s", expected[i], i, p)
		}
	}
}

// TestListCommonPrefixes_Paginates ensures a directory with more than a page
// of vault entries still returns all of them.
func TestListCommonPrefixes_Paginates(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s3PresignClient := NewMockPresignClientInterface(ctrl)
	s3Client := NewMockClientInterface(ctrl)
	downloadClient := NewMockDownloaderInterface(ctrl)
	uploadClient := NewMockUploaderInterface(ctrl)

	firstPage := &s3.ListObjectsV2Output{
		CommonPrefixes: []types.CommonPrefix{
			{Prefix: aws.String("backup-storage/granular/20260601T000000/")},
		},
		IsTruncated:           aws.Bool(true),
		NextContinuationToken: aws.String("token-1"),
	}
	secondPage := &s3.ListObjectsV2Output{
		CommonPrefixes: []types.CommonPrefix{
			{Prefix: aws.String("backup-storage/granular/20260728T113920/")},
		},
		IsTruncated: aws.Bool(false),
	}

	gomock.InOrder(
		s3Client.EXPECT().
			ListObjectsV2(gomock.Any(), gomock.Cond(func(in *s3.ListObjectsV2Input) bool {
				return in.ContinuationToken == nil && in.Delimiter != nil && *in.Delimiter == "/"
			}), gomock.Any()).
			Return(firstPage, nil),
		s3Client.EXPECT().
			ListObjectsV2(gomock.Any(), gomock.Cond(func(in *s3.ListObjectsV2Input) bool {
				return in.ContinuationToken != nil && *in.ContinuationToken == "token-1"
			}), gomock.Any()).
			Return(secondPage, nil),
	)

	s3clientRepository := NewS3ClientWithInterfaces(s3Client, s3PresignClient, downloadClient, uploadClient)

	prefixes, err := s3clientRepository.ListCommonPrefixes(context.Background(), "backup-storage/granular")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expected := []string{"backup-storage/granular/20260601T000000/", "backup-storage/granular/20260728T113920/"}
	if len(prefixes) != len(expected) {
		t.Fatalf("expected %d prefixes across both pages, got %d: %v", len(expected), len(prefixes), prefixes)
	}
	for i, p := range prefixes {
		if p != expected[i] {
			t.Errorf("expected %s at index %d, got %s", expected[i], i, p)
		}
	}
}

// TestPrefixExists_SingleCall guards against PrefixExists degrading into a
// full paginated listing: it must issue exactly one ListObjectsV2 call with
// MaxKeys: 1, regardless of how many objects actually live under the prefix.
func TestPrefixExists_SingleCall(t *testing.T) {
	testCases := []struct {
		name           string
		response       *s3.ListObjectsV2Output
		responseErr    error
		expectedExists bool
		expectErr      bool
	}{
		{
			name: "exists via contents",
			response: &s3.ListObjectsV2Output{
				Contents: []types.Object{{Key: aws.String("backup-storage/20260617T000000/.metrics")}},
			},
			expectedExists: true,
		},
		{
			name:           "does not exist",
			response:       &s3.ListObjectsV2Output{},
			expectedExists: false,
		},
		{
			name:        "s3 error",
			responseErr: errors.New("s3 error"),
			expectErr:   true,
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

			s3Client.EXPECT().
				ListObjectsV2(gomock.Any(), gomock.Cond(func(in *s3.ListObjectsV2Input) bool {
					return in.MaxKeys != nil && *in.MaxKeys == 1 && *in.Prefix == "backup-storage/20260617T000000/"
				}), gomock.Any()).
				Times(1).
				Return(tc.response, tc.responseErr)

			s3clientRepository := NewS3ClientWithInterfaces(s3Client, s3PresignClient, downloadClient, uploadClient)

			exists, err := s3clientRepository.PrefixExists(context.Background(), "backup-storage/20260617T000000")
			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			if exists != tc.expectedExists {
				t.Errorf("expected exists=%v, got %v", tc.expectedExists, exists)
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

// TestDownloadFolder_Paginates guards against the same unpaginated-listing bug
// fixed in ListFiles: DownloadFolder used to issue a single ListObjectsV2 call,
// so a backup with more objects than one S3 page (default cap 1000 keys) would
// silently restore incomplete instead of erroring or fetching every file.
func TestDownloadFolder_Paginates(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s3PresignClient := NewMockPresignClientInterface(ctrl)
	s3Client := NewMockClientInterface(ctrl)
	downloadClient := NewMockDownloaderInterface(ctrl)
	uploadClient := NewMockUploaderInterface(ctrl)

	firstPage := &s3.ListObjectsV2Output{
		Contents: []types.Object{
			{Key: aws.String("file1.txt")},
		},
		IsTruncated:           aws.Bool(true),
		NextContinuationToken: aws.String("token-1"),
	}
	secondPage := &s3.ListObjectsV2Output{
		Contents: []types.Object{
			{Key: aws.String("file2.txt")},
		},
		IsTruncated: aws.Bool(false),
	}

	gomock.InOrder(
		s3Client.EXPECT().
			ListObjectsV2(gomock.Any(), gomock.Cond(func(in *s3.ListObjectsV2Input) bool {
				return in.ContinuationToken == nil
			}), gomock.Any()).
			Return(firstPage, nil),
		s3Client.EXPECT().
			ListObjectsV2(gomock.Any(), gomock.Cond(func(in *s3.ListObjectsV2Input) bool {
				return in.ContinuationToken != nil && *in.ContinuationToken == "token-1"
			}), gomock.Any()).
			Return(secondPage, nil),
	)

	var downloadedKeys []string
	downloadClient.EXPECT().
		Download(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*manager.Downloader)) (int64, error) {
			downloadedKeys = append(downloadedKeys, aws.ToString(input.Key))
			n, _ := w.WriteAt([]byte("file content"), 0)
			return int64(n), nil
		}).Times(2)

	s3clientRepository := NewS3ClientWithInterfaces(s3Client, s3PresignClient, downloadClient, uploadClient)

	localDir := t.TempDir()
	if err := s3clientRepository.DownloadFolder(context.Background(), "./", localDir); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expected := []string{"file1.txt", "file2.txt"}
	if len(downloadedKeys) != len(expected) {
		t.Fatalf("expected %d files downloaded across both pages, got %d: %v", len(expected), len(downloadedKeys), downloadedKeys)
	}
	for i, k := range expected {
		if downloadedKeys[i] != k {
			t.Errorf("expected download of %s at index %d, got %s", k, i, downloadedKeys[i])
		}
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

func TestNewS3Retryer_MaxAttempts(t *testing.T) {
	retryer := newS3Retryer()

	if got := retryer.MaxAttempts(); got != s3MaxRetryAttempts {
		t.Fatalf("expected MaxAttempts %d, got: %d", s3MaxRetryAttempts, got)
	}
	if retryer.MaxAttempts() <= retry.DefaultMaxAttempts {
		t.Fatalf("expected configured MaxAttempts (%d) to exceed the SDK default (%d), "+
			"otherwise the extra retry headroom this client relies on is gone",
			retryer.MaxAttempts(), retry.DefaultMaxAttempts)
	}
}

func TestNewS3Retryer_RetriesGoAwayError(t *testing.T) {
	retryer := newS3Retryer()

	goAwayErr := fmt.Errorf("operation error S3: ListObjectsV2, https response error StatusCode: 200, "+
		"deserialization failed, failed to decode response body: %w",
		errors.New("http2: server sent GOAWAY and closed the connection; LastStreamID=199, ErrCode=NO_ERROR, debug=\"\""))

	if !retryer.IsErrorRetryable(goAwayErr) {
		t.Fatal("expected a GOAWAY-induced deserialization error to be retryable, but it was not -- " +
			"raising MaxAttempts alone does nothing if the error is never classified as retryable")
	}

	if retryer.IsErrorRetryable(errors.New("some unrelated permanent failure")) {
		t.Fatal("expected an unrelated error to fall through to the default (non-retryable) classification")
	}
}

func TestNewS3TransportOptions_IdleConnTimeout(t *testing.T) {
	tr := &http.Transport{}

	newS3TransportOptions(true, "", nil)(tr)

	if tr.IdleConnTimeout != idleConnTimeout {
		t.Fatalf("expected IdleConnTimeout %v, got: %v", idleConnTimeout, tr.IdleConnTimeout)
	}
}

func TestNewS3TransportOptions_InsecureSkipVerify(t *testing.T) {
	tr := &http.Transport{}

	newS3TransportOptions(false, "", nil)(tr)

	if tr.TLSClientConfig == nil || !tr.TLSClientConfig.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify to be set when sslVerify is false, got: %+v", tr.TLSClientConfig)
	}
	if tr.IdleConnTimeout != idleConnTimeout {
		t.Fatalf("expected IdleConnTimeout to still be set alongside TLS options, got: %v", tr.IdleConnTimeout)
	}
}

func TestNewS3TransportOptions_RootCAs(t *testing.T) {
	tr := &http.Transport{}
	pool := x509.NewCertPool()

	newS3TransportOptions(true, "/some/certs/path", pool)(tr)

	if tr.TLSClientConfig == nil || tr.TLSClientConfig.RootCAs != pool {
		t.Fatalf("expected RootCAs to be set to the provided pool, got: %+v", tr.TLSClientConfig)
	}
}

func TestNewS3TransportOptions_NoTLSOverridesWhenVerifiedWithoutCerts(t *testing.T) {
	tr := &http.Transport{}

	newS3TransportOptions(true, "", nil)(tr)

	if tr.TLSClientConfig != nil {
		t.Fatalf("expected no TLS overrides, got: %+v", tr.TLSClientConfig)
	}
}

// TestNewS3Client_RetriesTransientFailures exercises the real client built by
// NewS3Client (not the mocked ClientInterface used elsewhere in this file)
// against a test server that fails the first ListObjectsV2 call with a
// retryable 503/SlowDown error before succeeding. This is the closest local
// approximation of the production incident: a transient failure partway
// through an S3 call (there, an HTTP/2 GOAWAY during response deserialization)
// that should now be absorbed by the retryer instead of failing the request.
func TestNewS3Client_RetriesTransientFailures(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if atomic.AddInt32(&attempts, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Name>test-bucket</Name>
	<Prefix>prefix</Prefix>
	<KeyCount>1</KeyCount>
	<MaxKeys>1000</MaxKeys>
	<IsTruncated>false</IsTruncated>
	<Contents>
		<Key>prefix/file.txt</Key>
		<LastModified>2024-01-01T00:00:00.000Z</LastModified>
		<ETag>&quot;etag&quot;</ETag>
		<Size>123</Size>
		<StorageClass>STANDARD</StorageClass>
	</Contents>
</ListBucketResult>`))
	}))
	defer server.Close()

	repo, err := NewS3Client(context.Background(), server.URL, "id", "secret", "test-bucket", "us-east-1", true, "")
	if err != nil {
		t.Fatalf("NewS3Client returned error: %v", err)
	}

	files, err := repo.ListFiles(context.Background(), "prefix")
	if err != nil {
		t.Fatalf("expected ListFiles to succeed after a transient failure, got error: %v", err)
	}
	if len(files) != 1 || files[0] != "prefix/file.txt" {
		t.Fatalf("unexpected files: %v", files)
	}
	if got := atomic.LoadInt32(&attempts); got < 2 {
		t.Fatalf("expected the client to retry the transient failure, server only saw %d request(s)", got)
	}
}
