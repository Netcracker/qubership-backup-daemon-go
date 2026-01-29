package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"go.uber.org/mock/gomock"
)

func NewS3ClientWithInterfaces(client ClientInterface, presignClient PresignClientInterface,
	downloader DownloaderInterface, uploader UploaderInterface) *S3Client {
	return &S3Client{
		Client:        client,
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
			path:                    "../repo/granular",
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
