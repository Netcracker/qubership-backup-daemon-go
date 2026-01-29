package controller

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"golang.org/x/sync/errgroup"
)

const WorkerCount = 3

type S3ClientRepository interface {
	CreatePresignedUrl(ctx context.Context, objectName string, expiration int) (string, error)
	ListFiles(ctx context.Context, path string) ([]string, error)
	UploadFolder(ctx context.Context, path string) error
	UploadFolderWithPrefix(ctx context.Context, path, prefix string) error
	DownloadFolder(ctx context.Context, s3Folder string, localDir string) error
	DeletePrefix(ctx context.Context, prefix string) error
}

//go:generate mockgen -source=s3client.go -destination=s3mock.go -package=controller
type PresignClientInterface interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

type DownloaderInterface interface {
	Download(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, options ...func(*manager.Downloader)) (n int64, err error)
}

type UploaderInterface interface {
	Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

type ClientInterface interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

type S3Client struct {
	url             string
	accessKeyID     string
	accessKeySecret string
	bucketName      string
	region          string
	Client          ClientInterface
	PresignClient   PresignClientInterface
	Uploader        UploaderInterface
	Downloader      DownloaderInterface
}

func NewS3Client(ctx context.Context, url string, accessKeyID string, accessKeySecret string, bucketName string, region string, sslVerify bool) (S3ClientRepository, error) {
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		if !sslVerify {
			if tr.TLSClientConfig == nil {
				tr.TLSClientConfig = &tls.Config{}
			}
			tr.TLSClientConfig.InsecureSkipVerify = true
		}
	})

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithHTTPClient(httpClient),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, accessKeySecret, "")),
	)
	if err != nil {
		return nil, err
	}
	realClient := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(url)
		o.UsePathStyle = true
		// Disable request payload checksum computation for S3-compatible storage
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	presignClient := s3.NewPresignClient(realClient)
	return &S3Client{
		PresignClient: presignClient,
		Client:        realClient,
		Downloader: manager.NewDownloader(realClient, func(d *manager.Downloader) {
			d.PartSize = 64 * 1024 * 1024
		}),
		Uploader: manager.NewUploader(realClient, func(d *manager.Uploader) {
			d.PartSize = 64 * 1024 * 1024
		}),
		url:             url,
		accessKeyID:     accessKeyID,
		accessKeySecret: accessKeySecret,
		bucketName:      bucketName,
		region:          region,
	}, nil
}

func (s *S3Client) CreatePresignedUrl(ctx context.Context, objectName string, expiration int) (string, error) {
	if expiration == 0 {
		expiration = 3600
	}
	resp, err := s.PresignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectName),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = time.Duration(expiration * int(time.Second))
	})
	if err != nil {
		return "", fmt.Errorf("failed to create presigned url: %w", err)
	}
	return resp.URL, nil
}

func (s *S3Client) ListFiles(ctx context.Context, path string) ([]string, error) {
	path = strings.Trim(path, "/")
	var files []string
	objects, err := s.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucketName),
		Prefix: aws.String(path),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}
	for _, object := range objects.Contents {
		files = append(files, *object.Key)
	}
	return files, nil
}

func (s *S3Client) uploadFolderInternal(parent context.Context, localDir string, prefix string) error {
	g, ctx := errgroup.WithContext(parent)
	jobs := make(chan string)

	base := filepath.Clean(localDir)
	prefix = strings.Trim(prefix, "/")

	g.Go(func() error {
		defer close(jobs)
		return filepath.WalkDir(base, func(filePath string, d fs.DirEntry, err error) error {
			if err != nil {
				return fmt.Errorf("failed to walk path %s: %w", base, err)
			}
			if !d.IsDir() {
				select {
				case jobs <- filePath:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	})

	for i := 0; i < WorkerCount; i++ {
		g.Go(func() error {
			return s.workerUpload(ctx, jobs, base, prefix)
		})
	}
	return g.Wait()
}

func (s *S3Client) UploadFolder(ctx context.Context, localDir string) error {
	return s.uploadFolderInternal(ctx, localDir, "")
}

func (s *S3Client) UploadFolderWithPrefix(ctx context.Context, localDir, prefix string) error {
	return s.uploadFolderInternal(ctx, localDir, prefix)
}

func (s *S3Client) DownloadFolder(ctx context.Context, s3Folder string, localDir string) error {
	s3Folder = strings.Trim(s3Folder, "/")
	objects, err := s.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucketName),
		Prefix: aws.String(s3Folder),
	})
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}

	for _, object := range objects.Contents {
		key := aws.ToString(object.Key)
		if strings.HasPrefix(key, "/") {
			continue
		}

		var target string
		if len(localDir) == 0 {
			target = filepath.Join("/", key)
		} else {
			relPath, err := filepath.Rel(s3Folder, key)
			if err != nil {
				return fmt.Errorf("failed to get relative path: %w", err)
			}
			target = filepath.Join(localDir, relPath)
		}
		if err := os.MkdirAll(filepath.Dir(target), os.ModePerm); err != nil {
			return fmt.Errorf("failed to create dir for %s: %v", target, err)
		}
		if err := s.downloadFile(ctx, key, target); err != nil {
			return fmt.Errorf("failed to download file: %w", err)
		}
	}

	return nil
}

func (s *S3Client) uploadFile(ctx context.Context, src string, dest string) error {
	dest = strings.Trim(dest, "/")
	r, w := io.Pipe()

	go func() {
		defer func() {
			_ = w.Close()
		}()
		file, err := os.Open(src)
		if err != nil {
			_ = w.CloseWithError(fmt.Errorf("failed to open file %s: %w", src, err))
			return
		}
		defer func() {
			_ = file.Close()
		}()

		// TODO change to CopyByffer?
		_, err = io.Copy(w, file)
		if err != nil {
			_ = w.CloseWithError(fmt.Errorf("failed to copy file %s: %w", src, err))
		}
	}()

	_, err := s.Uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(dest),
		Body:   r,
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "EntityTooLarge" {
			return fmt.Errorf("error while uploading object to %s. The object is too large.\n"+
			"The maximum size for a multipart upload is 5TB", s.bucketName)
		}
		return fmt.Errorf("couldn't upload large object to %v:%v. Here's why: %w", s.bucketName, dest, err)
	}
	err = s3.NewObjectExistsWaiter(s.Client).Wait(
		ctx,
		&s3.HeadObjectInput{
			Bucket: aws.String(s.bucketName),
			Key:    aws.String(dest),
		},
		time.Minute)
	if err != nil {
		return fmt.Errorf("failed attempt to wait for object %s to exist err: %w", dest, err)
	}
	return nil
}

func (s *S3Client) downloadFile(ctx context.Context, src string, dest string) error {
	file, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", dest, err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close file %s: %w", dest, cerr)
		}
	}()

	_, err = s.Downloader.Download(ctx, file, &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(src),
	})
	if err != nil {
		return fmt.Errorf("couldn't download large object from %v:%v. Here's why: %w",
			s.bucketName, src, err)
	}
	return nil
}

func (s *S3Client) workerUpload(ctx context.Context, jobs <-chan string, baseDir string, prefix string) error {
	for file := range jobs {
		var key string

		if prefix == "" {
			key = strings.TrimLeft(filepath.ToSlash(file), "/")
		} else {
			rel, err := filepath.Rel(baseDir, file)
			if err != nil {
				return err
			}
			key = path.Join(prefix, filepath.ToSlash(rel))
		}

		if err := s.uploadFile(ctx, file, key); err != nil {
			return err
		}
	}
	return nil
}

func withContentMD5(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		_, _ = stack.Initialize.Remove("AWSChecksum:SetupInputContext")
		_, _ = stack.Build.Remove("AWSChecksum:RequestMetricsTracking")
		_, _ = stack.Finalize.Remove("AWSChecksum:ComputeInputPayloadChecksum")
		_, _ = stack.Finalize.Remove("addInputChecksumTrailer")

		return smithyhttp.AddContentChecksumMiddleware(stack)
	})
}

func (s *S3Client) DeletePrefix(ctx context.Context, prefix string) error {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return fmt.Errorf("prefix is empty")
	}

	var cont *string
	for {
		out, err := s.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucketName),
			Prefix:            aws.String(prefix),
			ContinuationToken: cont,
		})
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
		}

		if len(out.Contents) > 0 {
			objs := make([]types.ObjectIdentifier, 0, len(out.Contents))
			for _, o := range out.Contents {
				if o.Key == nil {
					continue
				}
				objs = append(objs, types.ObjectIdentifier{Key: o.Key})
			}

			_, err = s.Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(s.bucketName),
				Delete: &types.Delete{Objects: objs, Quiet: aws.Bool(true)},
			}, withContentMD5)
			if err != nil {
				return fmt.Errorf("delete objects: %w", err)
			}
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		cont = out.NextContinuationToken
	}
	return nil
}
