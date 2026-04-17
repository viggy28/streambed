package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ObjectStorage is the interface used by the iceberg catalog and writer to
// interact with an S3-compatible object store. It enables fault injection
// in tests by substituting a wrapper implementation.
type ObjectStorage interface {
	PutObject(ctx context.Context, key string, data []byte, contentType string) error
	GetObject(ctx context.Context, key string) ([]byte, error)
	HeadObject(ctx context.Context, key string) (bool, error)
	ListPrefix(ctx context.Context, prefix string) ([]string, error)
	DeleteObjects(ctx context.Context, keys []string) error
	Bucket() string
}

// Compile-time check that S3Client satisfies ObjectStorage.
var _ ObjectStorage = (*S3Client)(nil)

type S3Client struct {
	client *s3.Client
	bucket string
}

func NewS3Client(ctx context.Context, bucket, region, endpoint string) (*S3Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(region))

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
			// For MinIO / LocalStack, use static credentials if env vars are set
			o.Credentials = credentials.NewStaticCredentialsProvider(
				envOrDefault("AWS_ACCESS_KEY_ID", "minioadmin"),
				envOrDefault("AWS_SECRET_ACCESS_KEY", "minioadmin"),
				"",
			)
		})
	}

	client := s3.NewFromConfig(cfg, s3Opts...)
	return &S3Client{client: client, bucket: bucket}, nil
}

func (s *S3Client) PutObject(ctx context.Context, key string, data []byte, contentType string) error {
	return retryWithBackoff(ctx, func() error {
		input := &s3.PutObjectInput{
			Bucket:      aws.String(s.bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String(contentType),
		}
		_, err := s.client.PutObject(ctx, input)
		if err != nil {
			return fmt.Errorf("put s3://%s/%s: %w", s.bucket, key, err)
		}
		return nil
	})
}

func (s *S3Client) GetObject(ctx context.Context, key string) ([]byte, error) {
	var result []byte
	err := retryWithBackoff(ctx, func() error {
		output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return fmt.Errorf("get s3://%s/%s: %w", s.bucket, key, err)
		}
		defer output.Body.Close()
		result, err = io.ReadAll(output.Body)
		if err != nil {
			return fmt.Errorf("read body s3://%s/%s: %w", s.bucket, key, err)
		}
		return nil
	})
	return result, err
}

func (s *S3Client) HeadObject(ctx context.Context, key string) (bool, error) {
	var exists bool
	err := retryWithBackoff(ctx, func() error {
		_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			var notFound *types.NotFound
			if errors.As(err, &notFound) {
				exists = false
				return nil
			}
			var noSuchKey *types.NoSuchKey
			if errors.As(err, &noSuchKey) {
				exists = false
				return nil
			}
			return fmt.Errorf("head s3://%s/%s: %w", s.bucket, key, err)
		}
		exists = true
		return nil
	})
	return exists, err
}

// ListPrefix returns all object keys under the given prefix.
// Used by the table catalog to discover Iceberg tables on S3.
func (s *S3Client) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list s3://%s/%s: %w", s.bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys, nil
}

// Bucket returns the bucket name. Used by the query server for DuckDB S3 config.
func (s *S3Client) Bucket() string {
	return s.bucket
}

// DeleteObjects removes the given object keys from the bucket.
// Keys are deleted in batches of 1000 (S3's DeleteObjects limit).
func (s *S3Client) DeleteObjects(ctx context.Context, keys []string) error {
	const batchSize = 1000
	for i := 0; i < len(keys); i += batchSize {
		end := min(i+batchSize, len(keys))
		objects := make([]types.ObjectIdentifier, end-i)
		for j, k := range keys[i:end] {
			objects[j] = types.ObjectIdentifier{Key: aws.String(k)}
		}
		_, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("delete objects (batch starting %d): %w", i, err)
		}
	}
	return nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
