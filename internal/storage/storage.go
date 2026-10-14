package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Client struct {
	client *minio.Client
	bucket string
}

func New(ctx context.Context, endpoint, accessKey, secretKey, bucket string, secure bool) (*Client, error) {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, fmt.Errorf("create object storage client: %w", err)
	}

	client := &Client{client: minioClient, bucket: bucket}
	exists, err := minioClient.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("check object storage bucket %q: %w", bucket, err)
	}
	if !exists {
		if err := minioClient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("create object storage bucket %q: %w", bucket, err)
		}
	}

	return client, nil
}

func (c *Client) Upload(ctx context.Context, objectName string, reader io.Reader, size int64, contentType string) (int64, error) {
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	info, err := c.client.PutObject(ctx, c.bucket, objectName, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return 0, fmt.Errorf("upload object %q to bucket %q: %w", objectName, c.bucket, err)
	}
	return info.Size, nil
}

func (c *Client) Download(ctx context.Context, objectName string, writer io.Writer) (int64, error) {
	object, err := c.client.GetObject(ctx, c.bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("open object %q from bucket %q: %w", objectName, c.bucket, err)
	}
	defer object.Close()

	if _, err := object.Stat(); err != nil {
		return 0, fmt.Errorf("stat object %q from bucket %q: %w", objectName, c.bucket, err)
	}

	written, err := io.Copy(writer, object)
	if err != nil {
		return written, fmt.Errorf("download object %q from bucket %q: %w", objectName, c.bucket, err)
	}
	return written, nil
}
