package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Client struct {
	s3       *s3.Client
	uploader *transfermanager.Client
	bucket   string
}

func NewClient(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	return s3.NewFromConfig(cfg), nil
}

func New(client *s3.Client, bucket string) *Client {
	return &Client{
		s3:       client,
		uploader: transfermanager.New(client),
		bucket:   bucket,
	}
}

func (c *Client) Upload(ctx context.Context, objectName string, reader io.Reader, _ int64, contentType string) (int64, error) {
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	output, err := c.uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(objectName),
		Body:        reader,
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return 0, fmt.Errorf("upload object %q to bucket %q: %w", objectName, c.bucket, err)
	}
	return aws.ToInt64(output.ContentLength), nil
}

func (c *Client) Download(ctx context.Context, objectName string, writer io.Writer) (int64, error) {
	object, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(objectName),
	})
	if err != nil {
		return 0, fmt.Errorf("get object %q from bucket %q: %w", objectName, c.bucket, err)
	}
	defer object.Body.Close()

	written, err := io.Copy(writer, object.Body)
	if err != nil {
		return written, fmt.Errorf("download object %q from bucket %q: %w", objectName, c.bucket, err)
	}
	return written, nil
}
