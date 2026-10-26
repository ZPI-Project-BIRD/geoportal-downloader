package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

func NewClient(ctx context.Context) (*awssqs.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	return awssqs.NewFromConfig(cfg), nil
}
