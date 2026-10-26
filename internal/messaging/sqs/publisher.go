package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"bird-zpi/downloader/internal/detection"
	"bird-zpi/downloader/internal/download"
	"bird-zpi/downloader/internal/processor"
	"bird-zpi/downloader/internal/trace"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const requestIDAttribute = "request-id"

type Sender interface {
	SendMessage(context.Context, *awssqs.SendMessageInput, ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

type Publisher struct {
	client Sender
	queues Queues
}

type Queues struct {
	Download  string
	Process   string
	Detection string
}

func NewPublisher(client Sender, queues Queues) *Publisher {
	return &Publisher{client: client, queues: queues}
}

func (p *Publisher) DispatchDownloadFeature(ctx context.Context, job download.Job) error {
	return p.publish(ctx, p.queues.Download, "download feature", job)
}

func (p *Publisher) DispatchTileFeature(ctx context.Context, job processor.Job) error {
	return p.publish(ctx, p.queues.Process, "process feature", job)
}

func (p *Publisher) DispatchPerformDetection(ctx context.Context, job detection.Job) error {
	return p.publish(ctx, p.queues.Detection, "detection", job)
}

func (p *Publisher) publish(ctx context.Context, queueURL, jobName string, job any) error {
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("encode %s job: %w", jobName, err)
	}

	startedAt := time.Now()
	requestID := trace.ID(ctx)
	if requestID == "" {
		requestID = trace.NewID()
	}
	output, err := p.client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			requestIDAttribute: {
				DataType:    aws.String("String"),
				StringValue: aws.String(requestID),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("publish message: %w", err)
	}

	log.Printf(
		"event=message_published request_id=%q message_id=%q queue_url=%q bytes=%d duration=%s",
		requestID,
		aws.ToString(output.MessageId),
		queueURL,
		len(body),
		time.Since(startedAt).Round(time.Millisecond),
	)
	return nil
}
