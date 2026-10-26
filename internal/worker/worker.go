package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"bird-zpi/downloader/internal/messaging"
	"bird-zpi/downloader/internal/trace"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	retryDelaySeconds  = int32(5)
	requestIDAttribute = "request-id"
)

type Client interface {
	ReceiveMessage(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(context.Context, *awssqs.DeleteMessageInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(context.Context, *awssqs.ChangeMessageVisibilityInput, ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
}

func Run(ctx context.Context, client Client, queueURL string, handle func(context.Context, messaging.Message) error) error {
	for ctx.Err() == nil {
		output, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:              aws.String(queueURL),
			MaxNumberOfMessages:   1,
			WaitTimeSeconds:       20,
			MessageAttributeNames: []string{requestIDAttribute},
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
			},
		})
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			return fmt.Errorf("receive message from queue %q: %w", queueURL, err)
		}
		if len(output.Messages) == 0 {
			continue
		}

		delivery := output.Messages[0]
		startedAt := time.Now()
		messageID := aws.ToString(delivery.MessageId)
		requestID := messageRequestID(delivery)
		receiveCount := messageReceiveCount(delivery)
		log.Printf(
			"event=message_received request_id=%q message_id=%q queue_url=%q receive_count=%d",
			requestID,
			messageID,
			queueURL,
			receiveCount,
		)

		handlerContext := trace.WithID(ctx, requestID)
		message := messaging.Message{ID: messageID, Body: []byte(aws.ToString(delivery.Body))}
		if err := handle(handlerContext, message); err != nil {
			log.Printf(
				"event=message_failed request_id=%q message_id=%q queue_url=%q receive_count=%d retry_delay_seconds=%d duration=%s error=%q",
				requestID,
				messageID,
				queueURL,
				receiveCount,
				retryDelaySeconds,
				time.Since(startedAt).Round(time.Millisecond),
				err,
			)
			if _, visibilityErr := client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
				QueueUrl:          aws.String(queueURL),
				ReceiptHandle:     delivery.ReceiptHandle,
				VisibilityTimeout: retryDelaySeconds,
			}); visibilityErr != nil {
				return fmt.Errorf("schedule message retry on queue %q: %w", queueURL, visibilityErr)
			}
			continue
		}

		if _, err := client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: delivery.ReceiptHandle,
		}); err != nil {
			return fmt.Errorf("delete message from queue %q: %w", queueURL, err)
		}
		log.Printf(
			"event=message_completed request_id=%q message_id=%q queue_url=%q receive_count=%d duration=%s",
			requestID,
			messageID,
			queueURL,
			receiveCount,
			time.Since(startedAt).Round(time.Millisecond),
		)
	}

	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func messageRequestID(message types.Message) string {
	if attribute, ok := message.MessageAttributes[requestIDAttribute]; ok {
		if requestID := aws.ToString(attribute.StringValue); requestID != "" {
			return requestID
		}
	}
	return trace.NewID()
}

func messageReceiveCount(message types.Message) int {
	count, err := strconv.Atoi(message.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if err != nil {
		return 1
	}
	return count
}
