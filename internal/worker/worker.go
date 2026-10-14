package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"bird-zpi/downloader/internal/trace"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	retryDelay  = 5 * time.Second
	maxRetries  = int32(5)
	retryHeader = "x-retry-count"
)

func Run(ctx context.Context, rabbitMQURL, queue string, handle func(context.Context, amqp.Delivery) error) error {
	connection, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	defer channel.Close()

	if _, err := channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %q: %w", queue, err)
	}
	if err := channel.Qos(1, 0, false); err != nil {
		return fmt.Errorf("configure worker prefetch: %w", err)
	}

	deliveries, err := channel.ConsumeWithContext(ctx, queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume queue %q: %w", queue, err)
	}

	for delivery := range deliveries {
		startedAt := time.Now()
		if delivery.CorrelationId == "" {
			delivery.CorrelationId = trace.NewID()
		}
		if delivery.MessageId == "" {
			delivery.MessageId = trace.NewID()
		}
		requestID := delivery.CorrelationId
		retryCount := deliveryRetryCount(delivery)
		log.Printf(
			"event=message_received request_id=%q message_id=%q queue=%q retry=%d",
			requestID,
			delivery.MessageId,
			queue,
			retryCount,
		)
		handlerContext := trace.WithID(ctx, requestID)
		if err := handle(handlerContext, delivery); err != nil {
			if retryCount >= maxRetries {
				log.Printf(
					"event=message_discarded request_id=%q message_id=%q queue=%q retry=%d duration=%s error=%q",
					requestID,
					delivery.MessageId,
					queue,
					retryCount,
					time.Since(startedAt).Round(time.Millisecond),
					err,
				)
				if nackErr := delivery.Nack(false, false); nackErr != nil {
					return fmt.Errorf("reject message from queue %q: %w", queue, nackErr)
				}
				continue
			}

			nextRetry := retryCount + 1
			log.Printf(
				"event=message_retry_scheduled request_id=%q message_id=%q queue=%q retry=%d max_retries=%d delay=%s duration=%s error=%q",
				requestID,
				delivery.MessageId,
				queue,
				nextRetry,
				maxRetries,
				retryDelay,
				time.Since(startedAt).Round(time.Millisecond),
				err,
			)
			time.Sleep(retryDelay)

			if err := channel.PublishWithContext(ctx, "", queue, false, false, retryPublishing(delivery, nextRetry)); err != nil {
				return fmt.Errorf("republish message to queue %q for retry: %w", queue, err)
			}
			if err := delivery.Ack(false); err != nil {
				return fmt.Errorf("acknowledge retried message from queue %q: %w", queue, err)
			}
			continue
		}
		log.Printf(
			"event=message_completed request_id=%q message_id=%q queue=%q retry=%d duration=%s",
			requestID,
			delivery.MessageId,
			queue,
			retryCount,
			time.Since(startedAt).Round(time.Millisecond),
		)
	}

	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
}

func deliveryRetryCount(delivery amqp.Delivery) int32 {
	if count, ok := delivery.Headers[retryHeader].(int32); ok && count > 0 {
		return count
	}
	return 0
}

func retryPublishing(delivery amqp.Delivery, retryCount int32) amqp.Publishing {
	headers := make(amqp.Table, len(delivery.Headers)+1)
	for key, value := range delivery.Headers {
		headers[key] = value
	}
	headers[retryHeader] = retryCount

	return amqp.Publishing{
		Headers:         headers,
		ContentType:     delivery.ContentType,
		ContentEncoding: delivery.ContentEncoding,
		DeliveryMode:    delivery.DeliveryMode,
		Priority:        delivery.Priority,
		CorrelationId:   delivery.CorrelationId,
		ReplyTo:         delivery.ReplyTo,
		Expiration:      delivery.Expiration,
		MessageId:       delivery.MessageId,
		Timestamp:       delivery.Timestamp,
		Type:            delivery.Type,
		UserId:          delivery.UserId,
		AppId:           delivery.AppId,
		Body:            delivery.Body,
	}
}
