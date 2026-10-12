package worker

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Run(ctx context.Context, rabbitMQURL, queue string, handle func(context.Context, []byte) error) error {
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
		if err := handle(ctx, delivery.Body); err != nil {
			if nackErr := delivery.Nack(false, true); nackErr != nil {
				return fmt.Errorf("requeue message from queue %q: %w", queue, nackErr)
			}
			continue
		}

		if err := delivery.Ack(false); err != nil {
			return fmt.Errorf("acknowledge message from queue %q: %w", queue, err)
		}
	}

	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
