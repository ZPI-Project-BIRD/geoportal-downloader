package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"bird-zpi/downloader/internal/download"
	"bird-zpi/downloader/internal/gugik"
	"bird-zpi/downloader/internal/processor"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	connection *amqp.Connection
}

func NewPublisher(url string) (*Publisher, error) {
	connection, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	return &Publisher{connection: connection}, nil
}

func (p *Publisher) DownloadFeature(ctx context.Context, feature gugik.FeatureMember) error {
	body, err := json.Marshal(feature)
	if err != nil {
		return fmt.Errorf("encode download feature job: %w", err)
	}
	return p.publish(ctx, download.DownloadFeatureQueue, body)
}

func (p *Publisher) ProcessFeature(ctx context.Context, job processor.Job) error {
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("encode process feature job: %w", err)
	}
	return p.publish(ctx, processor.Queue, body)
}

func (p *Publisher) publish(ctx context.Context, queue string, body []byte) error {
	channel, err := p.connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	defer channel.Close()

	if _, err := channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %q: %w", queue, err)
	}
	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable publisher confirms for queue %q: %w", queue, err)
	}

	confirmation, err := channel.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("publish message to queue %q: %w", queue, err)
	}
	confirmed, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait for message confirmation from queue %q: %w", queue, err)
	}
	if !confirmed {
		return fmt.Errorf("message to queue %q was rejected by RabbitMQ", queue)
	}

	return nil
}

func (p *Publisher) Close() error {
	return p.connection.Close()
}
