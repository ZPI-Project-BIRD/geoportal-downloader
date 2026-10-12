package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"bird-zpi/downloader/internal/download"
	"bird-zpi/downloader/internal/gugik"

	amqp "github.com/rabbitmq/amqp091-go"
)

const DefaultURL = "amqp://app:app@localhost:5672/"

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

func (p *Publisher) publish(ctx context.Context, queue string, body []byte) error {
	channel, err := p.connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	defer channel.Close()

	if _, err := channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %q: %w", queue, err)
	}

	if err := channel.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}); err != nil {
		return fmt.Errorf("publish message to queue %q: %w", queue, err)
	}

	return nil
}

func (p *Publisher) Close() error {
	return p.connection.Close()
}
