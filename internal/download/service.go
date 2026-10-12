package download

import (
	"context"
	"net/http"
)

type Service struct {
	client *http.Client
}

func NewService() *Service {
	return &Service{
		client: &http.Client{},
	}
}

func (s *Service) CreateDownload(ctx context.Context, request CreateRequest) error {
	// Download processing will be implemented here. This method is invoked by
	// the worker after a request has been read from RabbitMQ.
	return nil
}
