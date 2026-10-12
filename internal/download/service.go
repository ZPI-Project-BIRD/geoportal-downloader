package download

import (
	"context"
	"fmt"
)

type Point struct {
	X float64
	Y float64
}

type CreateRequest struct {
	Layer       string
	TopLeft     Point
	BottomRight Point
}

type Service struct{}

func NewService() *Service {
	return &Service{}
}

func (s *Service) CreateDownload(ctx context.Context, request CreateRequest) error {
	fmt.Println(request)
	return nil
}
