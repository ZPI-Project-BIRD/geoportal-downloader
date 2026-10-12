package grpc

import (
	"context"

	downloaderv1 "bird-zpi/downloader/gen/downloader/v1"
	"bird-zpi/downloader/internal"
	"bird-zpi/downloader/internal/download"
)

type Server struct {
	downloaderv1.UnimplementedDownloaderServiceServer
	service *download.Service
}

func NewServer(publisher download.Publisher) *Server {
	return &Server{service: download.NewService(publisher)}
}

func (s *Server) CreateDownload(ctx context.Context, request *downloaderv1.CreateDownloadRequest) (*downloaderv1.CreateDownloadResponse, error) {
	topLeftX, topLeftY, err := internal.Convert4326to2180(request.GetTopLeft().GetX(), request.GetTopLeft().GetY())
	if err != nil {
		return nil, err
	}

	bottomRightX, bottomRightY, err := internal.Convert4326to2180(request.GetBottomRight().GetX(), request.GetBottomRight().GetY())
	if err != nil {
		return nil, err
	}

	if err := s.service.CreateDownload(ctx, download.CreateRequest{
		Layer: request.GetLayer(),
		TopLeft: download.Point{
			X: topLeftX,
			Y: topLeftY,
		},
		BottomRight: download.Point{
			X: bottomRightX,
			Y: bottomRightY,
		},
	}); err != nil {
		return nil, err
	}

	return &downloaderv1.CreateDownloadResponse{}, nil
}
