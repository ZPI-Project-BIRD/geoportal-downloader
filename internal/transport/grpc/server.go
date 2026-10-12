package grpc

import (
	"context"

	downloaderv1 "bird-zpi/downloader/gen/downloader/v1"
	"bird-zpi/downloader/internal/download"
)

type DownloadService interface {
	CreateDownload(context.Context, download.CreateRequest) error
}

type Server struct {
	downloaderv1.UnimplementedDownloaderServiceServer
	downloads DownloadService
}

func NewServer(downloads DownloadService) *Server {
	return &Server{downloads: downloads}
}

func (s *Server) CreateDownload(ctx context.Context, request *downloaderv1.CreateDownloadRequest) (*downloaderv1.CreateDownloadResponse, error) {
	if err := s.downloads.CreateDownload(ctx, download.CreateRequest{
		Layer: request.GetLayer(),
		TopLeft: download.Point{
			X: request.GetTopLeft().GetX(),
			Y: request.GetTopLeft().GetY(),
		},
		BottomRight: download.Point{
			X: request.GetBottomRight().GetX(),
			Y: request.GetBottomRight().GetY(),
		},
	}); err != nil {
		return nil, err
	}

	return &downloaderv1.CreateDownloadResponse{}, nil
}
