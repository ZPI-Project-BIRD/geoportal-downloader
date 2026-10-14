package grpc

import (
	"context"
	"log"
	"time"

	downloaderv1 "bird-zpi/downloader/gen/downloader/v1"
	"bird-zpi/downloader/internal"
	"bird-zpi/downloader/internal/download"
	"bird-zpi/downloader/internal/trace"
)

type Server struct {
	downloaderv1.UnimplementedDownloaderServiceServer
	service *download.Service
}

func NewServer(publisher download.Publisher) *Server {
	return &Server{service: download.NewService(publisher)}
}

func (s *Server) CreateDownload(ctx context.Context, request *downloaderv1.CreateDownloadRequest) (response *downloaderv1.CreateDownloadResponse, err error) {
	startedAt := time.Now()
	requestID := trace.NewID()
	ctx = trace.WithID(ctx, requestID)
	log.Printf(
		"event=grpc_request_started method=CreateDownload request_id=%q layer=%q top_left_x=%f top_left_y=%f bottom_right_x=%f bottom_right_y=%f",
		requestID,
		request.GetLayer(),
		request.GetTopLeft().GetX(),
		request.GetTopLeft().GetY(),
		request.GetBottomRight().GetX(),
		request.GetBottomRight().GetY(),
	)
	defer func() {
		status := "succeeded"
		if err != nil {
			status = "failed"
		}
		log.Printf(
			"event=grpc_request_completed method=CreateDownload request_id=%q status=%q duration=%s error=%q",
			requestID,
			status,
			time.Since(startedAt).Round(time.Millisecond),
			err,
		)
	}()

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
