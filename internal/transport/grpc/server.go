package grpc

import (
	"context"
	"errors"
	"log"
	"time"

	downloaderv1 "bird-zpi/downloader/gen/downloader/v1"
	"bird-zpi/downloader/internal"
	"bird-zpi/downloader/internal/download"
	"bird-zpi/downloader/internal/trace"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	downloaderv1.UnimplementedDownloaderServiceServer
	service *download.Service
}

func NewServer(featureDatabase download.FeatureDatabase, publisher download.RetryPublisher) *Server {
	return &Server{service: download.NewService(featureDatabase, publisher)}
}

func (s *Server) RetryFeature(ctx context.Context, request *downloaderv1.RetryFeatureRequest) (response *downloaderv1.RetryFeatureResponse, err error) {
	startedAt := time.Now()
	requestID := trace.NewID()
	ctx = trace.WithID(ctx, requestID)
	featureID := request.GetFeatureId()
	log.Printf("event=grpc_request_started method=RetryFeature request_id=%q feature_id=%q", requestID, featureID)
	defer func() {
		result := "succeeded"
		if err != nil {
			result = "failed"
		}
		log.Printf(
			"event=grpc_request_completed method=RetryFeature request_id=%q feature_id=%q status=%q duration=%s error=%q",
			requestID,
			featureID,
			result,
			time.Since(startedAt).Round(time.Millisecond),
			err,
		)
	}()

	if featureID == "" {
		return nil, status.Error(codes.InvalidArgument, "feature_id is required")
	}
	action, err := s.service.RetryFeature(ctx, featureID)
	if err != nil {
		if errors.Is(err, download.ErrFeatureNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	responseAction := downloaderv1.RetryFeatureResponse_ACTION_UNSPECIFIED
	switch action {
	case download.RetryDownloadQueued:
		responseAction = downloaderv1.RetryFeatureResponse_DOWNLOAD_QUEUED
	case download.RetryTilingQueued:
		responseAction = downloaderv1.RetryFeatureResponse_TILING_QUEUED
	case download.RetryAlreadyComplete:
		responseAction = downloaderv1.RetryFeatureResponse_ALREADY_COMPLETE
	}
	return &downloaderv1.RetryFeatureResponse{Action: responseAction}, nil
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
