package grpc

import (
	"context"
	"testing"

	downloaderv1 "bird-zpi/downloader/gen/downloader/v1"
	"bird-zpi/downloader/internal/download"
)

type recordingDownloadService struct {
	request download.CreateRequest
}

func (s *recordingDownloadService) CreateDownload(_ context.Context, request download.CreateRequest) error {
	s.request = request
	return nil
}

func TestCreateDownloadMapsRequest(t *testing.T) {
	service := &recordingDownloadService{}
	server := NewServer(service)

	_, err := server.CreateDownload(context.Background(), &downloaderv1.CreateDownloadRequest{
		Layer:       "gugik:SkorowidzOrtofomapy2025",
		TopLeft:     &downloaderv1.Point{X: 500_000, Y: 500_000},
		BottomRight: &downloaderv1.Point{X: 500_100, Y: 500_100},
	})
	if err != nil {
		t.Fatalf("CreateDownload returned an error: %v", err)
	}

	want := download.CreateRequest{
		Layer:       "gugik:SkorowidzOrtofomapy2025",
		TopLeft:     download.Point{X: 500_000, Y: 500_000},
		BottomRight: download.Point{X: 500_100, Y: 500_100},
	}
	if service.request != want {
		t.Errorf("request = %#v, want %#v", service.request, want)
	}
}
