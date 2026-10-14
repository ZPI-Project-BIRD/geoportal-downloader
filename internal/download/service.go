package download

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"

	"bird-zpi/downloader/internal/database"
	"bird-zpi/downloader/internal/gugik"
	"bird-zpi/downloader/internal/processor"

	amqp "github.com/rabbitmq/amqp091-go"
)

type FeatureDatabase interface {
	InsertFeature(context.Context, database.Feature) error
}

type ObjectStorage interface {
	Upload(context.Context, string, io.Reader, int64, string) (int64, error)
}

type ProcessorPublisher interface {
	ProcessFeature(context.Context, processor.Job) error
}

type Service struct {
	client        *http.Client
	publisher     Publisher
	database      FeatureDatabase
	objectStorage ObjectStorage
	processor     ProcessorPublisher
}

func NewService(publisher Publisher) *Service {
	return &Service{
		client:    &http.Client{},
		publisher: publisher,
	}
}

func NewDownloadWorker(featureDatabase FeatureDatabase, objectStorage ObjectStorage, processorPublisher ProcessorPublisher) *Service {
	return &Service{
		client:        &http.Client{},
		database:      featureDatabase,
		objectStorage: objectStorage,
		processor:     processorPublisher,
	}
}

func (s *Service) CreateDownload(ctx context.Context, req CreateRequest) error {
	bbox := fmt.Sprintf("%.2f,%.2f,%.2f,%.2f,EPSG:2180", req.TopLeft.X, req.TopLeft.Y, req.BottomRight.X, req.BottomRight.Y)
	url := fmt.Sprintf("https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&TYPENAME=gugik:SkorowidzOrtofomapy2025&SRSNAME=EPSG:2180&BBOX=%s", bbox)

	res, err := retryableRequest(s.client, url)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	var collection gugik.FeatureCollection
	if err := xml.NewDecoder(res.Body).Decode(&collection); err != nil {
		return err
	}

	for _, feature := range collection.FeatureMembers {
		if err := s.publisher.DownloadFeature(ctx, feature); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) HandleDownloadFeature(ctx context.Context, delivery amqp.Delivery) error {
	var feature gugik.FeatureMember
	if err := json.Unmarshal(delivery.Body, &feature); err != nil {
		return fmt.Errorf("decode download feature job: %w", err)
	}

	res, err := retryableRequest(s.client, feature.OrthophotoIndex.DownloadURL)
	if err != nil {
		return fmt.Errorf("download feature %q: %w", feature.OrthophotoIndex.ID, err)
	}
	defer res.Body.Close()

	objectName := fmt.Sprintf("%s.tif", feature.OrthophotoIndex.ID)
	fileSize, err := s.objectStorage.Upload(
		ctx,
		objectName,
		res.Body,
		res.ContentLength,
		res.Header.Get("Content-Type"),
	)
	if err != nil {
		return fmt.Errorf("store downloaded feature %q: %w", feature.OrthophotoIndex.ID, err)
	}

	if err := s.database.InsertFeature(ctx, database.Feature{
		ID:           feature.OrthophotoIndex.ID,
		ColorSpace:   feature.OrthophotoIndex.Color,
		PixelSize:    feature.OrthophotoIndex.PixelSize,
		DatasetDate:  feature.OrthophotoIndex.DatasetDate.Time(),
		FileSize:     fileSize,
		DownloadedAt: time.Now().UTC(),
	}); err != nil {
		return err
	}
	if err := s.processor.ProcessFeature(ctx, processor.Job{
		FeatureID:  feature.OrthophotoIndex.ID,
		ObjectName: objectName,
	}); err != nil {
		return fmt.Errorf("publish process feature job: %w", err)
	}

	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("acknowledge download feature job: %w", err)
	}

	return nil
}

func retryableRequest(client *http.Client, url string) (resp *http.Response, err error) {
	var lastError error
	for retry := range 8 {
		if retry > 0 {
			time.Sleep(time.Duration(retry) * time.Second)
		}

		res, err := client.Get(url)
		if err != nil {
			lastError = err
			continue
		}

		if res.StatusCode >= 400 {
			_ = res.Body.Close()
			lastError = fmt.Errorf("download request failed with error code %d", res.StatusCode)
			continue
		}

		return res, nil
	}

	return nil, lastError
}
