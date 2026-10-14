package download

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"bird-zpi/downloader/internal/database"
	"bird-zpi/downloader/internal/gugik"
	"bird-zpi/downloader/internal/processor"
	"bird-zpi/downloader/internal/trace"

	amqp "github.com/rabbitmq/amqp091-go"
)

type FeatureStatusDatabase interface {
	FeatureStatus(context.Context, string) (database.FeatureStatus, error)
}

type FeatureDatabase interface {
	FeatureStatusDatabase
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

func NewService(featureDatabase FeatureDatabase, publisher RetryPublisher) *Service {
	return &Service{
		client:    &http.Client{},
		publisher: publisher,
		database:  featureDatabase,
		processor: publisher,
	}
}

func (s *Service) RetryFeature(ctx context.Context, featureID string) (RetryAction, error) {
	status, err := s.database.FeatureStatus(ctx, featureID)
	if err != nil {
		return "", err
	}
	if status.Tiled {
		return RetryAlreadyComplete, nil
	}

	layer, _, found := strings.Cut(featureID, ".")
	if !found || layer == "" {
		return "", fmt.Errorf("feature ID %q must include its layer prefix", featureID)
	}
	if status.Downloaded {
		if err := s.processor.ProcessFeature(ctx, processor.Job{
			FeatureID:  featureID,
			LayerID:    layer,
			ObjectName: fmt.Sprintf("%s.tif", featureID),
		}); err != nil {
			return "", fmt.Errorf("publish manual tiling retry: %w", err)
		}
		return RetryTilingQueued, nil
	}

	feature, err := s.findFeature(ctx, layer, featureID)
	if err != nil {
		return "", err
	}
	if err := s.publisher.DownloadFeature(ctx, feature); err != nil {
		return "", fmt.Errorf("publish manual download retry: %w", err)
	}
	return RetryDownloadQueued, nil
}

func (s *Service) findFeature(ctx context.Context, layer, featureID string) (gugik.FeatureMember, error) {
	query := url.Values{
		"SERVICE":   {"WFS"},
		"VERSION":   {"1.1.0"},
		"REQUEST":   {"GetFeature"},
		"TYPENAME":  {"gugik:" + layer},
		"FEATUREID": {featureID},
	}
	requestURL := "https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze?" + query.Encode()

	res, err := retryableRequest(s.client, requestURL)
	if err != nil {
		return gugik.FeatureMember{}, fmt.Errorf("look up feature %q: %w", featureID, err)
	}
	defer res.Body.Close()

	var collection gugik.FeatureCollection
	if err := xml.NewDecoder(res.Body).Decode(&collection); err != nil {
		return gugik.FeatureMember{}, fmt.Errorf("decode feature %q lookup: %w", featureID, err)
	}
	for _, feature := range collection.FeatureMembers {
		if feature.OrthophotoIndex.ID == featureID {
			return feature, nil
		}
	}
	return gugik.FeatureMember{}, fmt.Errorf("%w: %s", ErrFeatureNotFound, featureID)
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
	requestID := trace.ID(ctx)
	bbox := fmt.Sprintf("%.2f,%.2f,%.2f,%.2f,EPSG:2180", req.TopLeft.X, req.TopLeft.Y, req.BottomRight.X, req.BottomRight.Y)
	url := fmt.Sprintf("https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&TYPENAME=gugik:SkorowidzOrtofomapy2025&SRSNAME=EPSG:2180&BBOX=%s", bbox)

	startedAt := time.Now()
	log.Printf("event=feature_discovery_started request_id=%q layer=%q bbox=%q", requestID, req.Layer, bbox)
	res, err := retryableRequest(s.client, url)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	var collection gugik.FeatureCollection
	if err := xml.NewDecoder(res.Body).Decode(&collection); err != nil {
		return err
	}
	log.Printf(
		"event=feature_discovery_completed request_id=%q feature_count=%d duration=%s",
		requestID,
		len(collection.FeatureMembers),
		time.Since(startedAt).Round(time.Millisecond),
	)

	for _, feature := range collection.FeatureMembers {
		if err := s.publisher.DownloadFeature(ctx, feature); err != nil {
			return err
		}
	}
	log.Printf("event=download_jobs_queued request_id=%q job_count=%d", requestID, len(collection.FeatureMembers))

	return nil
}

func (s *Service) HandleDownloadFeature(ctx context.Context, delivery amqp.Delivery) error {
	startedAt := time.Now()
	requestID := trace.ID(ctx)
	var feature gugik.FeatureMember
	if err := json.Unmarshal(delivery.Body, &feature); err != nil {
		return fmt.Errorf("decode download feature job: %w", err)
	}
	featureID := feature.OrthophotoIndex.ID
	log.Printf("event=feature_download_started request_id=%q message_id=%q feature_id=%q", requestID, delivery.MessageId, featureID)
	status, err := s.database.FeatureStatus(ctx, featureID)
	if err != nil {
		return err
	}
	if status.Downloaded {
		objectName := fmt.Sprintf("%s.tif", featureID)
		if !status.Tiled {
			if err := s.processor.ProcessFeature(ctx, processor.Job{
				FeatureID:  featureID,
				LayerID:    "SkorowidzOrtofomapy2025",
				ObjectName: objectName,
			}); err != nil {
				return fmt.Errorf("publish process feature job for previously downloaded feature: %w", err)
			}
		}
		if err := delivery.Ack(false); err != nil {
			return fmt.Errorf("acknowledge skipped download feature job: %w", err)
		}
		log.Printf(
			"event=feature_download_skipped request_id=%q message_id=%q feature_id=%q tiled=%t duration=%s",
			requestID,
			delivery.MessageId,
			featureID,
			status.Tiled,
			time.Since(startedAt).Round(time.Millisecond),
		)
		return nil
	}

	featureRecord := database.Feature{
		ID:          featureID,
		ColorSpace:  feature.OrthophotoIndex.Color,
		PixelSize:   feature.OrthophotoIndex.PixelSize,
		DatasetDate: feature.OrthophotoIndex.DatasetDate.Time(),
	}
	res, err := retryableRequest(s.client, feature.OrthophotoIndex.DownloadURL)
	if err != nil {
		return fmt.Errorf("download feature %q: %w", featureID, err)
	}
	defer res.Body.Close()

	objectName := fmt.Sprintf("%s.tif", featureID)
	uploadStartedAt := time.Now()
	fileSize, err := s.objectStorage.Upload(
		ctx,
		objectName,
		res.Body,
		res.ContentLength,
		res.Header.Get("Content-Type"),
	)
	if err != nil {
		return fmt.Errorf("store downloaded feature %q: %w", featureID, err)
	}
	log.Printf(
		"event=feature_stored request_id=%q feature_id=%q object_name=%q bytes=%d duration=%s",
		requestID,
		featureID,
		objectName,
		fileSize,
		time.Since(uploadStartedAt).Round(time.Millisecond),
	)

	featureRecord.FileSize = fileSize
	featureRecord.DownloadedAt = time.Now().UTC()
	if err := s.database.InsertFeature(ctx, featureRecord); err != nil {
		return err
	}
	log.Printf("event=feature_recorded request_id=%q feature_id=%q", requestID, featureID)
	if err := s.processor.ProcessFeature(ctx, processor.Job{
		FeatureID:  featureID,
		LayerID:    "SkorowidzOrtofomapy2025",
		ObjectName: objectName,
	}); err != nil {
		return fmt.Errorf("publish process feature job: %w", err)
	}
	log.Printf("event=processor_job_queued request_id=%q feature_id=%q object_name=%q", requestID, featureID, objectName)

	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("acknowledge download feature job: %w", err)
	}
	log.Printf(
		"event=feature_download_completed request_id=%q message_id=%q feature_id=%q bytes=%d duration=%s",
		requestID,
		delivery.MessageId,
		featureID,
		fileSize,
		time.Since(startedAt).Round(time.Millisecond),
	)

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
