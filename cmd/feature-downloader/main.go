package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"bird-zpi/downloader/internal/config"
	"bird-zpi/downloader/internal/database"
	"bird-zpi/downloader/internal/gugik"
	"bird-zpi/downloader/internal/logging"
	sqs "bird-zpi/downloader/internal/messaging/sqs"
	"bird-zpi/downloader/internal/requests"
	"bird-zpi/downloader/internal/storage"
	"bird-zpi/downloader/internal/trace"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

type DownloadFeaturePayload struct {
	ProjectID string              `json:"projectId"`
	Feature   gugik.FeatureMember `json:"feature"`
}

type configValues struct {
	ProcessQueueURL   string `envconfig:"SQS_PROCESS_QUEUE_URL" required:"true"`
	DetectionQueueURL string `envconfig:"SQS_DETECTION_QUEUE_URL" required:"true"`
	DatabaseURL       string `envconfig:"DATABASE_URL" required:"true"`
	SourceBucket      string `envconfig:"S3_SOURCE_BUCKET" default:"features"`
}

type handler struct {
	db        *database.Client
	storage   *storage.Client
	publisher *sqs.Publisher
	http      *http.Client
}

func (h *handler) downloadFeature(ctx context.Context, ev events.SQSEvent) (events.SQSEventResponse, error) {
	var resp events.SQSEventResponse
	for _, rec := range ev.Records {
		ctx := trace.WithID(ctx, rec.MessageId)
		if err := h.process(ctx, rec.Body); err != nil {
			slog.ErrorContext(ctx, "download feature failed", "message_id", rec.MessageId, "error", err)

			resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{
				ItemIdentifier: rec.MessageId,
			})
		}
	}
	return resp, nil
}

func (h *handler) process(ctx context.Context, body string) error {
	var p DownloadFeaturePayload
	if err := json.Unmarshal([]byte(body), &p); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}

	featureID := p.Feature.OrthophotoIndex.ID
	objectName := featureID + ".tif"
	started := time.Now()
	slog.InfoContext(ctx, "download started", "project_id", p.ProjectID, "feature_id", featureID)

	downloaded, err := h.db.IsFeatureDownloaded(ctx, featureID)
	if err != nil {
		return fmt.Errorf("check feature %q: %w", featureID, err)
	}
	if downloaded {
		goto detect
	}

	if err := h.download(ctx, p, objectName); err != nil {
		return err
	}

detect:
	if err := h.db.CreateDetectionJob(ctx, p.ProjectID, featureID); err != nil {
		return fmt.Errorf("failed creating a detection job: %w", err)
	}

	// Tiler is not needed for now, since Geoportal already provides tiled maps (WTMS).
	// if err := h.publisher.DispatchTileFeature(ctx, processor.Job{
	// 	ProjectID:  p.ProjectID,
	// 	FeatureID:  featureID,
	// 	ObjectName: objectName,
	// }); err != nil {
	// 	return fmt.Errorf("publish tiling job: %w", err)
	// }

	slog.InfoContext(ctx, "download completed",
		"project_id", p.ProjectID, "feature_id", featureID,
		"skipped_download", downloaded, "duration", time.Since(started).Round(time.Millisecond))

	return nil
}

func (h *handler) download(ctx context.Context, p DownloadFeaturePayload, objectName string) error {
	f := p.Feature.OrthophotoIndex

	res, err := requests.RetryableRequest(h.http, f.DownloadURL)
	if err != nil {
		return fmt.Errorf("download feature %q: %w", f.ID, err)
	}
	defer res.Body.Close()

	size, err := h.storage.Upload(ctx, objectName, res.Body, res.ContentLength, res.Header.Get("Content-Type"))
	if err != nil {
		return fmt.Errorf("store feature %q: %w", f.ID, err)
	}

	if err := h.db.UpsertDownloadedFeature(ctx, database.Feature{
		ID:           f.ID,
		SourceURL:    f.DownloadURL,
		ColorSpace:   f.Color,
		PixelSize:    f.PixelSize,
		DatasetDate:  f.DatasetDate.Time(),
		ObjectName:   objectName,
		FileSize:     size,
		DownloadedAt: time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("record feature %q: %w", f.ID, err)
	}

	slog.InfoContext(ctx, "feature stored", "feature_id", f.ID, "object_name", objectName, "bytes", size)
	return nil
}

var h *handler

func init() {
	logging.Setup()

	var cfg configValues
	if err := config.Load(&cfg); err != nil {
		logging.Fatal("configuration failed", err)
	}

	ctx := context.Background()

	db, err := database.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logging.Fatal("database initialization failed", err)
	}

	s3Client, err := storage.NewClient(ctx)
	if err != nil {
		logging.Fatal("S3 initialization failed", err)
	}

	sqsClient, err := sqs.NewClient(ctx)
	if err != nil {
		logging.Fatal("SQS initialization failed", err)
	}

	h = &handler{
		db:      db,
		storage: storage.New(s3Client, cfg.SourceBucket),
		publisher: sqs.NewPublisher(sqsClient, sqs.Queues{
			Process:   cfg.ProcessQueueURL,
			Detection: cfg.DetectionQueueURL,
		}),
		http: &http.Client{},
	}
}

func main() {
	lambda.Start(h.downloadFeature)
}
