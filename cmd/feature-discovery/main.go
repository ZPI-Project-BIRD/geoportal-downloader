package main

import (
	"bird-zpi/downloader/internal/config"
	"bird-zpi/downloader/internal/database"
	"bird-zpi/downloader/internal/download"
	"bird-zpi/downloader/internal/gugik"
	"bird-zpi/downloader/internal/logging"
	sqs "bird-zpi/downloader/internal/messaging/sqs"
	"bird-zpi/downloader/internal/requests"
	"bird-zpi/downloader/internal/trace"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/google/uuid"
)

type DiscoverFeaturesPayload struct {
	ProjectID uuid.UUID
}

type configValues struct {
	DownloadQueueURL string `envconfig:"SQS_DOWNLOAD_QUEUE_URL" required:"true"`
	DatabaseURL      string `envconfig:"DATABASE_URL" required:"true"`
}

type handler struct {
	db        *database.Client
	publisher *sqs.Publisher
	http      *http.Client
}

func (h *handler) discoverFeatures(ctx context.Context, data json.RawMessage) error {
	startedAt := time.Now()
	ctx = trace.WithID(ctx, trace.NewID())

	var payload DiscoverFeaturesPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		slog.ErrorContext(ctx, "DiscoverFeaturesMalformed", "data", string(data), "error", err)
		return err
	}
	projectID := payload.ProjectID.String()
	slog.InfoContext(ctx, "DiscoverFeaturesStarted", "project_id", projectID)

	area, err := h.db.ProjectArea(ctx, projectID)
	if err != nil {
		slog.ErrorContext(ctx, "DiscoverFeaturesBadProject", "project_id", projectID, "error", err)
		return err
	}

	collection, err := h.fetch(ctx, area)
	if err != nil {
		slog.ErrorContext(ctx, "DiscoverFeaturesFailedFetching", "project_id", projectID, "error", err)
		return err
	}

	for _, feature := range collection.FeatureMembers {
		if err := h.createFeature(ctx, payload.ProjectID, &feature); err != nil {
			return err
		}
	}

	slog.InfoContext(ctx, "DiscoverFeaturesCompleted",
		"project_id", projectID,
		"feature_count", len(collection.FeatureMembers),
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)

	return nil
}

func (h *handler) fetch(ctx context.Context, area database.ProjectArea) (*gugik.FeatureCollection, error) {
	fetchStartedAt := time.Now()

	bbox := fmt.Sprintf("%.2f,%.2f,%.2f,%.2f,EPSG:2180", area.TopLeft.X, area.TopLeft.Y, area.BottomRight.X, area.BottomRight.Y)
	url := fmt.Sprintf("https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&TYPENAME=gugik:SkorowidzOrtofomapy2025&SRSNAME=EPSG:2180&BBOX=%s", bbox)

	res, err := requests.RetryableRequest(h.http, url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var collection gugik.FeatureCollection
	if err := xml.NewDecoder(res.Body).Decode(&collection); err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, "DiscoverFeaturesFetched",
		"feature_count", len(collection.FeatureMembers),
		"duration_ms", time.Since(fetchStartedAt).Milliseconds(),
	)

	return &collection, nil
}

func (h *handler) createFeature(ctx context.Context, projectID uuid.UUID, feature *gugik.FeatureMember) error {
	featureID := feature.OrthophotoIndex.ID

	_, err := h.db.InsertDiscoveredFeature(ctx, projectID.String(), database.DiscoveredFeature{
		ID:          featureID,
		SourceURL:   feature.OrthophotoIndex.DownloadURL,
		ColorSpace:  feature.OrthophotoIndex.Color,
		PixelSize:   feature.OrthophotoIndex.PixelSize,
		DatasetDate: feature.OrthophotoIndex.DatasetDate.Time(),
	})
	if err != nil {
		slog.ErrorContext(ctx, "DiscoverFeaturesFailedCreating", "feature_id", featureID, "error", err)
		return err
	}

	if err := h.publisher.DispatchDownloadFeature(ctx, download.Job{
		ProjectID: projectID.String(),
		Feature:   *feature,
	}); err != nil {
		slog.ErrorContext(ctx, "DiscoverFeaturesFailedPublishing", "feature_id", featureID, "error", err)
		return err
	}

	slog.DebugContext(ctx, "DiscoverFeaturesCreated", "project_id", projectID.String(), "feature_id", featureID)
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

	sqsClient, err := sqs.NewClient(ctx)
	if err != nil {
		logging.Fatal("SQS initialization failed", err)
	}

	h = &handler{
		db:        db,
		publisher: sqs.NewPublisher(sqsClient, sqs.Queues{Download: cfg.DownloadQueueURL}),
		http:      &http.Client{},
	}
}

func main() {
	lambda.Start(h.discoverFeatures)
}
