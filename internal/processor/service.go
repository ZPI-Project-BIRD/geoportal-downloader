package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"bird-zpi/downloader/internal/database"
	"bird-zpi/downloader/internal/messaging"
	"bird-zpi/downloader/internal/trace"
)

type ObjectStorage interface {
	Download(context.Context, string, io.Writer) (int64, error)
	Upload(context.Context, string, io.Reader, int64, string) (int64, error)
}

type FeatureDatabase interface {
	FeatureStatus(context.Context, string) (database.FeatureStatus, error)
	MarkFeatureTiled(context.Context, string, int) error
}

type Service struct {
	database      FeatureDatabase
	sourceStorage ObjectStorage
	tileStorage   ObjectStorage
	tiler         Tiler
}

func NewService(featureDatabase FeatureDatabase, sourceStorage, tileStorage ObjectStorage, tiler Tiler) *Service {
	return &Service{
		database:      featureDatabase,
		sourceStorage: sourceStorage,
		tileStorage:   tileStorage,
		tiler:         tiler,
	}
}

func (s *Service) HandleProcessFeature(ctx context.Context, message messaging.Message) error {
	startedAt := time.Now()
	requestID := trace.ID(ctx)
	var job Job
	if err := json.Unmarshal(message.Body, &job); err != nil {
		return fmt.Errorf("decode process feature job: %w", err)
	}
	if job.ProjectID == "" || job.FeatureID == "" || job.ObjectName == "" {
		return fmt.Errorf("process feature job requires projectId, featureId, layerId, and objectName")
	}
	log.Printf(
		"event=feature_processing_started request_id=%q message_id=%q project_id=%q feature_id=%q object_name=%q",
		requestID,
		message.ID,
		job.ProjectID,
		job.FeatureID,
		job.ObjectName,
	)
	status, err := s.database.FeatureStatus(ctx, job.FeatureID)
	if err != nil {
		return err
	}
	if !status.Exists || !status.Downloaded {
		return fmt.Errorf("process feature %q: downloaded feature does not exist", job.FeatureID)
	}
	if status.Tiled {
		log.Printf(
			"event=feature_processing_skipped request_id=%q message_id=%q feature_id=%q duration=%s",
			requestID,
			message.ID,
			job.FeatureID,
			time.Since(startedAt).Round(time.Millisecond),
		)
		return nil
	}
	workingDirectory, err := os.MkdirTemp("", "zpi-processor-*")
	if err != nil {
		return fmt.Errorf("create processor working directory: %w", err)
	}
	defer os.RemoveAll(workingDirectory)

	inputPath := filepath.Join(workingDirectory, "source.tif")
	input, err := os.Create(inputPath)
	if err != nil {
		return fmt.Errorf("create temporary GeoTIFF: %w", err)
	}
	downloadStartedAt := time.Now()
	downloadedBytes, downloadErr := s.sourceStorage.Download(ctx, job.ObjectName, input)
	closeErr := input.Close()
	if downloadErr != nil {
		return downloadErr
	}
	if closeErr != nil {
		return fmt.Errorf("close temporary GeoTIFF: %w", closeErr)
	}
	log.Printf(
		"event=source_downloaded request_id=%q feature_id=%q object_name=%q bytes=%d duration=%s",
		requestID,
		job.FeatureID,
		job.ObjectName,
		downloadedBytes,
		time.Since(downloadStartedAt).Round(time.Millisecond),
	)

	tilesDirectory := filepath.Join(workingDirectory, "tiles")
	tilingStartedAt := time.Now()
	if err := s.tiler.Tile(ctx, inputPath, tilesDirectory); err != nil {
		return fmt.Errorf("tile feature %q: %w", job.FeatureID, err)
	}
	log.Printf(
		"event=feature_tiled request_id=%q feature_id=%q duration=%s",
		requestID,
		job.FeatureID,
		time.Since(tilingStartedAt).Round(time.Millisecond),
	)

	uploadStartedAt := time.Now()
	setID, _, _ := strings.Cut(job.FeatureID, ".")

	tileCount, err := s.uploadTiles(ctx, setID, tilesDirectory)
	if err != nil {
		return err
	}
	if tileCount == 0 {
		return fmt.Errorf("tiling feature %q produced no PNG tiles", job.FeatureID)
	}
	log.Printf(
		"event=tiles_uploaded request_id=%q feature_id=%q tile_count=%d duration=%s",
		requestID,
		job.FeatureID,
		tileCount,
		time.Since(uploadStartedAt).Round(time.Millisecond),
	)
	if err := s.database.MarkFeatureTiled(ctx, job.FeatureID, tileCount); err != nil {
		return err
	}
	log.Printf("event=feature_marked_tiled request_id=%q feature_id=%q", requestID, job.FeatureID)

	log.Printf(
		"event=feature_processing_completed request_id=%q message_id=%q feature_id=%q tile_count=%d duration=%s",
		requestID,
		message.ID,
		job.FeatureID,
		tileCount,
		time.Since(startedAt).Round(time.Millisecond),
	)

	return nil
}

func (s *Service) uploadTiles(ctx context.Context, featureID, tilesDirectory string) (int, error) {
	tileCount := 0
	err := filepath.WalkDir(tilesDirectory, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".png") {
			return nil
		}

		relativePath, err := filepath.Rel(tilesDirectory, filePath)
		if err != nil {
			return err
		}
		file, err := os.Open(filePath)
		if err != nil {
			return err
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return err
		}

		objectName := path.Join(featureID, filepath.ToSlash(relativePath))
		_, uploadErr := s.tileStorage.Upload(ctx, objectName, file, info.Size(), "image/png")
		closeErr := file.Close()
		if uploadErr != nil {
			return uploadErr
		}
		if closeErr != nil {
			return fmt.Errorf("close generated tile %q: %w", filePath, closeErr)
		}
		tileCount++
		return nil
	})
	if err != nil {
		return tileCount, fmt.Errorf("upload tiles for feature %q: %w", featureID, err)
	}
	return tileCount, nil
}
