package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ObjectStorage interface {
	Download(context.Context, string, io.Writer) (int64, error)
	Upload(context.Context, string, io.Reader, int64, string) (int64, error)
}

type Service struct {
	sourceStorage ObjectStorage
	tileStorage   ObjectStorage
	tiler         Tiler
}

func NewService(sourceStorage, tileStorage ObjectStorage, tiler Tiler) *Service {
	return &Service{
		sourceStorage: sourceStorage,
		tileStorage:   tileStorage,
		tiler:         tiler,
	}
}

func (s *Service) HandleProcessFeature(ctx context.Context, delivery amqp.Delivery) error {
	var job Job
	if err := json.Unmarshal(delivery.Body, &job); err != nil {
		return fmt.Errorf("decode process feature job: %w", err)
	}
	if job.FeatureID == "" || job.ObjectName == "" {
		return fmt.Errorf("process feature job requires featureId and objectName")
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
	_, downloadErr := s.sourceStorage.Download(ctx, job.ObjectName, input)
	closeErr := input.Close()
	if downloadErr != nil {
		return downloadErr
	}
	if closeErr != nil {
		return fmt.Errorf("close temporary GeoTIFF: %w", closeErr)
	}

	tilesDirectory := filepath.Join(workingDirectory, "tiles")
	if err := s.tiler.Tile(ctx, inputPath, tilesDirectory); err != nil {
		return fmt.Errorf("tile feature %q: %w", job.FeatureID, err)
	}

	tileCount, err := s.uploadTiles(ctx, job.FeatureID, tilesDirectory)
	if err != nil {
		return err
	}
	if tileCount == 0 {
		return fmt.Errorf("tiling feature %q produced no PNG tiles", job.FeatureID)
	}

	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("acknowledge process feature job: %w", err)
	}
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
