package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Feature struct {
	ID           string
	SourceURL    string
	ColorSpace   string
	PixelSize    float64
	DatasetDate  time.Time
	ObjectName   string
	FileSize     int64
	DownloadedAt time.Time
}

type DiscoveredFeature struct {
	ID          string
	SourceURL   string
	ColorSpace  string
	PixelSize   float64
	DatasetDate time.Time
}

type FeatureStatus struct {
	Exists     bool
	Downloaded bool
	Tiled      bool
}

var ErrProjectNotFound = errors.New("project not found")

type Point struct {
	X float64
	Y float64
}

type ProjectArea struct {
	TopLeft     Point
	BottomRight Point
}

type Client struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, url string) (*Client, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	return &Client{pool: pool}, nil
}

func (c *Client) Close() {
	c.pool.Close()
}

func (c *Client) ProjectArea(ctx context.Context, projectID string) (ProjectArea, error) {
	const query = `
		SELECT
			ST_X(ST_Transform(top_left, 2180)),
			ST_Y(ST_Transform(top_left, 2180)),
			ST_X(ST_Transform(bottom_right, 2180)),
			ST_Y(ST_Transform(bottom_right, 2180))
		FROM public.projects
		WHERE id = $1`

	var area ProjectArea
	err := c.pool.QueryRow(ctx, query, projectID).Scan(
		&area.TopLeft.X,
		&area.TopLeft.Y,
		&area.BottomRight.X,
		&area.BottomRight.Y,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProjectArea{}, fmt.Errorf("%w: %s", ErrProjectNotFound, projectID)
	}
	if err != nil {
		return ProjectArea{}, fmt.Errorf("get project %q area: %w", projectID, err)
	}
	return area, nil
}

func (c *Client) InsertDiscoveredFeature(ctx context.Context, projectID string, feature DiscoveredFeature) (bool, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin inserting feature %q: %w", feature.ID, err)
	}
	defer tx.Rollback(ctx)

	const insertFeature = `
		INSERT INTO geoportal.features (
			id, source_url, color_space, pixel_size, dataset_date
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE SET
			source_url = EXCLUDED.source_url,
			color_space = EXCLUDED.color_space,
			pixel_size = EXCLUDED.pixel_size,
			dataset_date = EXCLUDED.dataset_date,
			updated_at = NOW()`

	if _, err := tx.Exec(ctx, insertFeature,
		feature.ID,
		feature.SourceURL,
		feature.ColorSpace,
		feature.PixelSize,
		feature.DatasetDate,
	); err != nil {
		return false, fmt.Errorf("insert feature %q: %w", feature.ID, err)
	}

	const insertProjectFeature = `
		INSERT INTO geoportal.project_features (project_id, feature_id)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING`

	result, err := tx.Exec(ctx, insertProjectFeature, projectID, feature.ID)
	if err != nil {
		return false, fmt.Errorf("insert project feature %q: %w", feature.ID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit feature %q: %w", feature.ID, err)
	}
	return result.RowsAffected() == 1, nil
}

func (c *Client) UpsertDownloadedFeature(ctx context.Context, feature Feature) error {
	const query = `
		INSERT INTO geoportal.features (
			id, source_url, color_space, pixel_size, dataset_date,
			object_name, file_size, downloaded_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			source_url = EXCLUDED.source_url,
			color_space = EXCLUDED.color_space,
			pixel_size = EXCLUDED.pixel_size,
			dataset_date = EXCLUDED.dataset_date,
			object_name = EXCLUDED.object_name,
			file_size = EXCLUDED.file_size,
			downloaded_at = EXCLUDED.downloaded_at,
			updated_at = NOW()`

	if _, err := c.pool.Exec(ctx, query,
		feature.ID,
		feature.SourceURL,
		feature.ColorSpace,
		feature.PixelSize,
		feature.DatasetDate,
		feature.ObjectName,
		feature.FileSize,
		feature.DownloadedAt,
	); err != nil {
		return fmt.Errorf("upsert downloaded feature %q: %w", feature.ID, err)
	}
	return nil
}

func (c *Client) IsFeatureDownloaded(ctx context.Context, id string) (bool, error) {
	const query = `
		SELECT EXISTS (
			SELECT 1 FROM geoportal.features
			WHERE id = $1 AND downloaded_at IS NOT NULL
		)`

	var downloaded bool
	if err := c.pool.QueryRow(ctx, query, id).Scan(&downloaded); err != nil {
		return false, fmt.Errorf("check feature %q downloaded: %w", id, err)
	}
	return downloaded, nil
}

func (c *Client) CreateDetectionJob(ctx context.Context, projectID string, featureID string) error {
	const query = `
		WITH existing_job AS (
			SELECT id
			FROM geoportal.recognition_jobs
			WHERE project_id = $1 AND feature_id = $2
		), reset_failed_job AS (
			UPDATE geoportal.recognition_jobs
			SET
				status = '0_PENDING',
				created_at = NOW(),
				started_at = NOW(),
				finished_at = NULL,
				error = NULL
			WHERE id IN (SELECT id FROM existing_job)
				AND status = '3_FAILED'
		)
		INSERT INTO geoportal.recognition_jobs (
			project_id, feature_id, started_at
		)
		SELECT $1, $2, NOW()
		WHERE NOT EXISTS (SELECT 1 FROM existing_job)`

	if _, err := c.pool.Exec(ctx, query, projectID, featureID); err != nil {
		return fmt.Errorf("create detection job for project %q and feature %q: %w", projectID, featureID, err)
	}
	return nil
}

func (c *Client) FeatureStatus(ctx context.Context, id string) (FeatureStatus, error) {
	const query = `
		SELECT downloaded_at IS NOT NULL, tiled_at IS NOT NULL
		FROM geoportal.features
		WHERE id = $1`

	var status FeatureStatus
	err := c.pool.QueryRow(ctx, query, id).Scan(&status.Downloaded, &status.Tiled)
	if errors.Is(err, pgx.ErrNoRows) {
		return FeatureStatus{}, nil
	}
	if err != nil {
		return FeatureStatus{}, fmt.Errorf("get feature %q status: %w", id, err)
	}
	status.Exists = true
	return status, nil
}
