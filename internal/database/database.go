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
	ColorSpace   string
	PixelSize    float64
	DatasetDate  time.Time
	FileSize     int64
	DownloadedAt time.Time
}

type FeatureStatus struct {
	Exists     bool
	Downloaded bool
	Tiled      bool
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

func (c *Client) InsertFeature(ctx context.Context, feature Feature) error {
	const query = `
		INSERT INTO geoportal.features (
			id, color_space, pixel_size, dataset_date, file_size, downloaded, downloaded_at
		) VALUES ($1, $2, $3, $4, $5, TRUE, $6)
		ON CONFLICT (id) DO UPDATE SET
			color_space = EXCLUDED.color_space,
			pixel_size = EXCLUDED.pixel_size,
			dataset_date = EXCLUDED.dataset_date,
			file_size = EXCLUDED.file_size,
			downloaded = TRUE,
			downloaded_at = EXCLUDED.downloaded_at`

	if _, err := c.pool.Exec(ctx, query,
		feature.ID,
		feature.ColorSpace,
		feature.PixelSize,
		feature.DatasetDate,
		feature.FileSize,
		feature.DownloadedAt,
	); err != nil {
		return fmt.Errorf("insert downloaded feature %q: %w", feature.ID, err)
	}
	return nil
}

func (c *Client) FeatureStatus(ctx context.Context, id string) (FeatureStatus, error) {
	const query = `
		SELECT downloaded, tiled
		FROM geoportal.features
		WHERE id = $1`

	var status FeatureStatus
	err := c.pool.QueryRow(ctx, query, id).Scan(&status.Downloaded, &status.Tiled)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return status, nil
		}
		return FeatureStatus{}, fmt.Errorf("get feature %q status: %w", id, err)
	}
	status.Exists = true
	return status, nil
}

func (c *Client) MarkFeatureTiled(ctx context.Context, id string) error {
	const query = `
		UPDATE geoportal.features
		SET tiled = TRUE
		WHERE id = $1`

	result, err := c.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("mark feature %q as tiled: %w", id, err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("mark feature %q as tiled: feature does not exist", id)
	}
	return nil
}
