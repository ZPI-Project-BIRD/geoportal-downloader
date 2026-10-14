package database

import (
	"context"
	"fmt"
	"time"

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
			id, color_space, pixel_size, dataset_date, file_size, downloaded_at
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO UPDATE SET
			color_space = EXCLUDED.color_space,
			pixel_size = EXCLUDED.pixel_size,
			dataset_date = EXCLUDED.dataset_date,
			file_size = EXCLUDED.file_size,
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
