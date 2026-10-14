package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"bird-zpi/downloader/internal/config"
	"bird-zpi/downloader/internal/database"
	"bird-zpi/downloader/internal/processor"
	"bird-zpi/downloader/internal/storage"
	"bird-zpi/downloader/internal/worker"
)

type configValues struct {
	RabbitMQURL    string `envconfig:"RABBITMQ_URL" required:"true"`
	DatabaseURL    string `envconfig:"DATABASE_URL" required:"true"`
	MinioEndpoint  string `envconfig:"MINIO_ENDPOINT" default:"localhost:9000"`
	MinioAccessKey string `envconfig:"MINIO_ACCESS_KEY" required:"true"`
	MinioSecretKey string `envconfig:"MINIO_SECRET_KEY" required:"true"`
	SourceBucket   string `envconfig:"MINIO_SOURCE_BUCKET" default:"features"`
	TilesBucket    string `envconfig:"MINIO_TILES_BUCKET" default:"tiles"`
	MinioSecure    bool   `envconfig:"MINIO_SECURE" default:"false"`
	GdalCommand    string `envconfig:"GDAL2TILES_COMMAND" default:"gdal2tiles.py"`
	Zoom           string `envconfig:"TILE_ZOOM"`
}

func loadConfig() (configValues, error) {
	var values configValues
	if err := config.Load(&values); err != nil {
		return configValues{}, err
	}
	return values, nil
}

func run(ctx context.Context, cfg configValues) error {
	featureDatabase, err := database.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer featureDatabase.Close()

	sourceStorage, err := storage.New(ctx, cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey, cfg.SourceBucket, cfg.MinioSecure)
	if err != nil {
		return err
	}

	tileStorage, err := storage.New(ctx, cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey, cfg.TilesBucket, cfg.MinioSecure)
	if err != nil {
		return err
	}

	service := processor.NewService(featureDatabase, sourceStorage, tileStorage, processor.GDALTiler{
		Command: cfg.GdalCommand,
		Zoom:    cfg.Zoom,
	})

	log.Print("processor worker started")
	return worker.Run(ctx, cfg.RabbitMQURL, processor.Queue, service.HandleProcessFeature)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}
