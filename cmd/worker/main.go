package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"bird-zpi/downloader/internal/config"
	"bird-zpi/downloader/internal/database"
	"bird-zpi/downloader/internal/download"
	"bird-zpi/downloader/internal/messaging/rabbitmq"
	"bird-zpi/downloader/internal/storage"
	"bird-zpi/downloader/internal/worker"
)

type configValues struct {
	RabbitMQURL    string `envconfig:"RABBITMQ_URL" required:"true"`
	DatabaseURL    string `envconfig:"DATABASE_URL" required:"true"`
	MinioEndpoint  string `envconfig:"MINIO_ENDPOINT" default:"localhost:9000"`
	MinioAccessKey string `envconfig:"MINIO_ACCESS_KEY" required:"true"`
	MinioSecretKey string `envconfig:"MINIO_SECRET_KEY" required:"true"`
	MinioBucket    string `envconfig:"MINIO_SOURCE_BUCKET" default:"features"`
	MinioSecure    bool   `envconfig:"MINIO_SECURE" default:"false"`
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

	objectStorage, err := storage.New(
		ctx,
		cfg.MinioEndpoint,
		cfg.MinioAccessKey,
		cfg.MinioSecretKey,
		cfg.MinioBucket,
		cfg.MinioSecure,
	)
	if err != nil {
		return err
	}

	processorPublisher, err := rabbitmq.NewPublisher(cfg.RabbitMQURL)
	if err != nil {
		return err
	}
	defer processorPublisher.Close()

	service := download.NewDownloadWorker(featureDatabase, objectStorage, processorPublisher, processorPublisher)
	log.Print("download worker started")
	return worker.Run(
		ctx,
		cfg.RabbitMQURL,
		download.DownloadFeatureQueue,
		service.HandleDownloadFeature,
	)
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
