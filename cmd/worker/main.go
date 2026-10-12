package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"bird-zpi/downloader/internal/download"
	"bird-zpi/downloader/internal/messaging/rabbitmq"
	"bird-zpi/downloader/internal/worker"
)

func run(ctx context.Context, rabbitMQURL string) error {
	service := download.NewService()
	log.Print("download worker started")
	return worker.Run(
		ctx,
		rabbitMQURL,
		download.CreateDownloadQueue,
		download.HandleCreateDownload(service.CreateDownload),
	)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rabbitMQURL := flag.String("rabbitmq-url", rabbitmq.DefaultURL, "RabbitMQ connection URL")
	flag.Parse()

	if err := run(ctx, *rabbitMQURL); err != nil {
		log.Fatal(err)
	}
}
