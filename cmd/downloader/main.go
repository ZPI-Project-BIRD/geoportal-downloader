package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	downloaderv1 "bird-zpi/downloader/gen/downloader/v1"
	"bird-zpi/downloader/internal/config"
	"bird-zpi/downloader/internal/database"
	"bird-zpi/downloader/internal/download"
	"bird-zpi/downloader/internal/messaging/rabbitmq"
	grpcapi "bird-zpi/downloader/internal/transport/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func newGRPCServer(featureDatabase download.FeatureDatabase, publisher download.RetryPublisher) *grpc.Server {
	grpcServer := grpc.NewServer()
	downloaderv1.RegisterDownloaderServiceServer(grpcServer, grpcapi.NewServer(featureDatabase, publisher))
	reflection.Register(grpcServer)
	return grpcServer
}

func run(ctx context.Context, address, rabbitMQURL, databaseURL string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}
	defer listener.Close()

	featureDatabase, err := database.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer featureDatabase.Close()

	jobPublisher, err := rabbitmq.NewPublisher(rabbitMQURL)
	if err != nil {
		return err
	}
	defer jobPublisher.Close()

	grpcServer := newGRPCServer(featureDatabase, jobPublisher)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	log.Printf("downloader gRPC server listening on %s", listener.Addr())
	if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("serve gRPC: %w", err)
	}
	return nil
}

type configValues struct {
	Address     string `envconfig:"GRPC_ADDRESS" default:"0.0.0.0:50051"`
	RabbitMQURL string `envconfig:"RABBITMQ_URL" required:"true"`
	DatabaseURL string `envconfig:"DATABASE_URL" required:"true"`
}

func loadConfig() (configValues, error) {
	var values configValues
	if err := config.Load(&values); err != nil {
		return configValues{}, err
	}
	if _, _, err := net.SplitHostPort(values.Address); err != nil {
		return configValues{}, fmt.Errorf("invalid GRPC_ADDRESS %q: %w", values.Address, err)
	}
	return values, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	values, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	if err := run(ctx, values.Address, values.RabbitMQURL, values.DatabaseURL); err != nil {
		log.Fatal(err)
	}
}
