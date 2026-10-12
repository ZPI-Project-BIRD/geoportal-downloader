package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	downloaderv1 "bird-zpi/downloader/gen/downloader/v1"
	"bird-zpi/downloader/internal/download"
	grpcapi "bird-zpi/downloader/internal/transport/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func newGRPCServer(downloadService grpcapi.DownloadService) *grpc.Server {
	grpcServer := grpc.NewServer()
	downloaderv1.RegisterDownloaderServiceServer(grpcServer, grpcapi.NewServer(downloadService))
	reflection.Register(grpcServer)
	return grpcServer
}

func run(ctx context.Context, address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}

	downloadService := download.NewService()
	grpcServer := newGRPCServer(downloadService)

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

func printUsage(message string) {
	fmt.Fprintf(flag.CommandLine.Output(), "%s\n\n", message)
	fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options]\n\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	address := flag.String("address", "127.0.0.1:50051", "ip address with port that the gRPC server will listen on")
	flag.Parse()

	_, _, err := net.SplitHostPort(*address)
	if err != nil {
		printUsage("malformed --address provided")
	}

	if err := run(ctx, *address); err != nil {
		log.Fatal(err)
	}
}
