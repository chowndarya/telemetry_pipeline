package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto" // Replace with actual import path of generated proto package
)

// message represents a telemetry message stored in the queue.
type message struct {
	jsonPayload string
	timestamp   int64
}

// TelemetryQueueServer implements the TelemetryService gRPC server.
type TelemetryQueueServer struct {
	pb.UnimplementedTelemetryServiceServer

	mu       sync.Mutex
	messages []message
	cond     *sync.Cond
	closed   bool
}

// NewTelemetryQueueServer creates a new TelemetryQueueServer instance.
func NewTelemetryQueueServer() *TelemetryQueueServer {
	s := &TelemetryQueueServer{}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// SendTelemetry enqueues telemetry messages sent by producers (streamers).
func (s *TelemetryQueueServer) SendTelemetry(ctx context.Context, req *pb.TelemetryRequest) (*pb.TelemetryResponse, error) {
	if req == nil {
		return &pb.TelemetryResponse{
			Success: false,
			Message: "Request is nil",
		}, errors.New("nil request")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return &pb.TelemetryResponse{
			Success: false,
			Message: "Server is shutting down",
		}, errors.New("server closed")
	}

	// Append the message to the queue buffer
	s.messages = append(s.messages, message{
		jsonPayload: req.JsonPayload,
		timestamp:   req.Timestamp,
	})

	// Notify any waiting consumers (if implemented)
	s.cond.Signal()

	log.Printf("Enqueued telemetry message with timestamp %d", req.Timestamp)

	return &pb.TelemetryResponse{
		Success: true,
		Message: "Telemetry message enqueued successfully",
	}, nil
}

// StartGRPCServer starts the gRPC server without TLS and supports graceful shutdown.
func StartGRPCServer(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()

	queueServer := NewTelemetryQueueServer()
	pb.RegisterTelemetryServiceServer(grpcServer, queueServer)

	// Enable reflection for debugging and tooling support
	reflection.Register(grpcServer)

	// Setup graceful shutdown on SIGINT/SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("Starting gRPC server on %s (insecure mode)", address)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC server: %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down gRPC server...")

	// Mark server as closed to reject new requests
	queueServer.mu.Lock()
	queueServer.closed = true
	queueServer.mu.Unlock()

	grpcServer.GracefulStop()
	log.Println("gRPC server stopped gracefully")

	return nil
}

func main() {
	// Read server address from environment variable or use default
	address := ":50051"

	if err := StartGRPCServer(address); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
