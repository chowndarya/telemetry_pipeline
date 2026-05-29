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

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto" // replace with actual import path
)

type message struct {
	Timestamp  int64
	MetricName string
	GpuId      string
	Device     string
	Uuid       string
	ModelName  []string
	Namespace  string
	Value      float64
	LabelsRaw  string
}

type TelemetryQueueServer struct {
	pb.UnimplementedTelemetryServiceServer

	mu       sync.Mutex
	messages []message
	cond     *sync.Cond
	closed   bool
}

func NewTelemetryQueueServer() *TelemetryQueueServer {
	s := &TelemetryQueueServer{}
	s.cond = sync.NewCond(&s.mu)
	return s
}

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
		Timestamp:  req.Timestamp,
		MetricName: req.MetricName,
		GpuId:      req.GpuId,
		Device:     req.Device,
		Uuid:       req.Uuid,
		ModelName:  req.ModelName,
		Namespace:  req.Namespace,
		Value:      req.Value,
		LabelsRaw:  req.LabelsRaw,
	})

	s.cond.Signal()

	log.Printf("Enqueued telemetry message with timestamp %d, metric %s", req.Timestamp, req.MetricName)

	return &pb.TelemetryResponse{
		Success: true,
		Message: "Telemetry message enqueued successfully",
	}, nil
}

func (s *TelemetryQueueServer) CollectTelemetry(req *pb.TelemetryRequest, stream pb.TelemetryService_CollectTelemetryServer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		// Wait for messages to be available or server to be closed
		for len(s.messages) == 0 && !s.closed {
			s.cond.Wait()
		}

		// If server is closed and no messages left, end stream
		if s.closed && len(s.messages) == 0 {
			return nil
		}

		// Pop the first message from the queue
		msg := s.messages[0]
		s.messages = s.messages[1:]

		s.mu.Unlock()

		// Convert internal message to protobuf TelemetryRequest
		resp := &pb.TelemetryRequest{
			Timestamp:  msg.Timestamp,
			MetricName: msg.MetricName,
			GpuId:      msg.GpuId,
			Device:     msg.Device,
			Uuid:       msg.Uuid,
			ModelName:  msg.ModelName,
			Namespace:  msg.Namespace,
			Value:      msg.Value,
			LabelsRaw:  msg.LabelsRaw,
		}

		// Send the telemetry message to the client stream
		if err := stream.Send(resp); err != nil {
			s.mu.Lock()
			return err
		}

		s.mu.Lock()
	}
}

func StartGRPCServer(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()

	queueServer := NewTelemetryQueueServer()
	pb.RegisterTelemetryServiceServer(grpcServer, queueServer)

	reflection.Register(grpcServer)

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

	queueServer.mu.Lock()
	queueServer.closed = true
	queueServer.mu.Unlock()

	grpcServer.GracefulStop()
	log.Println("gRPC server stopped gracefully")

	return nil
}

func main() {
	address := ":50051"
	if err := StartGRPCServer(address); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
