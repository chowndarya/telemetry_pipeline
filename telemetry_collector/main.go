package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"google.golang.org/grpc"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto"
)

func parseLabelsRaw(labelsRaw string) map[string]string {
	tags := make(map[string]string)
	pairs := strings.Split(labelsRaw, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			key := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			if key != "" {
				tags[key] = value
			}
		} else {
			// If no '=', treat entire string as key with empty value
			tags[pair] = ""
		}
	}
	return tags
}

func main() {
	// InfluxDB connection parameters from environment variables
	influxURL := os.Getenv("INFLUXDB_URL")
	if influxURL == "" {
		influxURL = "http://localhost:8181" // Default port for InfluxDB 3
	}
	influxToken := os.Getenv("INFLUXDB_TOKEN")
	if influxToken == "" {
		influxToken = "apiv3_Y6QmczU2nRBzBMUFz9WMDkZc_S6PlzTe8Fs2OF2wg-uzjJmhRAqCLkQw8PEfuyO-NZm5y2dNsDpzIT0qRTVsUw"
		//log.Fatal("INFLUXDB_TOKEN environment variable not set")
	}
	influxOrg := os.Getenv("INFLUXDB_ORG")
	if influxOrg == "" {
		influxOrg = "ai_org"
		//log.Fatal("INFLUXDB_ORG environment variable not set")
	}

	// Use fixed database and table names as requested
	databaseName := "tel_db"
	tableName := "gpu_metrics"

	// Create InfluxDB client and write API
	influxClient := influxdb2.NewClient(influxURL, influxToken)
	defer influxClient.Close()
	writeAPI := influxClient.WriteAPIBlocking(influxOrg, databaseName)

	// gRPC server address from environment variable
	grpcServerAddr := os.Getenv("GRPC_SERVER_ADDR")
	if grpcServerAddr == "" {
		grpcServerAddr = "localhost:50051"
	}

	// Set up gRPC connection
	conn, err := grpc.Dial(grpcServerAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	client := pb.NewTelemetryServiceClient(conn)

	// Context with cancel for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals for graceful shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Prepare request (empty or with filters if needed)
	req := &pb.TelemetryRequest{}

	// Call server-streaming RPC
	stream, err := client.CollectTelemetry(ctx, req)
	if err != nil {
		log.Fatalf("Error calling CollectTelemetry: %v", err)
	}

	fmt.Println("Started receiving telemetry stream...")

	for {
		// Receive a TelemetryRequest message from the stream
		telemetryMsg, err := stream.Recv()
		if err == io.EOF {
			// Stream ended normally
			fmt.Println("Telemetry stream ended")
			break
		}
		if err != nil {
			log.Fatalf("Error receiving telemetry message: %v", err)
		}

		// Extract labels_raw and parse it into tags map
		labelsRaw := telemetryMsg.LabelsRaw
		var tags map[string]string
		if labelsRaw != "" {
			tags = parseLabelsRaw(labelsRaw)
		} else {
			tags = make(map[string]string)
		}

		// Add other tags from telemetry message fields
		if telemetryMsg.GpuId != "" {
			tags["gpu_id"] = telemetryMsg.GpuId
		}
		if telemetryMsg.Device != "" {
			tags["device"] = telemetryMsg.Device
		}
		if telemetryMsg.Uuid != "" {
			tags["uuid"] = telemetryMsg.Uuid
		}
		if telemetryMsg.Namespace != "" {
			tags["namespace"] = telemetryMsg.Namespace
		}
		if len(telemetryMsg.ModelName) > 0 {
			tags["modelName"] = strings.Join(telemetryMsg.ModelName, ",")
		}

		// Prepare fields
		fields := map[string]interface{}{
			"metric_name": telemetryMsg.MetricName,
			"value":       telemetryMsg.Value,
		}

		// Use timestamp from message or current time if zero
		var ts time.Time
		if telemetryMsg.Timestamp != 0 {
			ts = time.Unix(0, telemetryMsg.Timestamp)
		} else {
			ts = time.Now()
		}

		// Create InfluxDB point with specified table name
		point := influxdb2.NewPoint(tableName, tags, fields, ts)

		// Write point to InfluxDB
		if err := writeAPI.WritePoint(ctx, point); err != nil {
			log.Printf("Failed to write point to InfluxDB: %v", err)
		} else {
			log.Printf("Wrote telemetry data: GPU %s metric %s value %v", telemetryMsg.GpuId, telemetryMsg.MetricName, telemetryMsg.Value)
		}
	}
}
