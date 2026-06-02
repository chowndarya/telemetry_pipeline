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
	"google.golang.org/grpc/credentials/insecure"

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

func buildPointFromTelemetry(msg *pb.TelemetryRequest, tableName string) (string, map[string]string, map[string]interface{}, time.Time) {
	tags := make(map[string]string)
	if msg.LabelsRaw != "" {
		tags = parseLabelsRaw(msg.LabelsRaw)
	}

	if msg.GpuId != "" {
		tags["gpu_id"] = msg.GpuId
	}
	if msg.Device != "" {
		tags["device"] = msg.Device
	}
	if msg.Uuid != "" {
		tags["uuid"] = msg.Uuid
	}
	if msg.Namespace != "" {
		tags["namespace"] = msg.Namespace
	}
	if len(msg.ModelName) > 0 {
		tags["modelName"] = strings.Join(msg.ModelName, ",")
	}

	fields := map[string]interface{}{
		"metric_name": msg.MetricName,
		"value":       msg.Value,
	}

	var ts time.Time
	if msg.Timestamp != 0 {
		ts = time.Unix(0, msg.Timestamp)
	} else {
		ts = time.Now()
	}

	return tableName, tags, fields, ts
}

func main() {
	influxURL := os.Getenv("INFLUXDB_URL")
	if influxURL == "" {
		influxURL = "http://localhost:8181"
	}
	influxToken := os.Getenv("INFLUXDB_TOKEN")
	if influxToken == "" {
		influxToken = "apiv3_Y6QmczU2nRBzBMUFz9WMDkZc_S6PlzTe8Fs2OF2wg-uzjJmhRAqCLkQw8PEfuyO-NZm5y2dNsDpzIT0qRTVsUw"
	}
	influxOrg := os.Getenv("INFLUXDB_ORG")
	if influxOrg == "" {
		influxOrg = "ai_org"
	}

	databaseName := "tel_db"
	tableName := "gpu_metrics"

	influxClient := influxdb2.NewClient(influxURL, influxToken)
	defer influxClient.Close()
	writeAPI := influxClient.WriteAPIBlocking(influxOrg, databaseName)

	grpcServerAddr := os.Getenv("GRPC_SERVER_ADDR")
	if grpcServerAddr == "" {
		grpcServerAddr = "localhost:50051"
	}

	conn, err := grpc.NewClient(grpcServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewTelemetryServiceClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("Shutdown signal received")
		cancel()
	}()

	stream, err := client.CollectTelemetry(ctx, &pb.TelemetryRequest{})
	if err != nil {
		log.Fatalf("CollectTelemetry failed: %v", err)
	}

	log.Println("Started receiving telemetry stream...")

	var processed, ackFailed, writeFailed uint64

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Println("Stream ended")
			break
		}
		if err != nil {
			log.Printf("Recv error: %v", err)
			return
		}

		if msg.MessageId == "" {
			log.Printf("WARN: received message without MessageId — queue may be old version")
		}

		// ---- Process: write to InfluxDB ----
		tags := parseLabelsRaw(msg.LabelsRaw)
		if msg.GpuId != "" {
			tags["gpu_id"] = msg.GpuId
		}
		if msg.Device != "" {
			tags["device"] = msg.Device
		}
		if msg.Uuid != "" {
			tags["uuid"] = msg.Uuid
		}
		if msg.Namespace != "" {
			tags["namespace"] = msg.Namespace
		}
		if len(msg.ModelName) > 0 {
			tags["modelName"] = strings.Join(msg.ModelName, ",")
		}

		fields := map[string]interface{}{
			"metric_name": msg.MetricName,
			"value":       msg.Value,
		}

		ts := time.Now()
		if msg.Timestamp != 0 {
			ts = time.Unix(0, msg.Timestamp)
		}

		point := influxdb2.NewPoint(tableName, tags, fields, ts)

		writeCtx, writeCancel := context.WithTimeout(ctx, 10*time.Second)
		writeErr := writeAPI.WritePoint(writeCtx, point)
		writeCancel()

		if writeErr != nil {
			writeFailed++
			log.Printf("InfluxDB write FAILED for msg=%s: %v. NOT acking — will be requeued by server.",
				msg.MessageId, writeErr)
			// Critical: do NOT ack. Server timeout will requeue.
			continue
		}

		// ---- Ack ONLY after successful write ----
		if msg.MessageId != "" {
			if err := ackMessage(ctx, client, msg.MessageId); err != nil {
				ackFailed++
				log.Printf("Ack FAILED for msg=%s: %v. Message will be requeued and re-processed (duplicate write OK due to idempotent timestamp).",
					msg.MessageId, err)
				// Don't fail the loop — duplicates in InfluxDB are tolerable
			}
		}

		processed++
		if processed%100 == 0 {
			log.Printf("Stats: processed=%d, ackFailed=%d, writeFailed=%d",
				processed, ackFailed, writeFailed)
		}
		log.Printf("Processed msg=%s GPU=%s metric=%s value=%v",
			msg.MessageId, msg.GpuId, msg.MetricName, msg.Value)
	}
}

// ackMessage sends an ack with a short timeout & one retry.
func ackMessage(parentCtx context.Context, client pb.TelemetryServiceClient, messageId string) error {
	const maxAttempts = 2
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(parentCtx, 3*time.Second)
		resp, err := client.AckTelemetry(ctx, &pb.AckRequest{MessageId: messageId})
		cancel()
		if err == nil && resp.Success {
			return nil
		}
		lastErr = err
		if attempt < maxAttempts {
			time.Sleep(200 * time.Millisecond)
		}
	}
	return fmt.Errorf("ack failed after %d attempts: %w", maxAttempts, lastErr)
}
