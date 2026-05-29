package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto" // Replace with your actual proto package import

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Read CSV file path from environment variable
	csvFilePath := os.Getenv("CSV_FILE_PATH")
	if csvFilePath == "" {
		csvFilePath = "dcgm_metrics_20250718_134233.csv" // default path
		//log.Fatal("CSV_FILE_PATH environment variable is not set")
	}

	// Read sleep duration from environment variable or default to 10 seconds
	sleepDuration := 10 * time.Second
	if val := os.Getenv("SLEEP_DURATION_SECONDS"); val != "" {
		if sec, err := strconv.Atoi(val); err == nil {
			sleepDuration = time.Duration(sec) * time.Second
		}
	}

	// Setup gRPC connection to the server
	grpcServerAddr := os.Getenv("GRPC_SERVER_ADDR")
	if grpcServerAddr == "" {
		log.Fatal("GRPC_SERVER_ADDR environment variable is not set")
	}
	conn, err := grpc.NewClient(grpcServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()
	client := pb.NewTelemetryServiceClient(conn)

	for {
		err := processCSVAndSend(csvFilePath, client)
		if err != nil {
			log.Printf("Error processing CSV: %v", err)
		}
		log.Printf("Completed one full CSV read cycle, sleeping for %v", sleepDuration)
		time.Sleep(sleepDuration)
	}
}

func processCSVAndSend(csvFilePath string, client pb.TelemetryServiceClient) error {
	file, err := os.Open(csvFilePath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))

	// Read header line to get field names
	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("error reading CSV record: %w", err)
		}

		// Map CSV columns to variables by header index
		// Expected headers: timestamp, metric_name, gpu_id, device, uuid, modelName, modelName, modelName, modelName, namespace, value, labels_raw
		// We ignore CSV timestamp and generate current time instead

		// Defensive check for record length
		if len(record) < 12 {
			log.Printf("Skipping incomplete record: %v", record)
			continue
		}

		// Parse value field to float64
		val, err := strconv.ParseFloat(record[10], 64)
		if err != nil {
			log.Printf("Invalid value field '%s': %v", record[10], err)
			continue
		}

		// Collect modelName fields (indexes 5 to 8)
		modelNames := record[5:9]

		// Create TelemetryRequest with current timestamp
		req := &pb.TelemetryRequest{
			Timestamp:  time.Now().UnixNano(),
			MetricName: record[1],
			GpuId:      record[2],
			Device:     record[3],
			Uuid:       record[4],
			ModelName:  modelNames,
			Namespace:  record[9],
			Value:      val,
			LabelsRaw:  record[11],
		}
		// Send JSON data to gRPC service
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = client.SendTelemetry(ctx, req)
		if err != nil {
			log.Printf("Failed to send telemetry data: %v", err)
			continue
		}

		log.Printf("Sent telemetry data: %s", req.String())
	}

	return nil
}
