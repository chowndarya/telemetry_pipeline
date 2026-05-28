package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
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
	headers, err := reader.Read()
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

		// Map CSV record to a map[string]string using headers
		dataMap := make(map[string]string)
		for i, header := range headers {
			if i < len(record) {
				dataMap[header] = record[i]
			}
		}

		// Convert map to JSON
		jsonData, err := json.Marshal(dataMap)
		if err != nil {
			log.Printf("Failed to marshal JSON: %v", err)
			continue
		}

		// Send JSON data to gRPC service
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		req := &pb.TelemetryRequest{
			JsonPayload: string(jsonData),
			Timestamp:   time.Now().UnixNano(), // Use current time as timestamp
		}

		_, err = client.SendTelemetry(ctx, req)
		if err != nil {
			log.Printf("Failed to send telemetry data: %v", err)
			continue
		}

		log.Printf("Sent telemetry data: %s", jsonData)
	}

	return nil
}
