package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ---- Retry/backoff configuration ----
const (
	defaultMaxRetries     = 5
	defaultInitialBackoff = 100 * time.Millisecond
	defaultMaxBackoff     = 5 * time.Second
	defaultBackoffFactor  = 2.0
	defaultPerCallTimeout = 5 * time.Second
)

type retryConfig struct {
	maxRetries     int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	factor         float64
	perCallTimeout time.Duration
}

func loadRetryConfig() retryConfig {
	cfg := retryConfig{
		maxRetries:     defaultMaxRetries,
		initialBackoff: defaultInitialBackoff,
		maxBackoff:     defaultMaxBackoff,
		factor:         defaultBackoffFactor,
		perCallTimeout: defaultPerCallTimeout,
	}
	if v := os.Getenv("MAX_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.maxRetries = n
		}
	}
	return cfg
}

// isRetryable returns true if the error suggests a transient failure.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.ResourceExhausted, // queue full (our backpressure signal)
		codes.Unavailable,      // server temporarily down
		codes.DeadlineExceeded: // RPC timed out
		return true
	default:
		return false
	}
}

// computeBackoff returns the backoff duration for a given attempt with full jitter.
// attempt=0 is the first retry.
func computeBackoff(attempt int, cfg retryConfig) time.Duration {
	// Exponential: initial * factor^attempt
	d := float64(cfg.initialBackoff) * math.Pow(cfg.factor, float64(attempt))
	if d > float64(cfg.maxBackoff) {
		d = float64(cfg.maxBackoff)
	}
	// Full jitter: random value in [0, d)
	jittered := time.Duration(rand.Float64() * d)
	return jittered
}

// sendWithRetry sends a telemetry request with exponential-backoff retries on
// transient errors (queue full, server unavailable, deadline exceeded).
// Returns nil on success, or the last error if all attempts fail.
func sendWithRetry(
	ctx context.Context,
	client pb.TelemetryServiceClient,
	req *pb.TelemetryRequest,
	cfg retryConfig,
) error {
	var lastErr error

	// Total attempts = 1 initial + maxRetries
	for attempt := 0; attempt <= cfg.maxRetries; attempt++ {
		// Per-call timeout, independent of parent ctx (but respects parent cancel)
		callCtx, cancel := context.WithTimeout(ctx, cfg.perCallTimeout)
		resp, err := client.SendTelemetry(callCtx, req)
		cancel()

		// Success at gRPC level — but check application-level Success too
		if err == nil {
			if resp != nil && !resp.Success {
				// Server returned a logical failure (e.g., queue full but no gRPC error)
				// Treat "queue full" message as retryable.
				log.Printf("Server returned Success=false: %s", resp.Message)
				lastErr = fmt.Errorf("server rejected: %s", resp.Message)
				// fall through to retry logic below
			} else {
				return nil // ✅ true success
			}
		} else {
			lastErr = err
		}

		// Decide whether to retry
		if !isRetryable(lastErr) && err != nil {
			log.Printf("Non-retryable error, giving up: %v", err)
			return lastErr
		}

		// No more attempts left
		if attempt == cfg.maxRetries {
			break
		}

		// Compute backoff & wait (respecting context cancellation)
		wait := computeBackoff(attempt, cfg)
		code := status.Code(lastErr)
		log.Printf("Retry %d/%d after %v (code=%s, err=%v)",
			attempt+1, cfg.maxRetries, wait, code, lastErr)

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
		case <-time.After(wait):
		}
	}

	return fmt.Errorf("exhausted %d retries: %w", cfg.maxRetries, lastErr)
}

func main() {
	rand.Seed(time.Now().UnixNano())

	csvFilePath := os.Getenv("CSV_FILE_PATH")
	if csvFilePath == "" {
		csvFilePath = "dcgm_metrics_20250718_134233.csv"
	}

	sleepDuration := 10 * time.Second
	if val := os.Getenv("SLEEP_DURATION_SECONDS"); val != "" {
		if sec, err := strconv.Atoi(val); err == nil {
			sleepDuration = time.Duration(sec) * time.Second
		}
	}

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

	retryCfg := loadRetryConfig()
	log.Printf("Retry config: maxRetries=%d, initialBackoff=%v, maxBackoff=%v",
		retryCfg.maxRetries, retryCfg.initialBackoff, retryCfg.maxBackoff)

	// Track drop counter across cycles
	var totalDropped, totalSent uint64

	for {
		sent, dropped, err := processCSVAndSend(csvFilePath, client, retryCfg)
		totalSent += sent
		totalDropped += dropped
		if err != nil {
			log.Printf("Error processing CSV: %v", err)
		}
		log.Printf("Cycle done. sent=%d, dropped=%d (total sent=%d, total dropped=%d). Sleeping %v",
			sent, dropped, totalSent, totalDropped, sleepDuration)
		time.Sleep(sleepDuration)
	}
}

func processCSVAndSend(
	csvFilePath string,
	client pb.TelemetryServiceClient,
	retryCfg retryConfig,
) (sent, dropped uint64, err error) {
	file, ferr := os.Open(csvFilePath)
	if ferr != nil {
		return 0, 0, fmt.Errorf("failed to open CSV file: %w", ferr)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))

	if _, err := reader.Read(); err != nil {
		return 0, 0, fmt.Errorf("failed to read CSV header: %w", err)
	}

	hostname, _ := os.Hostname()

	for {
		record, rerr := reader.Read()
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return sent, dropped, fmt.Errorf("error reading CSV record: %w", rerr)
		}

		req, mapErr := mapRecordToRequest(record, hostname)
		if mapErr != nil {
			log.Printf("Skipping record: %v", mapErr)
			continue
		}

		// Long-lived parent context for this row's retries
		ctx, cancel := context.WithTimeout(context.Background(),
			retryCfg.perCallTimeout*time.Duration(retryCfg.maxRetries+1)+retryCfg.maxBackoff*time.Duration(retryCfg.maxRetries))

		sendErr := sendWithRetry(ctx, client, req, retryCfg)
		cancel()

		if sendErr != nil {
			dropped++
			log.Printf("DROPPED message ts=%d metric=%s after retries: %v",
				req.Timestamp, req.MetricName, sendErr)
			continue
		}

		sent++
		log.Printf("Sent telemetry: ts=%d metric=%s", req.Timestamp, req.MetricName)
	}

	return sent, dropped, nil
}

func mapRecordToRequest(record []string, hostname string) (*pb.TelemetryRequest, error) {
	if len(record) < 12 {
		return nil, fmt.Errorf("incomplete record")
	}
	val, err := strconv.ParseFloat(record[10], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid value '%s': %w", record[10], err)
	}
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return nil, fmt.Errorf("value is not a finite number: %f", val)
	}

	return &pb.TelemetryRequest{
		Timestamp:  time.Now().UnixNano(),
		MetricName: record[1],
		GpuId:      record[2],
		Device:     record[3],
		Uuid:       record[4],
		ModelName:  record[5:9],
		Namespace:  record[9],
		Value:      val,
		LabelsRaw:  fmt.Sprintf("%s,source_node=%s", record[11], hostname),
	}, nil
}
