package main

import (
	"errors"
	"os"
	"testing"
	"time"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto"
	"github.com/chowndarya/telemetry_pipeline/mocks"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// testRetryConfig returns a fast retry config so tests don't take forever.
func testRetryConfig() retryConfig {
	return retryConfig{
		maxRetries:     2,
		initialBackoff: 10 * time.Millisecond,
		maxBackoff:     50 * time.Millisecond,
		factor:         2.0,
		perCallTimeout: 500 * time.Millisecond,
	}
}

// writeTestCSV creates a temporary CSV file with the given content.
func writeTestCSV(t *testing.T, content string) string {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "test-*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpFile.Close()
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })
	return tmpFile.Name()
}

const validCSVHeader = "timestamp,metric_name,gpu_id,device,uuid,m1,m2,m3,m4,ns,value,labels\n"
const validCSVRow = "123,temp,0,dev1,uuid1,a,b,c,d,ns1,25.5,node=test\n"

// --- Test 1: Successful single send ---
func TestProcessCSVAndSend_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockTelemetryServiceClient(ctrl)
	csvPath := writeTestCSV(t, validCSVHeader+validCSVRow)

	mockClient.EXPECT().
		SendTelemetry(gomock.Any(), gomock.Any()).
		Return(&pb.TelemetryResponse{Success: true}, nil).
		Times(1)

	sent, dropped, err := processCSVAndSend(csvPath, mockClient, testRetryConfig())

	t.Logf("Result: sent=%d, dropped=%d, err=%v", sent, dropped, err)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if sent != 1 {
		t.Errorf("Expected sent=1, got %d", sent)
	}
	if dropped != 0 {
		t.Errorf("Expected dropped=0, got %d", dropped)
	}
}

// --- Test 2: Retry succeeds after transient ResourceExhausted (queue full) ---
func TestProcessCSVAndSend_RetrySucceedsAfterQueueFull(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockTelemetryServiceClient(ctrl)
	csvPath := writeTestCSV(t, validCSVHeader+validCSVRow)

	queueFullErr := status.Error(codes.ResourceExhausted, "queue full")

	gomock.InOrder(
		mockClient.EXPECT().
			SendTelemetry(gomock.Any(), gomock.Any()).
			Return(nil, queueFullErr),
		mockClient.EXPECT().
			SendTelemetry(gomock.Any(), gomock.Any()).
			Return(&pb.TelemetryResponse{Success: true}, nil),
	)

	sent, dropped, err := processCSVAndSend(csvPath, mockClient, testRetryConfig())

	t.Logf("Result: sent=%d, dropped=%d, err=%v", sent, dropped, err)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if sent != 1 {
		t.Errorf("Expected sent=1 (succeeded on retry), got %d", sent)
	}
	if dropped != 0 {
		t.Errorf("Expected dropped=0, got %d", dropped)
	}
}

// --- Test 3: All retries fail → message dropped ---
func TestProcessCSVAndSend_AllRetriesFailDrops(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockTelemetryServiceClient(ctrl)
	csvPath := writeTestCSV(t, validCSVHeader+validCSVRow)

	cfg := testRetryConfig() // maxRetries=2, so 3 total attempts
	queueFullErr := status.Error(codes.ResourceExhausted, "queue full")

	mockClient.EXPECT().
		SendTelemetry(gomock.Any(), gomock.Any()).
		Return(nil, queueFullErr).
		Times(cfg.maxRetries + 1) // 3 total calls (1 initial + 2 retries)

	sent, dropped, err := processCSVAndSend(csvPath, mockClient, cfg)

	t.Logf("Result: sent=%d, dropped=%d, err=%v", sent, dropped, err)

	if err != nil {
		t.Errorf("Expected no error (drop is logged, not returned), got: %v", err)
	}
	if sent != 0 {
		t.Errorf("Expected sent=0, got %d", sent)
	}
	if dropped != 1 {
		t.Errorf("Expected dropped=1, got %d", dropped)
	}
}

// --- Test 4: Non-retryable error → immediate drop, no retries ---
func TestProcessCSVAndSend_NonRetryableErrorDropsImmediately(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockTelemetryServiceClient(ctrl)
	csvPath := writeTestCSV(t, validCSVHeader+validCSVRow)

	nonRetryableErr := status.Error(codes.InvalidArgument, "bad request")

	// Should be called exactly once — no retries for non-retryable errors
	mockClient.EXPECT().
		SendTelemetry(gomock.Any(), gomock.Any()).
		Return(nil, nonRetryableErr).
		Times(1)

	sent, dropped, err := processCSVAndSend(csvPath, mockClient, testRetryConfig())

	t.Logf("Result: sent=%d, dropped=%d, err=%v", sent, dropped, err)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if sent != 0 {
		t.Errorf("Expected sent=0, got %d", sent)
	}
	if dropped != 1 {
		t.Errorf("Expected dropped=1, got %d", dropped)
	}
}

// --- Test 5: Server returns Success=false (logical failure) → retry ---
func TestProcessCSVAndSend_ServerSuccessFalseTriggersRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockTelemetryServiceClient(ctrl)
	csvPath := writeTestCSV(t, validCSVHeader+validCSVRow)

	gomock.InOrder(
		mockClient.EXPECT().
			SendTelemetry(gomock.Any(), gomock.Any()).
			Return(&pb.TelemetryResponse{Success: false, Message: "queue full"}, nil),
		mockClient.EXPECT().
			SendTelemetry(gomock.Any(), gomock.Any()).
			Return(&pb.TelemetryResponse{Success: true}, nil),
	)

	sent, dropped, err := processCSVAndSend(csvPath, mockClient, testRetryConfig())

	t.Logf("Result: sent=%d, dropped=%d, err=%v", sent, dropped, err)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if sent != 1 {
		t.Errorf("Expected sent=1 (succeeded on retry), got %d", sent)
	}
}

// --- Test 6: Multiple rows with mixed success/failure ---
func TestProcessCSVAndSend_MixedResults(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockTelemetryServiceClient(ctrl)

	// 3 rows in CSV
	content := validCSVHeader +
		"123,temp,0,dev1,uuid1,a,b,c,d,ns1,25.5,node=test\n" +
		"124,util,1,dev2,uuid2,a,b,c,d,ns1,80.0,node=test\n" +
		"125,power,2,dev3,uuid3,a,b,c,d,ns1,150.0,node=test\n"
	csvPath := writeTestCSV(t, content)

	cfg := testRetryConfig() // maxRetries=2
	queueFullErr := status.Error(codes.ResourceExhausted, "queue full")

	gomock.InOrder(
		// Row 1: success
		mockClient.EXPECT().SendTelemetry(gomock.Any(), gomock.Any()).
			Return(&pb.TelemetryResponse{Success: true}, nil),
		// Row 2: fails all retries (3 attempts)
		mockClient.EXPECT().SendTelemetry(gomock.Any(), gomock.Any()).
			Return(nil, queueFullErr).Times(cfg.maxRetries+1),
		// Row 3: success
		mockClient.EXPECT().SendTelemetry(gomock.Any(), gomock.Any()).
			Return(&pb.TelemetryResponse{Success: true}, nil),
	)

	sent, dropped, err := processCSVAndSend(csvPath, mockClient, cfg)

	t.Logf("Result: sent=%d, dropped=%d, err=%v", sent, dropped, err)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if sent != 2 {
		t.Errorf("Expected sent=2, got %d", sent)
	}
	if dropped != 1 {
		t.Errorf("Expected dropped=1, got %d", dropped)
	}
}

// --- Test 7: isRetryable unit test ---
func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"ResourceExhausted (queue full)", status.Error(codes.ResourceExhausted, ""), true},
		{"Unavailable", status.Error(codes.Unavailable, ""), true},
		{"DeadlineExceeded", status.Error(codes.DeadlineExceeded, ""), true},
		{"InvalidArgument (non-retryable)", status.Error(codes.InvalidArgument, ""), false},
		{"NotFound (non-retryable)", status.Error(codes.NotFound, ""), false},
		{"non-gRPC error", errors.New("plain error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Running: %s", tt.name)
			if got := isRetryable(tt.err); got != tt.expected {
				t.Errorf("Expected %v, got %v for err: %v", tt.expected, got, tt.err)
			}
		})
	}
}

// --- Test 8: computeBackoff bounded by maxBackoff ---
func TestComputeBackoff(t *testing.T) {
	cfg := retryConfig{
		initialBackoff: 100 * time.Millisecond,
		maxBackoff:     1 * time.Second,
		factor:         2.0,
	}

	for attempt := 0; attempt < 10; attempt++ {
		got := computeBackoff(attempt, cfg)
		t.Logf("Attempt %d: backoff=%v", attempt, got)
		if got > cfg.maxBackoff {
			t.Errorf("Attempt %d: backoff %v exceeded max %v", attempt, got, cfg.maxBackoff)
		}
		if got < 0 {
			t.Errorf("Attempt %d: negative backoff %v", attempt, got)
		}
	}
}
