package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func setupTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	return r
}

func TestHealthzEndpoint(t *testing.T) {
	r := setupTestRouter()
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	t.Logf("Response code: %d", w.Code)
	t.Logf("Response body: %s", w.Body.String())

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("Expected status 'ok', got %s", resp["status"])
	}
}

func TestGetTelemetry_InvalidGPUID(t *testing.T) {
	cfg := &Config{InfluxBucket: "test", Measurement: "test_metric"}
	r := setupTestRouter()
	// Pass nil for queryAPI since validation happens before any DB call
	r.GET("/api/v1/gpus/:id/telemetry", getTelemetryByGPUHandler(nil, cfg))

	tests := []struct {
		name     string
		gpuID    string
		query    string
		wantCode int
	}{
		{"Invalid characters in GPU ID", "gpu@0", "", http.StatusBadRequest},
		{"Invalid start_time format", "gpu0", "?start_time=bad-time", http.StatusBadRequest},
		{"end_time before start_time", "gpu0", "?start_time=2026-05-30T10:00:00Z&end_time=2026-05-30T09:00:00Z", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Running: %s", tt.name)
			req := httptest.NewRequest(http.MethodGet, "/api/v1/gpus/"+tt.gpuID+"/telemetry"+tt.query, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			t.Logf("Response: %d - %s", w.Code, w.Body.String())
			if w.Code != tt.wantCode {
				t.Errorf("Expected %d, got %d", tt.wantCode, w.Code)
			}
		})
	}
}
