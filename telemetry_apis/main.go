package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/chowndarya/telemetry_pipeline/telemetry_apis/docs"
	"github.com/gin-gonic/gin"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// @title GPU Telemetry API
// @version 1.0
// @description API to query GPU telemetry data stored in InfluxDB 2.
// @contact.name API Support
// @contact.email support@example.com
// @host localhost:8080
// @BasePath /api/v1

// TelemetryEntry represents a telemetry data point
/*type TelemetryEntry struct {
	Timestamp  time.Time `json:"timestamp"`
	MetricName string    `json:"metric_name"`
	GpuID      string    `json:"gpu_id"`
	Device     string    `json:"device"`
	Value      float64   `json:"value"`
}*/

// TelemetryEntry represents a telemetry data point with all columns.
// Uses a flexible map so any tag/field column from InfluxDB shows up automatically.
type TelemetryEntry map[string]interface{}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}

// Config holds runtime configuration
type Config struct {
	InfluxURL    string
	InfluxToken  string
	InfluxOrg    string
	InfluxBucket string
	Measurement  string
	ServerPort   string
}

func loadConfig() *Config {
	cfg := &Config{
		InfluxURL:    getEnv("INFLUXDB_URL", "http://localhost:8181"),
		InfluxToken:  os.Getenv("INFLUXDB_TOKEN"),
		InfluxOrg:    os.Getenv("INFLUXDB_ORG"),
		InfluxBucket: getEnv("INFLUXDB_BUCKET", "tel_db"),
		Measurement:  getEnv("INFLUXDB_MEASUREMENT", "gpu_metrics"),
		ServerPort:   getEnv("SERVER_PORT", "8080"),
	}

	// Fail fast on missing required config — no silent fallbacks
	if cfg.InfluxToken == "" {
		log.Fatal("INFLUXDB_TOKEN environment variable not set")
	}
	if cfg.InfluxOrg == "" {
		log.Fatal("INFLUXDB_ORG environment variable not set")
	}

	log.Printf("Config: URL=%s, Org=%s, Bucket=%s, Measurement=%s, Port=%s",
		cfg.InfluxURL, cfg.InfluxOrg, cfg.InfluxBucket, cfg.Measurement, cfg.ServerPort)

	return cfg
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// parseTimeOrDefault parses an RFC3339 timestamp; returns the default if the input is empty.
func parseTimeOrDefault(s string, def time.Time) (time.Time, error) {
	if s == "" {
		return def, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

// isSafeIdentifier returns true if s contains only safe characters for use
// inside a Flux string literal (defends against injection in dynamic queries).
func isSafeIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_', r == '.', r == ':':
			continue
		default:
			return false
		}
	}
	return true
}

func main() {
	cfg := loadConfig()

	// Create InfluxDB 2 client
	client := influxdb2.NewClient(cfg.InfluxURL, cfg.InfluxToken)
	defer client.Close()

	queryAPI := client.QueryAPI(cfg.InfluxOrg)

	// Health check at startup — fail loud if InfluxDB unreachable
	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	/*if health, err := client.Health(ctx); err != nil {
		log.Fatalf("InfluxDB health check failed: %v", err)
	} else {
		log.Printf("InfluxDB health: %s", health.Status)
	}*/

	r := gin.Default()

	// Swagger endpoint
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// Liveness/readiness for k8s probes
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// API routes
	apiGroup := r.Group("/api/v1")
	{
		apiGroup.GET("/gpus", listGPUsHandler(queryAPI, cfg))
		apiGroup.GET("/gpus/:id/telemetry", getTelemetryByGPUHandler(queryAPI, cfg))
	}

	addr := "0.0.0.0:" + cfg.ServerPort
	log.Printf("Starting API server on %s", addr)
	if err := r.Run(addr); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// listGPUsHandler returns a list of unique GPU IDs
// @Summary List GPUs
// @Description Get list of unique GPU IDs from telemetry data
// @Tags GPUs
// @Produce json
// @Success 200 {array} string
// @Failure 500 {object} ErrorResponse
// @Router /gpus [get]
func listGPUsHandler(queryAPI api.QueryAPI, cfg *Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		flux := fmt.Sprintf(`
            from(bucket: "%s")
              |> range(start: -30d)
              |> filter(fn: (r) => r._measurement == "%s")
              |> keep(columns: ["gpu_id"])
              |> distinct(column: "gpu_id")
        `, cfg.InfluxBucket, cfg.Measurement)

		result, err := queryAPI.Query(c.Request.Context(), flux)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}
		defer result.Close()

		gpuIDs := []string{}
		for result.Next() {
			if v, ok := result.Record().ValueByKey("gpu_id").(string); ok && v != "" {
				gpuIDs = append(gpuIDs, v)
			}
		}
		if result.Err() != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: result.Err().Error()})
			return
		}

		c.JSON(http.StatusOK, gpuIDs)
	}
}

// getTelemetryByGPUHandler returns telemetry for a specific GPU within a time window.
// Returns ALL columns (tags + fields) present on each point.
// @Summary Get GPU Telemetry
// @Description Get telemetry data for a specific GPU between start_time and end_time (RFC3339).
// @Description Returns all available tag and field columns dynamically.
// @Tags GPUs
// @Produce json
// @Param id path string true "GPU ID"
// @Param start_time query string false "Start time in RFC3339 (e.g. 2026-05-30T10:00:00Z)"
// @Param end_time query string false "End time in RFC3339 (e.g. 2026-05-30T11:00:00Z)"
// @Success 200 {array} TelemetryEntry
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /gpus/{id}/telemetry [get]
func getTelemetryByGPUHandler(queryAPI api.QueryAPI, cfg *Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		gpuID := c.Param("id")
		if gpuID == "" {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: "gpu id is required"})
			return
		}
		if !isSafeIdentifier(gpuID) {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Error: "gpu id contains invalid characters",
			})
			return
		}

		// Time window — default to last 1 hour
		now := time.Now().UTC()
		startTime, err := parseTimeOrDefault(c.Query("start_time"), now.Add(-1*time.Hour))
		if err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Error: fmt.Sprintf("invalid start_time: %v (expected RFC3339)", err),
			})
			return
		}
		endTime, err := parseTimeOrDefault(c.Query("end_time"), now)
		if err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Error: fmt.Sprintf("invalid end_time: %v (expected RFC3339)", err),
			})
			return
		}
		if !endTime.After(startTime) {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Error: "end_time must be after start_time",
			})
			return
		}

		// pivot() merges separate _field rows (metric_name, value) into a single row
		// with one column per field. This way the response has the natural shape:
		//   { timestamp, measurement, gpu_id, device, uuid, namespace, modelName,
		//     <any labels_raw keys>, metric_name, value }
		flux := fmt.Sprintf(`
            from(bucket: "%s")
              |> range(start: %s, stop: %s)
              |> filter(fn: (r) => r._measurement == "%s")
              |> filter(fn: (r) => r.gpu_id == "%s")
              |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        `,
			cfg.InfluxBucket,
			startTime.Format(time.RFC3339Nano),
			endTime.Format(time.RFC3339Nano),
			cfg.Measurement,
			gpuID,
		)

		result, err := queryAPI.Query(c.Request.Context(), flux)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}
		defer result.Close()

		// Internal Flux/system columns we don't want to leak in responses
		systemCols := map[string]struct{}{
			"_start": {},
			"_stop":  {},
			"_time":  {},
			"result": {},
			"table":  {},
		}

		entries := []TelemetryEntry{}
		for result.Next() {
			rec := result.Record()
			values := rec.Values() // map[string]interface{} of every column on this record

			entry := TelemetryEntry{
				"timestamp":   rec.Time(),
				"measurement": rec.Measurement(),
			}

			// Copy every non-system column into the response.
			// This includes all tags (gpu_id, device, uuid, namespace, modelName,
			// plus dynamic keys from labels_raw) and all pivoted fields
			// (metric_name, value, plus anything you add later).
			for k, v := range values {
				if _, skip := systemCols[k]; skip {
					continue
				}
				// Avoid clobbering "measurement" we already set
				if k == "_measurement" {
					continue
				}
				entry[k] = v
			}

			entries = append(entries, entry)
		}
		if result.Err() != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: result.Err().Error()})
			return
		}

		c.JSON(http.StatusOK, entries)
	}
}
