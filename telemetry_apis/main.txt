package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	influxdb3 "github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/apache/arrow/go/v15/arrow"
	_ "github.com/chowndarya/telemetry_pipeline/telemetry_apis/docs"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// @title GPU Telemetry API
// @version 1.0
// @description API to query GPU telemetry data stored in InfluxDB 3.
// @contact.name API Support
// @contact.email support@example.com
// @host localhost:8080
// @BasePath /api/v1

// TelemetryEntry represents a telemetry data point
type TelemetryEntry struct {
	Timestamp  time.Time `json:"timestamp"`
	MetricName string    `json:"metric_name"`
	GpuID      string    `json:"gpu_id"`
	Device     string    `json:"device"`
	Value      float64   `json:"value"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}

// arrowTimestampToTime converts an arrow.Timestamp to time.Time
// Arrow timestamps in InfluxDB 3 are in nanoseconds precision
func arrowTimestampToTime(ts arrow.Timestamp) time.Time {
	// arrow.Timestamp is an int64 representing nanoseconds since Unix epoch
	return time.Unix(0, int64(ts)).UTC()
}

func main() {
	// InfluxDB 3 connection parameters
	influxURL := "http://localhost:8181"
	influxToken := "apiv3_Y6QmczU2nRBzBMUFz9WMDkZc_S6PlzTe8Fs2OF2wg-uzjJmhRAqCLkQw8PEfuyO-NZm5y2dNsDpzIT0qRTVsUw"
	influxDatabase := "tel_db"

	// Create InfluxDB 3 client
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     influxURL,
		Token:    influxToken,
		Database: influxDatabase,
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create InfluxDB 3 client: %v", err))
	}
	defer client.Close()

	r := gin.Default()

	// Swagger endpoint
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// API routes
	apiGroup := r.Group("/api/v1")
	{
		apiGroup.GET("/gpus", listGPUsHandler(client, influxDatabase))
		apiGroup.GET("/gpus/:id/telemetry", getTelemetryByGPUHandler(client, influxDatabase))
	}

	r.Run(":8080")
}

// listGPUsHandler returns a list of unique GPU IDs
// @Summary List GPUs
// @Description Get list of unique GPU IDs from telemetry data
// @Tags GPUs
// @Produce json
// @Success 200 {array} string
// @Failure 500 {object} ErrorResponse
// @Router /gpus [get]
func listGPUsHandler(client *influxdb3.Client, database string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// SQL query to get distinct GPU IDs
		query := `SELECT DISTINCT gpu_id FROM gpu_metrics WHERE gpu_id IS NOT NULL`

		iterator, err := client.Query(context.Background(), query)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}

		var gpuIDs []string
		for iterator.Next() {
			value := iterator.Value()
			if gpuID, ok := value["gpu_id"].(string); ok && gpuID != "" {
				gpuIDs = append(gpuIDs, gpuID)
			}
		}

		c.JSON(http.StatusOK, gpuIDs)
	}
}

// getTelemetryByGPUHandler returns telemetry entries for a specific GPU with optional time filters
// @Summary Get telemetry by GPU ID
// @Description Get telemetry data for a specific GPU ordered by time, optionally filtered by start_time and end_time (RFC3339 format)
// @Tags GPUs
// @Produce json
// @Param id path string true "GPU ID"
// @Param start_time query string false "Start time (inclusive), RFC3339 format e.g. 2026-01-01T00:00:00Z"
// @Param end_time query string false "End time (inclusive), RFC3339 format e.g. 2026-12-31T23:59:59Z"
// @Success 200 {array} TelemetryEntry
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /gpus/{id}/telemetry [get]
func getTelemetryByGPUHandler(client *influxdb3.Client, database string) gin.HandlerFunc {
	return func(c *gin.Context) {
		gpuID := c.Param("id")
		startTimeStr := c.Query("start_time")
		endTimeStr := c.Query("end_time")

		// Build SQL query with optional time filters
		query := fmt.Sprintf(`
			SELECT
				time,
				metric_name,
				gpu_id,
				device,
				value
			FROM gpu_metrics
			WHERE gpu_id = '%s'`, gpuID)

		// Add optional time filters
		if startTimeStr != "" {
			// Validate RFC3339 format
			if _, err := time.Parse(time.RFC3339, startTimeStr); err != nil {
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Error: "Invalid start_time format, must be RFC3339 e.g. 2026-01-01T00:00:00Z",
				})
				return
			}
			query += fmt.Sprintf(` AND time >= '%s'`, startTimeStr)
		}

		if endTimeStr != "" {
			// Validate RFC3339 format
			if _, err := time.Parse(time.RFC3339, endTimeStr); err != nil {
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Error: "Invalid end_time format, must be RFC3339 e.g. 2026-12-31T23:59:59Z",
				})
				return
			}
			query += fmt.Sprintf(` AND time <= '%s'`, endTimeStr)
		}

		query += ` ORDER BY time ASC`

		fmt.Println(query)

		iterator, err := client.Query(context.Background(), query)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}

		var telemetry []TelemetryEntry
		for iterator.Next() {
			rec := iterator.Value()

			// Debug: print all fields and their types
			//for k, v := range rec {
			//	fmt.Printf("Key: %s, Value: %v, Type: %T\n", k, v, v)
			//}

			// Safely extract each field from the record map
			// Handle arrow.Timestamp type for time field
			var ts time.Time
			if t, ok := rec["time"].(arrow.Timestamp); ok {
				ts = arrowTimestampToTime(t)
			} else {
				fmt.Printf("Unexpected time type: %T value: %v\n", rec["time"], rec["time"])
				ts = time.Now().UTC()
			}

			metricName, _ := rec["metric_name"].(string)
			gpuIDVal, _ := rec["gpu_id"].(string)
			device, _ := rec["device"].(string)
			value, _ := rec["value"].(float64)

			entry := TelemetryEntry{
				Timestamp:  ts,
				MetricName: metricName,
				GpuID:      gpuIDVal,
				Device:     device,
				Value:      value,
			}
			telemetry = append(telemetry, entry)
		}

		c.JSON(http.StatusOK, telemetry)
	}
}
