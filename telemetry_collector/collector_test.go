package main

import (
	"reflect"
	"testing"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto"
)

func TestParseLabelsRaw(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]string
	}{
		{
			name:     "Standard key=value pairs",
			input:    "host=server1,region=us-east",
			expected: map[string]string{"host": "server1", "region": "us-east"},
		},
		{
			name:     "Empty input string",
			input:    "",
			expected: map[string]string{},
		},
		{
			name:     "Whitespace handling",
			input:    " host = server1 , region = us-east ",
			expected: map[string]string{"host": "server1", "region": "us-east"},
		},
		{
			name:     "Key without value",
			input:    "standalone_tag",
			expected: map[string]string{"standalone_tag": ""},
		},
		{
			name:     "Mixed pairs and empty entries",
			input:    "a=1,,b=2",
			expected: map[string]string{"a": "1", "b": "2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Running: %s", tt.name)
			result := parseLabelsRaw(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			} else {
				t.Logf("Successfully parsed labels: %v", result)
			}
		})
	}
}

func TestBuildPointFromTelemetry(t *testing.T) {
	msg := &pb.TelemetryRequest{
		Timestamp:  1700000000000000000,
		MetricName: "gpu_temp",
		GpuId:      "0",
		Device:     "nvidia0",
		Uuid:       "uuid-123",
		ModelName:  []string{"A100", "PCIe"},
		Namespace:  "ai-prod",
		Value:      75.5,
		LabelsRaw:  "rack=R1,zone=z2",
	}

	table, tags, fields, ts := buildPointFromTelemetry(msg, "gpu_metrics")

	t.Logf("Table: %s", table)
	t.Logf("Tags: %v", tags)
	t.Logf("Fields: %v", fields)
	t.Logf("Timestamp: %v", ts)

	if table != "gpu_metrics" {
		t.Errorf("Expected table 'gpu_metrics', got %s", table)
	}
	if tags["gpu_id"] != "0" || tags["rack"] != "R1" || tags["modelName"] != "A100,PCIe" {
		t.Errorf("Tags not built correctly: %v", tags)
	}
	if fields["value"].(float64) != 75.5 {
		t.Errorf("Expected value 75.5, got %v", fields["value"])
	}
}
