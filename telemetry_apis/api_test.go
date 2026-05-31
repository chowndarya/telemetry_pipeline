package main

import (
	"os"
	"testing"
	"time"
)

func TestGetEnv(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    string
		fallback string
		expected string
	}{
		{"Env var set", "TEST_KEY_1", "actual_value", "fallback", "actual_value"},
		{"Env var empty", "TEST_KEY_2", "", "fallback", "fallback"},
		{"Env var not set", "TEST_KEY_NOT_EXIST", "", "default", "default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Running: %s", tt.name)
			if tt.value != "" {
				os.Setenv(tt.key, tt.value)
				defer os.Unsetenv(tt.key)
			}

			result := getEnv(tt.key, tt.fallback)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			} else {
				t.Logf("Got expected value: %s", result)
			}
		})
	}
}

func TestParseTimeOrDefault(t *testing.T) {
	defaultTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		input       string
		expectError bool
		useDefault  bool
	}{
		{"Empty string returns default", "", false, true},
		{"Valid RFC3339", "2026-05-30T10:00:00Z", false, false},
		{"Valid RFC3339 with timezone", "2026-05-30T10:00:00+05:30", false, false},
		{"Invalid format", "2026-05-30 10:00:00", true, false},
		{"Garbage input", "not-a-time", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Running: %s with input '%s'", tt.name, tt.input)
			result, err := parseTimeOrDefault(tt.input, defaultTime)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if tt.useDefault && !result.Equal(defaultTime) {
				t.Errorf("Expected default time, got %v", result)
			}
			if !tt.expectError {
				t.Logf("Parsed time: %v", result)
			}
		})
	}
}

func TestIsSafeIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"Valid alphanumeric", "gpu0", true},
		{"Valid with underscore", "gpu_id_1", true},
		{"Valid with hyphen", "gpu-001", true},
		{"Valid with dot and colon", "node1.local:0", true},
		{"Empty string", "", false},
		{"SQL injection attempt", "0\" or \"1\"=\"1", false},
		{"Flux injection attempt", "0\") |> drop()", false},
		{"Spaces not allowed", "gpu 0", false},
		{"Special chars not allowed", "gpu@0", false},
		{"Slash not allowed", "gpu/0", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Running: %s with input '%s'", tt.name, tt.input)
			result := isSafeIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			} else {
				t.Logf("Result: %v", result)
			}
		})
	}
}
