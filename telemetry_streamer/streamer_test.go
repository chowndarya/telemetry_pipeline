package main

import (
	"testing"
)

func TestMapRecordToRequest(t *testing.T) {
	tests := []struct {
		name     string
		record   []string
		hostname string
		wantErr  bool
	}{
		{
			name:     "Valid Record Processing",
			record:   []string{"t", "m1", "g1", "d1", "u1", "m1", "m2", "m3", "m4", "ns1", "10.5", "label1"},
			hostname: "test-host",
			wantErr:  false,
		},
		{
			name:     "Incomplete Record Handling",
			record:   []string{"t", "m1"},
			hostname: "test-host",
			wantErr:  true,
		},
		{
			name:     "Invalid Float Value Handling",
			record:   []string{"t", "m1", "g1", "d1", "u1", "m1", "m2", "m3", "m4", "ns1", "NaN", "label1"},
			hostname: "test-host",
			wantErr:  true,
		},
		{
			name:     "Invalid Non-Numeric Value Handling",
			record:   []string{"t", "m1", "g1", "d1", "u1", "m1", "m2", "m3", "m4", "ns1", "abc", "label1"},
			hostname: "test-host",
			wantErr:  true,
		},
		{
			name:     "NaN Value Handling",
			record:   []string{"t", "m1", "g1", "d1", "u1", "m1", "m2", "m3", "m4", "ns1", "NaN", "label1"},
			hostname: "test-host",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Running test case: %s", tt.name)

			req, err := mapRecordToRequest(tt.record, tt.hostname)

			if (err != nil) != tt.wantErr {
				t.Errorf("Test '%s' failed: expected error = %v, got error = %v", tt.name, tt.wantErr, err)
				return
			}

			if !tt.wantErr {
				t.Logf("Successfully mapped record for: %s. Value: %f", tt.name, req.Value)
			} else {
				t.Logf("Successfully caught expected error for: %s", tt.name)
			}
		})
	}
}
