package main

import (
	"os"
	"testing"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto"
	"github.com/chowndarya/telemetry_pipeline/mocks" // Adjust path as needed
	"go.uber.org/mock/gomock"
)

func TestProcessCSVAndSend(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockTelemetryServiceClient(ctrl)

	// 1. Create a temporary CSV file for testing
	content := "timestamp,metric_name,gpu_id,device,uuid,m1,m2,m3,m4,ns,value,labels\n" +
		"123,temp,0,dev1,uuid1,a,b,c,d,ns1,25.5,node=test"
	tmpFile, _ := os.CreateTemp("", "test.csv")
	defer os.Remove(tmpFile.Name())
	tmpFile.WriteString(content)
	tmpFile.Close()

	// 2. Expect the gRPC call to be made exactly once with specific parameters
	mockClient.EXPECT().
		SendTelemetry(gomock.Any(), gomock.Any()).
		Return(&pb.TelemetryResponse{}, nil).
		Times(1)

	// 3. Run the function
	err := processCSVAndSend(tmpFile.Name(), mockClient)

	if err != nil {
		t.Errorf("processCSVAndSend failed: %v", err)
	}
}
