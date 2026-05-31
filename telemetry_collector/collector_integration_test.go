package main

import (
	"io"
	"testing"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto"
	"github.com/chowndarya/telemetry_pipeline/mocks"
	"go.uber.org/mock/gomock"
)

func TestCollectorStreamReceive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Specify the response type as TelemetryRequest
	mockStream := mocks.NewMockTelemetryService_CollectTelemetryClient[pb.TelemetryRequest](ctrl)

	gomock.InOrder(
		mockStream.EXPECT().Recv().Return(&pb.TelemetryRequest{
			MetricName: "gpu_temp",
			GpuId:      "0",
			Value:      72.5,
			LabelsRaw:  "host=node1",
		}, nil),
		mockStream.EXPECT().Recv().Return(&pb.TelemetryRequest{
			MetricName: "gpu_util",
			GpuId:      "1",
			Value:      88.0,
			LabelsRaw:  "host=node2",
		}, nil),
		mockStream.EXPECT().Recv().Return(nil, io.EOF),
	)

	count := 0
	for {
		msg, err := mockStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, tags, fields, _ := buildPointFromTelemetry(msg, "gpu_metrics")
		t.Logf("Received message #%d: tags=%v fields=%v", count+1, tags, fields)
		count++
	}

	if count != 2 {
		t.Errorf("Expected to process 2 messages, got %d", count)
	}
}
