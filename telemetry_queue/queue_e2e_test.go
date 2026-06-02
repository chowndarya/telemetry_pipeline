package main

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStream implements pb.TelemetryService_CollectTelemetryServer for testing
type fakeStream struct {
	pb.TelemetryService_CollectTelemetryServer
	ctx      context.Context
	received []*pb.TelemetryRequest
	mu       sync.Mutex
	sendErr  error
	maxSends int // stop after this many sends; 0 = unlimited
}

func (f *fakeStream) Send(req *pb.TelemetryRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return f.sendErr
	}
	f.received = append(f.received, req)
	if f.maxSends > 0 && len(f.received) >= f.maxSends {
		// Cancel context to stop the loop
		if cancel, ok := f.ctx.Value("cancel").(context.CancelFunc); ok {
			cancel()
		}
	}
	return nil
}

func (f *fakeStream) Context() context.Context {
	return f.ctx
}

func TestE2E_EnqueueDispatchAck(t *testing.T) {
	q := newTestQueue(t, 10, 30*time.Second)

	// Enqueue a message
	_, err := q.SendTelemetry(context.Background(), sampleReq(1, "e2e-metric"))
	require.NoError(t, err)

	// Set up fake collector stream
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, "cancel", cancel)
	stream := &fakeStream{ctx: ctx, maxSends: 1}

	// Run CollectTelemetry in background; it will exit when stream context is cancelled
	done := make(chan error, 1)
	go func() {
		done <- q.CollectTelemetry(&pb.TelemetryRequest{}, stream)
	}()

	// Wait for dispatch
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("CollectTelemetry did not return within 2s")
	}

	// Verify dispatch
	require.Len(t, stream.received, 1)
	dispatchedID := stream.received[0].MessageId
	assert.NotEmpty(t, dispatchedID, "MessageId should be assigned at dispatch")
	assert.Equal(t, "e2e-metric", stream.received[0].MetricName)

	// Verify it's in pending
	q.mu.Lock()
	assert.Equal(t, 1, len(q.pending))
	q.mu.Unlock()

	// Ack it
	resp, err := q.AckTelemetry(context.Background(), &pb.AckRequest{MessageId: dispatchedID})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	// Pending should be empty now
	q.mu.Lock()
	assert.Equal(t, 0, len(q.pending))
	assert.Equal(t, uint64(1), q.ackedCount)
	q.mu.Unlock()
}
