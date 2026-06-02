package main

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// shutdownQueue performs an idempotent shutdown of the queue.
// Safe to call multiple times.
func shutdownQueue(q *TelemetryQueueServer) {
	q.mu.Lock()
	alreadyClosed := q.closed
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()

	if !alreadyClosed {
		close(q.stopRequeue)
		q.wg.Wait()
	}

	if q.db != nil {
		_ = q.db.Close()
	}
}

// helper: build a sample telemetry request
func sampleReq(ts int64, metric string) *pb.TelemetryRequest {
	return &pb.TelemetryRequest{
		Timestamp:  ts,
		MetricName: metric,
		GpuId:      "gpu0",
		Device:     "nvidia0",
		Uuid:       "uuid-test",
		ModelName:  []string{"A100"},
		Namespace:  "ns",
		Value:      42.0,
		LabelsRaw:  "host=test",
	}
}

func newTestQueue(t *testing.T, maxSize int, ackTimeout time.Duration) *TelemetryQueueServer {
	t.Helper()
	q := NewTelemetryQueueServer(maxSize, ackTimeout)
	t.Cleanup(func() {
		q.mu.Lock()
		alreadyClosed := q.closed
		q.closed = true
		q.cond.Broadcast()
		q.mu.Unlock()

		if !alreadyClosed {
			close(q.stopRequeue)
			q.wg.Wait()
		}
	})
	return q
}

// ============================================================
// Backpressure Tests
// ============================================================

func TestSendTelemetry_Success(t *testing.T) {
	q := newTestQueue(t, 10, 30*time.Second)

	resp, err := q.SendTelemetry(context.Background(), sampleReq(1, "temp"))

	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "enqueued", resp.Message)
	assert.Equal(t, uint64(1), q.enqueuedCount)
	assert.Equal(t, 1, len(q.messages))
}

func TestSendTelemetry_NilRequest(t *testing.T) {
	q := newTestQueue(t, 10, 30*time.Second)

	resp, err := q.SendTelemetry(context.Background(), nil)

	require.Error(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSendTelemetry_QueueFull_ReturnsResourceExhausted(t *testing.T) {
	q := newTestQueue(t, 3, 30*time.Second) // tiny queue

	// Fill the queue exactly to capacity
	for i := 0; i < 3; i++ {
		resp, err := q.SendTelemetry(context.Background(), sampleReq(int64(i), "metric"))
		require.NoError(t, err)
		require.True(t, resp.Success)
	}

	// Next send must be rejected with ResourceExhausted
	resp, err := q.SendTelemetry(context.Background(), sampleReq(99, "overflow"))

	require.Error(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.Equal(t, "queue full", resp.Message)
	assert.Equal(t, uint64(1), q.rejectedCount)
	t.Logf("Rejected count: %d, queue size: %d", q.rejectedCount, len(q.messages))
}

func TestSendTelemetry_AfterClose_ReturnsUnavailable(t *testing.T) {
	q := newTestQueue(t, 10, 30*time.Second)

	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()

	resp, err := q.SendTelemetry(context.Background(), sampleReq(1, "metric"))

	require.Error(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestSendTelemetry_ConcurrentProducers(t *testing.T) {
	q := newTestQueue(t, 100, 30*time.Second)

	var wg sync.WaitGroup
	const numProducers = 10
	const messagesPerProducer = 10

	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < messagesPerProducer; i++ {
				_, err := q.SendTelemetry(context.Background(),
					sampleReq(int64(pid*100+i), "concurrent"))
				assert.NoError(t, err)
			}
		}(p)
	}
	wg.Wait()

	assert.Equal(t, uint64(numProducers*messagesPerProducer), q.enqueuedCount)
	assert.Equal(t, numProducers*messagesPerProducer, len(q.messages))
	t.Logf("Enqueued %d messages from %d concurrent producers", q.enqueuedCount, numProducers)
}

// ============================================================
// At-Least-Once Delivery Tests
// ============================================================

func TestAckTelemetry_Success(t *testing.T) {
	q := newTestQueue(t, 10, 30*time.Second)

	// Manually inject a pending message (simulate dispatch)
	msgID := "test-msg-1"
	q.mu.Lock()
	q.pending[msgID] = pendingMessage{
		msg:        message{MessageId: msgID, Timestamp: 100},
		sentAt:     time.Now(),
		deliveries: 1,
	}
	q.mu.Unlock()

	resp, err := q.AckTelemetry(context.Background(), &pb.AckRequest{MessageId: msgID})

	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(1), q.ackedCount)
	assert.Equal(t, 0, len(q.pending))
}

func TestAckTelemetry_UnknownMessageID_Idempotent(t *testing.T) {
	q := newTestQueue(t, 10, 30*time.Second)

	resp, err := q.AckTelemetry(context.Background(), &pb.AckRequest{MessageId: "ghost"})

	// Idempotent: unknown acks should not fail
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(0), q.ackedCount)
}

func TestAckTelemetry_EmptyMessageID(t *testing.T) {
	q := newTestQueue(t, 10, 30*time.Second)

	resp, err := q.AckTelemetry(context.Background(), &pb.AckRequest{MessageId: ""})

	require.Error(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestRequeueOne_PutsMessageBackInQueue(t *testing.T) {
	q := newTestQueue(t, 10, 30*time.Second)

	msgID := "msg-to-requeue"
	q.mu.Lock()
	q.pending[msgID] = pendingMessage{
		msg:        message{MessageId: msgID, Timestamp: 200, MetricName: "temp"},
		sentAt:     time.Now(),
		deliveries: 1,
	}
	q.mu.Unlock()

	q.requeueOne(msgID)

	q.mu.Lock()
	defer q.mu.Unlock()
	assert.Equal(t, 0, len(q.pending))
	assert.Equal(t, 1, len(q.messages))
	assert.Equal(t, "", q.messages[0].MessageId, "MessageId should be cleared on requeue")
	assert.Equal(t, "temp", q.messages[0].MetricName)
	assert.Equal(t, uint64(1), q.requeuedCount)
}

func TestScanAndRequeue_TimeoutTriggersRequeue(t *testing.T) {
	// Use very short ack timeout for test
	q := newTestQueue(t, 10, 50*time.Millisecond)

	// Inject pending message that's already expired
	msgID := "expired-msg"
	q.mu.Lock()
	q.pending[msgID] = pendingMessage{
		msg:        message{MessageId: msgID, Timestamp: 300},
		sentAt:     time.Now().Add(-1 * time.Second), // expired
		deliveries: 1,
	}
	q.mu.Unlock()

	q.scanAndRequeue()

	q.mu.Lock()
	defer q.mu.Unlock()
	assert.Equal(t, 0, len(q.pending), "expired message should be removed from pending")
	assert.Equal(t, 1, len(q.messages), "expired message should be back in queue")
	assert.Equal(t, uint64(1), q.requeuedCount)
}

func TestScanAndRequeue_FreshMessagesNotRequeued(t *testing.T) {
	q := newTestQueue(t, 10, 30*time.Second)

	msgID := "fresh-msg"
	q.mu.Lock()
	q.pending[msgID] = pendingMessage{
		msg:        message{MessageId: msgID},
		sentAt:     time.Now(), // just now
		deliveries: 1,
	}
	q.mu.Unlock()

	q.scanAndRequeue()

	q.mu.Lock()
	defer q.mu.Unlock()
	assert.Equal(t, 1, len(q.pending), "fresh messages should not be requeued")
	assert.Equal(t, 0, len(q.messages))
	assert.Equal(t, uint64(0), q.requeuedCount)
}
