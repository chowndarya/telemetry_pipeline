package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Stress Test Configuration
// ============================================================

const (
	stressNumProducers    = 10
	stressNumCollectors   = 10
	stressMessagesPerProd = 100 // 10 producers × 100 = 1000 total messages
	stressQueueCapacity   = 200 // intentionally small to force backpressure
	stressAckTimeout      = 500 * time.Millisecond
	stressFailureRate     = 0.10 // 10% of collectors fail to ack (simulating crashes)
)

// ============================================================
// Mock Collector Stream
// ============================================================

// stressStream is a fake bidirectional stream that captures dispatched messages
// and supports simulated failures.
type stressStream struct {
	pb.TelemetryService_CollectTelemetryServer
	ctx      context.Context
	received chan *pb.TelemetryRequest
	closed   atomic.Bool
}

func newStressStream(ctx context.Context, bufSize int) *stressStream {
	return &stressStream{
		ctx:      ctx,
		received: make(chan *pb.TelemetryRequest, bufSize),
	}
}

func (s *stressStream) Send(req *pb.TelemetryRequest) error {
	if s.closed.Load() {
		return fmt.Errorf("stream closed")
	}
	select {
	case s.received <- req:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *stressStream) Context() context.Context {
	return s.ctx
}

func (s *stressStream) Close() {
	s.closed.Store(true)
}

// ============================================================
// Stress Test 1: 10 Producers + 10 Collectors, No Failures
// ============================================================

func TestStress_TenProducersTenCollectors_NoFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in -short mode")
	}

	q := newTestQueue(t, stressQueueCapacity, stressAckTimeout)

	totalMessages := stressNumProducers * stressMessagesPerProd
	var (
		dispatchedCount atomic.Uint64
		ackedCount      atomic.Uint64
		seen            sync.Map // messageId -> true (track unique deliveries for at-least-once)
	)

	// --- Start collectors ---
	collectorCtx, cancelCollectors := context.WithCancel(context.Background())
	defer cancelCollectors()

	collectorWg := sync.WaitGroup{}
	streams := make([]*stressStream, stressNumCollectors)

	for c := 0; c < stressNumCollectors; c++ {
		stream := newStressStream(collectorCtx, totalMessages)
		streams[c] = stream

		collectorWg.Add(1)
		go func(s *stressStream, id int) {
			defer collectorWg.Done()
			err := q.CollectTelemetry(&pb.TelemetryRequest{}, s)
			t.Logf("Collector %d exited: %v", id, err)
		}(stream, c)
	}

	// --- Start ack workers (one per collector stream) ---
	ackWg := sync.WaitGroup{}
	for c := 0; c < stressNumCollectors; c++ {
		ackWg.Add(1)
		go func(s *stressStream) {
			defer ackWg.Done()
			for {
				select {
				case msg := <-s.received:
					if msg == nil {
						return
					}
					dispatchedCount.Add(1)
					seen.Store(msg.MessageId, true)
					_, err := q.AckTelemetry(context.Background(),
						&pb.AckRequest{MessageId: msg.MessageId})
					if err == nil {
						ackedCount.Add(1)
					}
				case <-collectorCtx.Done():
					return
				}
			}
		}(streams[c])
	}

	// --- Start producers ---
	producerWg := sync.WaitGroup{}
	for p := 0; p < stressNumProducers; p++ {
		producerWg.Add(1)
		go func(pid int) {
			defer producerWg.Done()
			for i := 0; i < stressMessagesPerProd; i++ {
				ts := int64(pid*stressMessagesPerProd + i)
				// Retry on backpressure (simulating real streamer behavior)
				for {
					_, err := q.SendTelemetry(context.Background(), sampleReq(ts, "stress"))
					if err == nil {
						break
					}
					time.Sleep(5 * time.Millisecond) // backoff
				}
			}
		}(p)
	}

	producerWg.Wait()
	t.Logf("All producers done. Enqueued: %d, Rejected: %d", q.enqueuedCount, q.rejectedCount)

	// --- Wait for all messages to be acked ---
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if ackedCount.Load() >= uint64(totalMessages) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancelCollectors()
	collectorWg.Wait()
	ackWg.Wait()

	// --- Assertions ---
	t.Logf("=== Final Stats ===")
	t.Logf("Total messages: %d", totalMessages)
	t.Logf("Enqueued:       %d", q.enqueuedCount)
	t.Logf("Rejected:       %d (retried by producer)", q.rejectedCount)
	t.Logf("Dispatched:     %d", dispatchedCount.Load())
	t.Logf("Acked:          %d", ackedCount.Load())
	t.Logf("Requeued:       %d", q.requeuedCount)

	assert.Equal(t, uint64(totalMessages), q.enqueuedCount,
		"all messages should eventually be enqueued (with retry)")
	assert.GreaterOrEqual(t, ackedCount.Load(), uint64(totalMessages),
		"every message must be acked at least once")

	// Verify at-least-once: count unique message IDs that were seen
	uniqueCount := 0
	seen.Range(func(k, v interface{}) bool {
		uniqueCount++
		return true
	})
	assert.Equal(t, totalMessages, uniqueCount,
		"every unique message should have been dispatched")
}

// ============================================================
// Stress Test 2: With Random Collector Failures (Ack Drops)
// ============================================================

func TestStress_WithRandomAckFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in -short mode")
	}

	q := newTestQueue(t, stressQueueCapacity, stressAckTimeout)

	totalMessages := stressNumProducers * stressMessagesPerProd
	var (
		dispatchedCount atomic.Uint64
		ackedCount      atomic.Uint64
		droppedAcks     atomic.Uint64
		uniqueAcked     sync.Map
	)

	collectorCtx, cancelCollectors := context.WithCancel(context.Background())
	defer cancelCollectors()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var rngMu sync.Mutex // rand.Rand is not thread-safe

	// --- Start collectors with simulated ack failures ---
	collectorWg := sync.WaitGroup{}
	ackWg := sync.WaitGroup{}
	for c := 0; c < stressNumCollectors; c++ {
		stream := newStressStream(collectorCtx, totalMessages)

		collectorWg.Add(1)
		go func(s *stressStream, id int) {
			defer collectorWg.Done()
			q.CollectTelemetry(&pb.TelemetryRequest{}, s)
		}(stream, c)

		ackWg.Add(1)
		go func(s *stressStream) {
			defer ackWg.Done()
			for {
				select {
				case msg := <-s.received:
					if msg == nil {
						return
					}
					dispatchedCount.Add(1)

					// Random failure: drop ack with stressFailureRate probability
					rngMu.Lock()
					shouldDrop := rng.Float64() < stressFailureRate
					rngMu.Unlock()

					if shouldDrop {
						droppedAcks.Add(1)
						// Simulate collector crash: don't ack — let timeout requeue it
						continue
					}

					_, err := q.AckTelemetry(context.Background(),
						&pb.AckRequest{MessageId: msg.MessageId})
					if err == nil {
						ackedCount.Add(1)
						uniqueAcked.Store(msg.Timestamp, true) // dedup by timestamp
					}
				case <-collectorCtx.Done():
					return
				}
			}
		}(stream)
	}

	// --- Producers ---
	producerWg := sync.WaitGroup{}
	for p := 0; p < stressNumProducers; p++ {
		producerWg.Add(1)
		go func(pid int) {
			defer producerWg.Done()
			for i := 0; i < stressMessagesPerProd; i++ {
				ts := int64(pid*stressMessagesPerProd + i)
				for {
					_, err := q.SendTelemetry(context.Background(), sampleReq(ts, "stress-fail"))
					if err == nil {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(p)
	}

	producerWg.Wait()

	// --- Wait for ack timeouts to trigger requeues, then re-acks ---
	// We expect: dropped messages get requeued after ackTimeout, then re-dispatched and acked
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		uniqueCount := 0
		uniqueAcked.Range(func(k, v interface{}) bool {
			uniqueCount++
			return true
		})
		if uniqueCount >= totalMessages {
			break
		}
		// Trigger requeue scan manually for faster test
		q.scanAndRequeue()
		time.Sleep(100 * time.Millisecond)
	}

	cancelCollectors()
	collectorWg.Wait()
	ackWg.Wait()

	// --- Assertions ---
	uniqueCount := 0
	uniqueAcked.Range(func(k, v interface{}) bool {
		uniqueCount++
		return true
	})

	t.Logf("=== Final Stats ===")
	t.Logf("Total messages:    %d", totalMessages)
	t.Logf("Enqueued:          %d", q.enqueuedCount)
	t.Logf("Dispatched:        %d", dispatchedCount.Load())
	t.Logf("Acked:             %d", ackedCount.Load())
	t.Logf("Dropped acks:      %d (simulated failures)", droppedAcks.Load())
	t.Logf("Requeued:          %d", q.requeuedCount)
	t.Logf("Unique acked:      %d", uniqueCount)

	// AT-LEAST-ONCE GUARANTEE: every unique message must have been acked at least once
	assert.Equal(t, totalMessages, uniqueCount,
		"at-least-once delivery: every message must be acked at least once despite failures")

	// Some messages should have been redelivered (dispatched > total)
	assert.Greater(t, dispatchedCount.Load(), uint64(totalMessages),
		"dropped acks should cause redelivery")

	// Requeue counter should be approximately equal to dropped acks
	assert.GreaterOrEqual(t, q.requeuedCount, droppedAcks.Load()/2,
		"requeue count should reflect dropped acks")
}

// ============================================================
// Stress Test 3: Backpressure Under Saturation
// ============================================================

func TestStress_BackpressureUnderSaturation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in -short mode")
	}

	// Tiny queue + many producers + NO collectors → forces saturation
	q := newTestQueue(t, 50, stressAckTimeout)

	const numProducers = 20
	const messagesPerProducer = 100
	totalAttempts := numProducers * messagesPerProducer

	var (
		successCount atomic.Uint64
		rejectCount  atomic.Uint64
	)

	wg := sync.WaitGroup{}
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < messagesPerProducer; i++ {
				_, err := q.SendTelemetry(context.Background(),
					sampleReq(int64(pid*1000+i), "saturate"))
				if err == nil {
					successCount.Add(1)
				} else {
					rejectCount.Add(1)
				}
			}
		}(p)
	}
	wg.Wait()

	t.Logf("=== Saturation Stats ===")
	t.Logf("Total attempts: %d", totalAttempts)
	t.Logf("Accepted:       %d", successCount.Load())
	t.Logf("Rejected:       %d", rejectCount.Load())
	t.Logf("Queue size:     %d", len(q.messages))
	t.Logf("Reject rate:    %.2f%%", float64(rejectCount.Load())/float64(totalAttempts)*100)

	// Verify backpressure kicked in
	assert.Equal(t, uint64(totalAttempts), successCount.Load()+rejectCount.Load(),
		"every attempt should result in either success or rejection")
	assert.Greater(t, rejectCount.Load(), uint64(0),
		"queue should reject messages when full")
	assert.LessOrEqual(t, len(q.messages), 50,
		"queue size must not exceed maxQueueSize")
	assert.Equal(t, q.rejectedCount, rejectCount.Load(),
		"queue's internal rejected counter must match observed rejections")
}

// ============================================================
// Stress Test 4: Persistence Under Load + Recovery
// ============================================================

func TestStress_PersistenceUnderLoadAndRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in -short mode")
	}

	dbPath := tempDBPath(t)

	const totalMessages = 500
	const numProducers = 10

	// --- Phase 1: bulk write under concurrent load ---
	q1 := newTestQueueWithDB(t, dbPath, 1000, stressAckTimeout)

	wg := sync.WaitGroup{}
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < totalMessages/numProducers; i++ {
				ts := int64(pid*1000 + i)
				_, err := q1.SendTelemetry(context.Background(), sampleReq(ts, "persist"))
				require.NoError(t, err)
			}
		}(p)
	}
	wg.Wait()

	t.Logf("Phase 1: enqueued %d messages", q1.enqueuedCount)
	assert.Equal(t, uint64(totalMessages), q1.enqueuedCount)

	// --- Simulate crash: shut down without acking anything ---
	shutdownQueue(q1)

	// --- Phase 2: restart and verify ALL messages recovered ---
	q2 := newTestQueueWithDB(t, dbPath, 1000, stressAckTimeout)

	t.Logf("Phase 2: recovered %d messages", q2.recoveredCount)
	assert.Equal(t, uint64(totalMessages), q2.recoveredCount,
		"persistence guarantee: all unacked messages must survive restart")
	assert.Equal(t, totalMessages, len(q2.messages))
}
