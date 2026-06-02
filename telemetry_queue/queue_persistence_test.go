package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper: create an isolated BoltDB in a temp dir
func tempDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test_queue.db")
}

// helper: build queue with persistence and register cleanup
func newTestQueueWithDB(t *testing.T, dbPath string, maxSize int, ackTimeout time.Duration) *TelemetryQueueServer {
	t.Helper()
	q, err := NewTelemetryQueueServerWithDB(maxSize, ackTimeout, dbPath)
	require.NoError(t, err)

	t.Cleanup(func() {
		q.mu.Lock()
		alreadyClosed := q.closed
		q.closed = true
		q.cond.Broadcast()
		q.mu.Unlock()

		// Only close the channel and wait for goroutines if not already done
		if !alreadyClosed {
			close(q.stopRequeue)
			q.wg.Wait()
		}

		// Close DB only if still open (closing a closed bbolt DB returns an error)
		if q.db != nil {
			_ = q.db.Close() // ignore error — may already be closed
		}
	})
	return q
}

// ============================================================
// Persistence Tests
// ============================================================

func TestPersistence_NewQueueWithDB_CleanStart(t *testing.T) {
	dbPath := tempDBPath(t)
	q := newTestQueueWithDB(t, dbPath, 10, 30*time.Second)

	assert.NotNil(t, q.db, "DB should be initialized")
	assert.Equal(t, 0, len(q.messages))
	assert.Equal(t, uint64(0), q.recoveredCount)
}

func TestPersistence_SendTelemetry_PersistsToDisk(t *testing.T) {
	dbPath := tempDBPath(t)
	q := newTestQueueWithDB(t, dbPath, 10, 30*time.Second)

	resp, err := q.SendTelemetry(context.Background(), sampleReq(1, "persisted"))
	require.NoError(t, err)
	require.True(t, resp.Success)

	// Verify message has a non-zero PersistKey
	q.mu.Lock()
	defer q.mu.Unlock()
	require.Equal(t, 1, len(q.messages))
	assert.NotZero(t, q.messages[0].PersistKey, "PersistKey should be assigned")
	t.Logf("Persisted with key: %d", q.messages[0].PersistKey)
}

func TestPersistence_RecoveryAfterRestart(t *testing.T) {
	dbPath := tempDBPath(t)

	// --- Phase 1: write messages and shut down ---
	q1 := newTestQueueWithDB(t, dbPath, 10, 30*time.Second)
	for i := 0; i < 5; i++ {
		resp, err := q1.SendTelemetry(context.Background(), sampleReq(int64(i), "metric"))
		require.NoError(t, err)
		require.True(t, resp.Success)
	}

	// Simulate restart — cleanup will be a no-op since we're already shutting down
	shutdownQueue(q1)

	// --- Phase 2: reopen the same DB and verify recovery ---
	q2 := newTestQueueWithDB(t, dbPath, 10, 30*time.Second)

	assert.Equal(t, uint64(5), q2.recoveredCount)
	assert.Equal(t, 5, len(q2.messages))

	for i, msg := range q2.messages {
		assert.NotZero(t, msg.PersistKey, "recovered msg %d should have PersistKey", i)
	}
	t.Logf("Recovered %d messages successfully", q2.recoveredCount)
}

func TestPersistence_AckDeletesFromDisk(t *testing.T) {
	dbPath := tempDBPath(t)

	// --- Phase 1: send 3 messages, ack 1 ---
	q1 := newTestQueueWithDB(t, dbPath, 10, 30*time.Second)
	for i := 0; i < 3; i++ {
		_, err := q1.SendTelemetry(context.Background(), sampleReq(int64(i), "m"))
		require.NoError(t, err)
	}

	// Move first message to pending (simulating dispatch)
	q1.mu.Lock()
	msg := q1.messages[0]
	q1.messages = q1.messages[1:]
	msg.MessageId = "msg-to-ack"
	q1.pending[msg.MessageId] = pendingMessage{msg: msg, sentAt: time.Now(), deliveries: 1}
	q1.mu.Unlock()

	// Ack it
	_, err := q1.AckTelemetry(context.Background(), &pb.AckRequest{MessageId: "msg-to-ack"})
	require.NoError(t, err)

	// Shut down cleanly
	q1.mu.Lock()
	q1.closed = true
	q1.cond.Broadcast()
	q1.mu.Unlock()
	close(q1.stopRequeue)
	q1.wg.Wait()
	require.NoError(t, q1.db.Close())

	// --- Phase 2: reopen and verify acked message is GONE ---
	q2, err := NewTelemetryQueueServerWithDB(10, 30*time.Second, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		q2.mu.Lock()
		q2.closed = true
		q2.cond.Broadcast()
		q2.mu.Unlock()
		close(q2.stopRequeue)
		q2.wg.Wait()
		q2.db.Close()
	})

	assert.Equal(t, uint64(2), q2.recoveredCount, "only 2 unacked messages should be recovered")
	t.Logf("After ack + restart: recovered=%d (expected 2)", q2.recoveredCount)
}

func TestPersistence_RequeuePreservesPersistKey(t *testing.T) {
	dbPath := tempDBPath(t)
	q := newTestQueueWithDB(t, dbPath, 10, 30*time.Second)

	// Send a message
	_, err := q.SendTelemetry(context.Background(), sampleReq(1, "metric"))
	require.NoError(t, err)

	// Capture original PersistKey
	q.mu.Lock()
	originalKey := q.messages[0].PersistKey
	require.NotZero(t, originalKey)

	// Move to pending
	msg := q.messages[0]
	q.messages = q.messages[1:]
	msg.MessageId = "msg-x"
	q.pending["msg-x"] = pendingMessage{msg: msg, sentAt: time.Now(), deliveries: 1}
	q.mu.Unlock()

	// Requeue
	q.requeueOne("msg-x")

	// PersistKey must be preserved
	q.mu.Lock()
	defer q.mu.Unlock()
	require.Equal(t, 1, len(q.messages))
	assert.Equal(t, originalKey, q.messages[0].PersistKey,
		"PersistKey must survive requeue so BoltDB entry stays valid")
	assert.Empty(t, q.messages[0].MessageId, "MessageId should be cleared on requeue")
}

func TestPersistence_InvalidDBPath_ReturnsError(t *testing.T) {
	// Use a path that cannot be created
	q, err := NewTelemetryQueueServerWithDB(10, 30*time.Second, "/nonexistent/dir/queue.db")
	assert.Error(t, err)
	assert.Nil(t, q)
}

func TestPersistence_ManyMessages_AllRecovered(t *testing.T) {
	dbPath := tempDBPath(t)

	const numMessages = 100

	// Phase 1: bulk insert
	q1 := newTestQueueWithDB(t, dbPath, 200, 30*time.Second)
	for i := 0; i < numMessages; i++ {
		_, err := q1.SendTelemetry(context.Background(), sampleReq(int64(i), "bulk"))
		require.NoError(t, err)
	}
	q1.mu.Lock()
	q1.closed = true
	q1.cond.Broadcast()
	q1.mu.Unlock()
	close(q1.stopRequeue)
	q1.wg.Wait()
	require.NoError(t, q1.db.Close())

	// Phase 2: recover and verify count
	q2, err := NewTelemetryQueueServerWithDB(200, 30*time.Second, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		q2.mu.Lock()
		q2.closed = true
		q2.cond.Broadcast()
		q2.mu.Unlock()
		close(q2.stopRequeue)
		q2.wg.Wait()
		q2.db.Close()
	})

	assert.Equal(t, uint64(numMessages), q2.recoveredCount)
	assert.Equal(t, numMessages, len(q2.messages))
	t.Logf("Bulk recovery: %d messages", q2.recoveredCount)
}
