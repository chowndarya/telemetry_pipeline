package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pb "github.com/chowndarya/telemetry_pipeline/grpc_proto"
)

const (
	defaultMaxQueueSize = 10000
	highWaterMarkPct    = 80
	defaultAckTimeout   = 30 * time.Second
	requeueScanInterval = 10 * time.Second
	defaultBoltDBPath   = "/data/queue.db"
)

type message struct {
	PersistKey uint64 // BoltDB key, assigned at enqueue time. 0 = not persisted (test-only).
	MessageId  string // assigned at dispatch time
	Timestamp  int64
	MetricName string
	GpuId      string
	Device     string
	Uuid       string
	ModelName  []string
	Namespace  string
	Value      float64
	LabelsRaw  string
}

type pendingMessage struct {
	msg        message
	sentAt     time.Time
	deliveries int // how many times we've attempted delivery (for poison-pill detection later)
}

type TelemetryQueueServer struct {
	pb.UnimplementedTelemetryServiceServer

	mu           sync.Mutex
	messages     []message
	pending      map[string]pendingMessage // messageId -> pending
	cond         *sync.Cond
	closed       bool
	maxQueueSize int
	ackTimeout   time.Duration

	// Persistence — may be nil if running in tests without BoltDB.
	db *bbolt.DB

	// Metrics
	enqueuedCount  uint64
	rejectedCount  uint64
	ackedCount     uint64
	requeuedCount  uint64
	recoveredCount uint64

	// Requeue goroutine lifecycle
	stopRequeue chan struct{}
	wg          sync.WaitGroup
}

// NewTelemetryQueueServer constructs a queue WITHOUT persistence. Kept for
// backward compatibility with existing unit tests that don't need BoltDB.
func NewTelemetryQueueServer(maxQueueSize int, ackTimeout time.Duration) *TelemetryQueueServer {
	if maxQueueSize <= 0 {
		maxQueueSize = defaultMaxQueueSize
	}
	if ackTimeout <= 0 {
		ackTimeout = defaultAckTimeout
	}
	s := &TelemetryQueueServer{
		pending:      make(map[string]pendingMessage),
		maxQueueSize: maxQueueSize,
		ackTimeout:   ackTimeout,
		stopRequeue:  make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)

	s.wg.Add(1)
	go s.requeueLoop()
	return s
}

// NewTelemetryQueueServerWithDB constructs a queue with BoltDB persistence and
// recovers any messages persisted from a previous run.
func NewTelemetryQueueServerWithDB(maxQueueSize int, ackTimeout time.Duration, dbPath string) (*TelemetryQueueServer, error) {
	if maxQueueSize <= 0 {
		maxQueueSize = defaultMaxQueueSize
	}
	if ackTimeout <= 0 {
		ackTimeout = defaultAckTimeout
	}

	db, err := openDB(dbPath)
	if err != nil {
		return nil, err
	}

	s := &TelemetryQueueServer{
		pending:      make(map[string]pendingMessage),
		maxQueueSize: maxQueueSize,
		ackTimeout:   ackTimeout,
		stopRequeue:  make(chan struct{}),
		db:           db,
	}
	s.cond = sync.NewCond(&s.mu)

	// CRITICAL: recover BEFORE serving any RPCs.
	recovered, err := recoverMessages(db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("recover messages: %w", err)
	}
	if len(recovered) > 0 {
		s.messages = recovered
		s.recoveredCount = uint64(len(recovered))
		log.Printf("RECOVERY: loaded %d messages from BoltDB at %s", len(recovered), dbPath)
	} else {
		log.Printf("RECOVERY: no messages to recover (clean start) at %s", dbPath)
	}

	s.wg.Add(1)
	go s.requeueLoop()
	return s, nil
}

// SendTelemetry — producer side (with backpressure + persistence)
func (s *TelemetryQueueServer) SendTelemetry(ctx context.Context, req *pb.TelemetryRequest) (*pb.TelemetryResponse, error) {
	if req == nil {
		return &pb.TelemetryResponse{Success: false, Message: "nil request"},
			status.Error(codes.InvalidArgument, "nil request")
	}

	// First lock window: backpressure check.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return &pb.TelemetryResponse{Success: false, Message: "server shutting down"},
			status.Error(codes.Unavailable, "server closed")
	}
	if len(s.messages) >= s.maxQueueSize {
		s.rejectedCount++
		current := len(s.messages)
		rejected := s.rejectedCount
		s.mu.Unlock()
		log.Printf("Queue FULL (size=%d/%d). Rejected ts=%d. Total rejected=%d",
			current, s.maxQueueSize, req.Timestamp, rejected)
		return &pb.TelemetryResponse{Success: false, Message: "queue full"},
			status.Error(codes.ResourceExhausted, "queue full")
	}
	s.mu.Unlock()
	// Lock released — persistence (fsync) happens outside the lock so other
	// producers/consumers aren't blocked during disk I/O. The trade-off is a
	// soft (not hard) max-queue-size under heavy concurrent enqueue.

	msg := message{
		// MessageId is assigned at dispatch time, not enqueue time
		Timestamp:  req.Timestamp,
		MetricName: req.MetricName,
		GpuId:      req.GpuId,
		Device:     req.Device,
		Uuid:       req.Uuid,
		ModelName:  req.ModelName,
		Namespace:  req.Namespace,
		Value:      req.Value,
		LabelsRaw:  req.LabelsRaw,
	}

	// Persist BEFORE acknowledging the producer (only when DB is configured).
	if s.db != nil {
		key, err := persistMessage(s.db, &msg)
		if err != nil {
			log.Printf("Persistence FAILED for ts=%d: %v", req.Timestamp, err)
			return &pb.TelemetryResponse{Success: false, Message: "persistence failed"},
				status.Error(codes.Internal, "persistence failed")
		}
		msg.PersistKey = key
	}

	// Second lock window: append to in-memory queue + signal.
	s.mu.Lock()
	if s.closed {
		// Shutdown started while we were doing fsync. Roll back the persisted
		// entry so it doesn't reappear on next start.
		s.mu.Unlock()
		if s.db != nil && msg.PersistKey != 0 {
			_ = deletePersisted(s.db, msg.PersistKey)
		}
		return &pb.TelemetryResponse{Success: false, Message: "server shutting down"},
			status.Error(codes.Unavailable, "server closed")
	}
	s.messages = append(s.messages, msg)
	s.enqueuedCount++
	depth := len(s.messages)

	if depth*100/s.maxQueueSize >= highWaterMarkPct {
		log.Printf("WARN: queue at %d%% (size=%d/%d, pending=%d)",
			depth*100/s.maxQueueSize, depth, s.maxQueueSize, len(s.pending))
	}

	s.cond.Signal()
	s.mu.Unlock()

	return &pb.TelemetryResponse{Success: true, Message: "enqueued"}, nil
}

// CollectTelemetry — dispatch with MessageId, move to pending
func (s *TelemetryQueueServer) CollectTelemetry(req *pb.TelemetryRequest, stream pb.TelemetryService_CollectTelemetryServer) error {
	for {
		select {
		case <-stream.Context().Done():
			log.Println("Collector disconnected")
			return nil
		default:
		}

		s.mu.Lock()
		for len(s.messages) == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed && len(s.messages) == 0 {
			s.mu.Unlock()
			return nil
		}

		// Pop
		msg := s.messages[0]
		s.messages = s.messages[1:]

		// Assign MessageId & track in pending. PersistKey is preserved from
		// enqueue time so we can delete from BoltDB on ack.
		msg.MessageId = uuid.NewString()
		s.pending[msg.MessageId] = pendingMessage{
			msg:        msg,
			sentAt:     time.Now(),
			deliveries: 1,
		}
		s.mu.Unlock()

		resp := &pb.TelemetryRequest{
			MessageId:  msg.MessageId,
			Timestamp:  msg.Timestamp,
			MetricName: msg.MetricName,
			GpuId:      msg.GpuId,
			Device:     msg.Device,
			Uuid:       msg.Uuid,
			ModelName:  msg.ModelName,
			Namespace:  msg.Namespace,
			Value:      msg.Value,
			LabelsRaw:  msg.LabelsRaw,
		}

		if err := stream.Send(resp); err != nil {
			log.Printf("Send failed for msg=%s: %v. Requeuing immediately.", msg.MessageId, err)
			// Send failed → put it back ourselves (don't wait for ack timeout).
			// Note: PersistKey is preserved through requeue, so the BoltDB
			// entry remains valid until eventual ack.
			s.requeueOne(msg.MessageId)
			return err
		}
		log.Printf("Dispatched msg=%s ts=%d (pending=%d)", msg.MessageId, msg.Timestamp, len(s.pending))
	}
}

// AckTelemetry — collector confirms successful processing
func (s *TelemetryQueueServer) AckTelemetry(ctx context.Context, req *pb.AckRequest) (*pb.AckResponse, error) {
	if req == nil || req.MessageId == "" {
		return &pb.AckResponse{Success: false, Message: "missing message_id"},
			status.Error(codes.InvalidArgument, "missing message_id")
	}

	s.mu.Lock()
	p, ok := s.pending[req.MessageId]
	if !ok {
		s.mu.Unlock()
		// Already requeued/acked — idempotent
		log.Printf("Ack for unknown/expired msg=%s (likely already requeued)", req.MessageId)
		return &pb.AckResponse{Success: true, Message: "unknown or already acked"}, nil
	}
	delete(s.pending, req.MessageId)
	s.ackedCount++
	persistKey := p.msg.PersistKey
	acked := s.ackedCount
	pendingCount := len(s.pending)
	s.mu.Unlock()

	// Delete from BoltDB OUTSIDE the lock. A failure here is not fatal: the
	// message is acked in memory, but on restart it would be re-recovered and
	// re-dispatched, producing an idempotent duplicate write to InfluxDB.
	if s.db != nil {
		if err := deletePersisted(s.db, persistKey); err != nil {
			log.Printf("WARN: failed to delete persisted msg=%s key=%d: %v",
				req.MessageId, persistKey, err)
		}
	}

	log.Printf("ACK msg=%s (acked=%d, pending=%d)", req.MessageId, acked, pendingCount)
	return &pb.AckResponse{Success: true, Message: "acked"}, nil
}

// requeueOne — internal helper to put a single pending message back.
// PersistKey is preserved so the BoltDB entry remains valid.
func (s *TelemetryQueueServer) requeueOne(messageId string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pending[messageId]
	if !ok {
		return
	}
	delete(s.pending, messageId)
	// Reset MessageId so it gets a fresh one on next dispatch.
	// PersistKey stays — message is still durable in BoltDB.
	p.msg.MessageId = ""
	s.messages = append(s.messages, p.msg)
	s.requeuedCount++
	s.cond.Signal()
}

// requeueLoop — background scanner for unacked messages past timeout
func (s *TelemetryQueueServer) requeueLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(requeueScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopRequeue:
			return
		case <-ticker.C:
			s.scanAndRequeue()
		}
	}
}

func (s *TelemetryQueueServer) scanAndRequeue() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var requeued int
	for id, p := range s.pending {
		if now.Sub(p.sentAt) > s.ackTimeout {
			log.Printf("Ack timeout — requeuing msg=%s (waited %v, deliveries=%d)",
				id, now.Sub(p.sentAt), p.deliveries)
			delete(s.pending, id)
			p.msg.MessageId = ""
			// PersistKey preserved — message is still in BoltDB until acked.
			s.messages = append(s.messages, p.msg)
			s.requeuedCount++
			requeued++
		}
	}
	if requeued > 0 {
		s.cond.Broadcast() // wake up collectors
		log.Printf("Requeued %d unacked messages (total requeued=%d, pending=%d)",
			requeued, s.requeuedCount, len(s.pending))
	}
}

func StartGRPCServer(address string, maxQueueSize int, ackTimeout time.Duration, dbPath string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	queueServer, err := NewTelemetryQueueServerWithDB(maxQueueSize, ackTimeout, dbPath)
	if err != nil {
		return fmt.Errorf("init queue server: %w", err)
	}
	pb.RegisterTelemetryServiceServer(grpcServer, queueServer)
	reflection.Register(grpcServer)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("gRPC server listening on %s (maxQueue=%d, ackTimeout=%v, db=%s)",
			address, queueServer.maxQueueSize, queueServer.ackTimeout, dbPath)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Serve failed: %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down...")

	queueServer.mu.Lock()
	queueServer.closed = true
	queueServer.cond.Broadcast()
	log.Printf("Final stats: enqueued=%d, rejected=%d, acked=%d, requeued=%d, recovered=%d, queue=%d, pending=%d",
		queueServer.enqueuedCount, queueServer.rejectedCount, queueServer.ackedCount,
		queueServer.requeuedCount, queueServer.recoveredCount,
		len(queueServer.messages), len(queueServer.pending))
	queueServer.mu.Unlock()

	close(queueServer.stopRequeue)
	queueServer.wg.Wait()

	grpcServer.GracefulStop()

	if queueServer.db != nil {
		if err := queueServer.db.Close(); err != nil {
			log.Printf("WARN: error closing BoltDB: %v", err)
		}
	}
	log.Println("gRPC server stopped, BoltDB closed")
	return errors.Join()
}

func main() {
	address := os.Getenv("GRPC_SERVER_ADDR")
	if address == "" {
		address = ":50051"
	}

	maxQueueSize := defaultMaxQueueSize
	if v := os.Getenv("QUEUE_MAX_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxQueueSize = n
		}
	}

	ackTimeout := defaultAckTimeout
	if v := os.Getenv("ACK_TIMEOUT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			ackTimeout = time.Duration(n) * time.Second
		}
	}

	dbPath := os.Getenv("BOLTDB_PATH")
	if dbPath == "" {
		dbPath = defaultBoltDBPath
	}

	if err := StartGRPCServer(address, maxQueueSize, ackTimeout, dbPath); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
