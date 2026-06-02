package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.etcd.io/bbolt"
)

const (
	bucketMessages = "messages"
)

// persistedMessage is the JSON shape stored in BoltDB.
// MessageId is intentionally NOT persisted — it's assigned at dispatch time
// and is only meaningful while a message is in the pending map. After recovery,
// messages get fresh MessageIds when they're dispatched again.
type persistedMessage struct {
	PersistKey uint64   `json:"persist_key"`
	Timestamp  int64    `json:"timestamp"`
	MetricName string   `json:"metric_name"`
	GpuId      string   `json:"gpu_id"`
	Device     string   `json:"device"`
	Uuid       string   `json:"uuid"`
	ModelName  []string `json:"model_name"`
	Namespace  string   `json:"namespace"`
	Value      float64  `json:"value"`
	LabelsRaw  string   `json:"labels_raw"`
}

// openDB opens (or creates) the BoltDB file and ensures the bucket exists.
func openDB(path string) (*bbolt.DB, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{
		Timeout: 5 * time.Second, // fail fast if another process holds the lock
	})
	if err != nil {
		return nil, fmt.Errorf("open boltdb at %s: %w", path, err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketMessages))
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create bucket: %w", err)
	}

	return db, nil
}

// persistMessage writes a single message to BoltDB and returns the assigned
// monotonic key. MUST be called before acknowledging the producer.
func persistMessage(db *bbolt.DB, msg *message) (uint64, error) {
	var key uint64
	err := db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketMessages))

		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		key = seq

		pm := persistedMessage{
			PersistKey: key,
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
		encoded, err := json.Marshal(pm)
		if err != nil {
			return err
		}
		return b.Put(uint64ToKey(key), encoded)
	})
	return key, err
}

// deletePersisted removes a message from BoltDB. Called on ack.
// Idempotent — deleting a non-existent key is not an error.
func deletePersisted(db *bbolt.DB, key uint64) error {
	if key == 0 {
		// PersistKey==0 means the message was never persisted (shouldn't happen
		// in normal flow, but be defensive).
		return nil
	}
	return db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte(bucketMessages)).Delete(uint64ToKey(key))
	})
}

// recoverMessages reads all persisted messages back into memory in ascending
// key order (which is enqueue order, since keys are monotonic).
func recoverMessages(db *bbolt.DB) ([]message, error) {
	var recovered []message
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketMessages))
		return b.ForEach(func(k, v []byte) error {
			var pm persistedMessage
			if err := json.Unmarshal(v, &pm); err != nil {
				log.Printf("WARN: skipping corrupt entry key=%v: %v", k, err)
				return nil // don't fail the whole recovery on one bad entry
			}
			recovered = append(recovered, message{
				PersistKey: pm.PersistKey,
				Timestamp:  pm.Timestamp,
				MetricName: pm.MetricName,
				GpuId:      pm.GpuId,
				Device:     pm.Device,
				Uuid:       pm.Uuid,
				ModelName:  pm.ModelName,
				Namespace:  pm.Namespace,
				Value:      pm.Value,
				LabelsRaw:  pm.LabelsRaw,
			})
			return nil
		})
	})
	return recovered, err
}

// uint64ToKey converts a sequence number to a big-endian byte key so that
// lexicographic byte order matches numeric order (required for ForEach to
// iterate in enqueue order).
func uint64ToKey(n uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}
