package screening

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
)

type DedupCache struct {
	seen map[string]int64
	mu   sync.RWMutex
	ttl  int64
}

func NewDedupCache(ttlSeconds int) *DedupCache {
	return &DedupCache{
		seen: make(map[string]int64),
		ttl:  int64(ttlSeconds),
	}
}

func (d *DedupCache) IsDuplicate(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	currentTime := time.Now().Unix()

	if expiry, exist := d.seen[id]; exist {
		if currentTime < expiry {
			return true
		}
	}

	d.seen[id] = currentTime + d.ttl

	return false
}

func (d *DedupCache) LoadSnapshot(client *minio.Client, bucketName string) {
	ctx := context.Background()
	objectName := "dedup/window/snapshot.json"

	obj, err := client.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		fmt.Println("[Dedup] There is no snapshot, begin with new memory.")
		return
	}
	defer obj.Close()

	fileBytes, err := io.ReadAll(obj)
	if err != nil {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	tempMap := make(map[string]int64)
	if err := json.Unmarshal(fileBytes, &tempMap); err == nil {
		currentTime := time.Now().Unix()

		for id, expiry := range tempMap {
			if currentTime < expiry {
				d.seen[id] = expiry
			}
		}
		fmt.Printf("[Dedup] Succesfully restored %d snapshots from MinIO!\n", len(d.seen))
	}
}

func (d *DedupCache) SaveSnapshot(client *minio.Client, bucketName string) {
	ctx := context.Background()
	objectName := "dedup/window/snapshot.json"

	d.mu.RLock()
	jsonData, err := json.Marshal(d.seen)
	d.mu.RUnlock()

	if err != nil {
		log.Printf("Failed to create JSON snapshot: %v", err)
		return
	}

	reader := bytes.NewReader(jsonData)
	_, err = client.PutObject(ctx, bucketName, objectName, reader, int64(len(jsonData)), minio.PutObjectOptions{
		ContentType: "application/json",
	})

	if err != nil {
		log.Printf("[Dedup] Failed to store snapshot to MinIO: %v", err)
	} else {
		fmt.Println("[Dedup] Snapshot saved to MinIO successfully before shutdown!")
	}
}
