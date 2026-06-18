package screening

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
	"watchtower/models"

	"github.com/minio/minio-go/v7"
)

type Rule struct {
	Source   string      `json:"source"`
	Key      string      `json:"key"`
	Value    interface{} `json:"value"`
	Priority string      `json:"priority"`
}

type Policy struct {
	Rules           []Rule `json:"rules"`
	DefaultPriority string `json:"default_priority"`
}

type PolicyManager struct {
	policy Policy
	mu     sync.RWMutex
	etag   string
}

func NewPolicyManager(defaultPolicy Policy) *PolicyManager {
	return &PolicyManager{
		policy: defaultPolicy,
	}
}

func (pm *PolicyManager) UpdatePolicy(newPolicy Policy, newETag string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.policy = newPolicy
	pm.etag = newETag
	fmt.Println("\n[HOT-RELOAD] Policy Updated Successfuly From MinIO!")
}

func (pm *PolicyManager) WatchPolicy(client *minio.Client, bucketName string) {
	ctx := context.Background()
	objectName := "policy/screening.json"

	for {
		time.Sleep(5 * time.Second)

		stat, err := client.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
		if err != nil {
			continue
		}

		pm.mu.RLock()
		currentETag := pm.etag
		pm.mu.RUnlock()

		if stat.ETag != "" && stat.ETag != currentETag {
			obj, err := client.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
			if err != nil {
				log.Printf("Failed to fetch policy: %v", err)
				continue
			}

			fileBytes, _ := io.ReadAll(obj)
			var newPolicy Policy

			if err := json.Unmarshal(fileBytes, &newPolicy); err == nil {
				pm.UpdatePolicy(newPolicy, stat.ETag)
			} else {
				log.Printf("[HOT-RELOAD] Failed to read JSON: %v", err)
			}
			obj.Close()
		}
	}
}

func (pm *PolicyManager) GetPolicy() Policy {
	return pm.policy
}

func (pm *PolicyManager) RLock()   { pm.mu.RLock() }
func (pm *PolicyManager) RUnlock() { pm.mu.RUnlock() }

func Classify(data *models.EventEnvelope, pm *PolicyManager) {
	pm.mu.RLock()
	currentPolicy := pm.policy
	pm.mu.RUnlock()

	stamp := currentPolicy.DefaultPriority

	for _, rule := range currentPolicy.Rules {
		if data.Source == rule.Source {
			if isiPayload, exist := data.Payload[rule.Key]; exist {
				if fmt.Sprintf("%v", isiPayload) == fmt.Sprintf("%v", rule.Value) {
					stamp = rule.Priority
					break
				}
			}
		}
	}

	data.Payload["priority"] = stamp
}
