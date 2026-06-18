package screening

import (
	"fmt"
	"sync"
	"time"
	"watchtower/models"
)

type NoiseFilter struct {
	seen   map[string]int64
	mu     sync.RWMutex
	window int64
}

func NewNoiseFilter(windowSeconds int) *NoiseFilter {
	return &NoiseFilter{
		seen:   make(map[string]int64),
		window: int64(windowSeconds),
	}
}

// func (nf *NoiseFilter) IsNoise(data *models.EventEnvelope) bool {
// 	prioritas := fmt.Sprintf("%v", data.Payload["priority"])
// 	signature := fmt.Sprintf("%s-%s", data.Source, prioritas)

// 	nf.mu.Lock()
// 	defer nf.mu.Unlock()

// 	currentTime := time.Now().Unix()

// 	if expiry, exist := nf.seen[signature]; exist {
// 		if currentTime < expiry {
// 			return true
// 		}
// 	}

// 	nf.seen[signature] = currentTime + nf.window

// 	return false
// }

func (nf *NoiseFilter) IsNoise(data *models.EventEnvelope) bool {
	prioritas := fmt.Sprintf("%v", data.Payload["priority"])

	if prioritas == "P4" || prioritas == "<nil>" {
		return false
	}
	// -----------------------------------

	signature := fmt.Sprintf("%s-%s", data.Source, prioritas)

	nf.mu.Lock()
	defer nf.mu.Unlock()

	currentTime := time.Now().Unix()

	if expiry, exist := nf.seen[signature]; exist {
		if currentTime < expiry {
			return true
		}
	}

	nf.seen[signature] = currentTime + nf.window

	return false
}
