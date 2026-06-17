package screening

import (
	"fmt"
	"sync"
	"watchtower/models"
)

func StartScreeningPipeline(
	inputPipe <-chan models.EventEnvelope,
	outputPipe chan<- models.EventEnvelope,
	workerCount int,
	dedupCache *DedupCache,
	NoiseFilter *NoiseFilter,
	policyManager *PolicyManager,
) {
	var wg sync.WaitGroup

	for i := 1; i <= workerCount; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()
			fmt.Printf("[Screening] Worker Ready!\n", workerID)

			for data := range inputPipe {

				if dedupCache.IsDuplicate(data.ID) {
					continue 
				}

				Classify(&data, &policyManager.policy)

				if NoiseFilter.IsNoise(&data) {
					continue 
				}

				outputPipe <- data 
				prioritas := data.Payload["priority"]
				fmt.Printf("[Lolos Pos 2] Worker %d - ID: %s | Priority: %v\n", workerID, data.ID, prioritas)
			}
		}(i)
	}
}
