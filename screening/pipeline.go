package screening

import (
	"fmt"
	"sync"
	"watchtower/models"
)

func StartScreeningPipeline(
	inputPipe <-chan models.EventEnvelope,
	outputPipe chan<- models.EventEnvelope,
	quarantinePipe chan<- models.EventEnvelope,
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
			fmt.Printf("[Screening] Worker %d Ready!\n", workerID)

			for data := range inputPipe {

				isValid, reason := ValidateEvent(&data)
				if !isValid {
					// Selipkan alasan error ke dalam payload
					data.Payload["quarantine_reason"] = reason

					// Lempar data ke pipa karantina
					quarantinePipe <- data

					// Hentikan proses, jangan lanjut ke Deduplikasi
					continue
				}

				if dedupCache.IsDuplicate(data.ID) {
					continue
				}

				Classify(&data, policyManager)

				if NoiseFilter.IsNoise(&data) {
					continue
				}

				outputPipe <- data
				prioritas := data.Payload["priority"]
				if prioritas == "P1" || prioritas == "P2" || prioritas == "P3" {
					fmt.Printf("\n🔥 [ALARM POS 2] Worker %d - %s | Priority: %v 🔥\n\n", workerID, data.Source, prioritas)
				} else {
					fmt.Printf("[Pass Post 2] Worker %d - ID: %s | Priority: %v\n", workerID, data.ID, prioritas)
				}
			}
		}(i)
	}
}
