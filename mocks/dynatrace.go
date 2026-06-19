package mocks

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"
	"watchtower/config"
	"watchtower/models"
)

func GenerateDynatrace(dataPipe chan<- models.EventEnvelope, cfg *config.AppConfig, dropCounter *atomic.Uint64, successCounter *atomic.Uint64) {
	fmt.Println("[Dynatrace] mock mulai beroperasi")
	var droppedCounter int64
	for iteration := 1; ; iteration++ {

		cpuPercentage := 40 + rand.Float64()*40

		if iteration%5 == 0 {
			cpuPercentage = 90 + rand.Float64()*10
		}

		currentTime := time.Now().Unix()

		incomingData := models.EventEnvelope{
			Version:   "1.0",
			ID:        fmt.Sprintf("dyna-%d", currentTime),
			Source:    "dynatrace",
			Timestamp: currentTime,
			Payload: map[string]interface{}{
				"metric":     "cpu_usage_percent",
				"value":      cpuPercentage,
				"host":       "server-jkt-01",
				"slo_breach": iteration%5 == 0,
			},
		}

		select {
		case dataPipe <- incomingData:
			successCounter.Add(1)

		default:
			dropCounter.Add(1)
			fmt.Printf("[Backpressure] Pipa penuh! %s terpaksa dibuang. (Total Dibuang: %d)\n", incomingData.ID, droppedCounter)
		}
		time.Sleep(time.Duration(cfg.Mocks.EmitIntervalMs) * time.Millisecond)
	}
}
