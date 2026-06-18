package mocks

import (
	"fmt"
	"math/rand/v2"
	"time"
	"watchtower/config"
	"watchtower/models"
)

func GenerateDynatrace(dataPipe chan<- models.EventEnvelope, cfg *config.AppConfig) {
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
				"metric":     "cpu_usage_percent", // Tetap berupa teks statis
				"value":      cpuPercentage,       // Diisi dengan variabel acak yang Anda buat
				"host":       "server-jkt-01",
				"slo_breach": iteration%5 == 0, // Bernilai true jika sedang anomali, false jika normal
			},
		}

		select {
		case dataPipe <- incomingData:

		default:
			droppedCounter++
			fmt.Printf("[Backpressure] Pipa penuh! %s terpaksa dibuang. (Total Dibuang: %d)\n", incomingData.ID, droppedCounter)
		}
		time.Sleep(time.Duration(cfg.Mocks.EmitIntervalMs) * time.Millisecond)
	}
}
