package mocks

import (
	"fmt"
	"math/rand"
	"time"
	"watchtower/config"
	"watchtower/models"
)

func GenerateSplunk(dataPipe chan<- models.EventEnvelope, cfg *config.AppConfig) {
	fmt.Println("[Dynatrace] mock mulai beroperasi")

	severity := []string{
		"INFO",
		"WARN",
		"ERROR",
		"CRITICAL"}

	var droppedCounter int64

	for iteration := 1; ; iteration++ {

		randomizer := rand.Intn(3)

		if iteration%10 == 0 {

			burstTime := time.Now().Unix()
			burstID := fmt.Sprintf("splunk-%d", burstTime)

			for i := 1; i <= 20; i++ {
				incomingData := models.EventEnvelope{
					Version:   "1.0",
					ID:        burstID,
					Source:    "splunktrace",
					Timestamp: burstTime,
					Payload: map[string]interface{}{
						"severity":  "CRITICAL",
						"source_ip": "192.168.1.55",
						"service":   "auth-service",
						"message":   "Failed login attempt",
					},
				}

				dataPipe <- incomingData

			}
		}

		currentTime := time.Now().Unix()

		incomingData := models.EventEnvelope{
			Version:   "1.0",
			ID:        fmt.Sprintf("splunk-%d", currentTime),
			Source:    "splunktrace",
			Timestamp: currentTime,
			Payload: map[string]interface{}{
				"severity":  severity[randomizer],
				"source_ip": "192.168.1.55",
				"service":   "auth-service",
				"message":   "Failed login attempt",
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
