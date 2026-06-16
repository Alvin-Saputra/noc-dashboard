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

	for iteration := 1; ; iteration++ {

		randomizer := rand.Intn(3)

		if iteration%50 == 0 {
			for iteration := 1; iteration <= 20; iteration++ {
				currentTime := time.Now().Unix()

				incomingData := models.EventEnvelope{
					Version:   "1.0",
					ID:        fmt.Sprintf("splunk-%d", currentTime),
					Source:    "splunktrace",
					Timestamp: currentTime,
					Payload: map[string]interface{}{
						"severity":  "CRITICAL",
						"source_ip": "192.168.1.55",
						"service":   "auth-service",
						"message":   "Failed login attempt",
					},
				}

				dataPipe <- incomingData

				time.Sleep(3 * time.Second)
				continue
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

		dataPipe <- incomingData

		time.Sleep(time.Duration(cfg.Mocks.EmitIntervalMs) * time.Millisecond)
	}
}
