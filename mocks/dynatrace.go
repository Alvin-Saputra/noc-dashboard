package mocks

import (
	"fmt"
	"math/rand/v2"
	"time"
	"watchtower/models"
)

func GenerateDynatrace(dataPipe chan<- models.EventEnvelope) {
	fmt.Println("[Dynatrace] mock mulai beroperasi")
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
				"metric": cpuPercentage,
				"value":  65.5,
				"host":   "server-jkt-01",
			},
		}

		dataPipe <- incomingData

		time.Sleep(2 * time.Second)
	}
}
