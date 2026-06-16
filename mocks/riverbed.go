package mocks

import (
	"fmt"
	"math/rand"
	"time"
	"watchtower/config"
	"watchtower/models"
)

func GenerateRiverBed(dataPipe chan<- models.EventEnvelope, cfg *config.AppConfig) {
	fmt.Println("[Dynatrace] mock mulai beroperasi")
	sitePairs := []string{"SG-HQ - ID-JKT", "SG-HQ - US-WEST"}

	currentIndex := 0
	for {
		currentTime := time.Now().Unix()
		currentLocation := sitePairs[currentIndex]

		randomThroughput := 10.0 + rand.Float64()*90.0
		randomPacketLoss := rand.Float64() * 2.0
		randomRtt := 10 + rand.Intn(140)

		if rand.Float32() < float32(cfg.Mocks.AnomalyProbability) {
			randomPacketLoss = 15.5
			randomRtt = 450
		}

		incomingData := models.EventEnvelope{
			Version:   "1.0",
			ID:        fmt.Sprintf("riverbed-%d", currentTime),
			Source:    "riverbedtrace",
			Timestamp: currentTime,
			Payload: map[string]interface{}{
				"site_pair":           currentLocation,
				"throughput_mbps":     randomThroughput,
				"packet_loss_percent": randomPacketLoss,
				"rtt_ms":              randomRtt,
			},
		}

		dataPipe <- incomingData

		currentIndex = (currentIndex + 1) % len(sitePairs)

		time.Sleep(time.Duration(cfg.Mocks.EmitIntervalMs) * time.Millisecond)
	}
}
