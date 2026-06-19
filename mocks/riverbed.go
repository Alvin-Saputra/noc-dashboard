package mocks

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
	"watchtower/config"
	"watchtower/models"
)

func GenerateRiverBed(dataPipe chan<- models.EventEnvelope, cfg *config.AppConfig, dropCounter *atomic.Uint64, successCounter *atomic.Uint64) {
	fmt.Println("[Dynatrace] mock mulai beroperasi")
	sitePairs := []string{"SG-HQ - ID-JKT", "SG-HQ - US-WEST"}

	var droppedCounter int64

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

		select {
		case dataPipe <- incomingData:
			successCounter.Add(1)

		default:
			dropCounter.Add(1)
			fmt.Printf("[Backpressure] Pipe Full! %s had to be discarded. (Total Discarded: %d)\n", incomingData.ID, droppedCounter)
		}

		currentIndex = (currentIndex + 1) % len(sitePairs)

		time.Sleep(time.Duration(cfg.Mocks.EmitIntervalMs) * time.Millisecond)
	}
}
