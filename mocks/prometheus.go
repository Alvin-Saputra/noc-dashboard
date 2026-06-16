package mocks

import (
	"fmt"
	"math/rand/v2"
	"time"
	"watchtower/config"
	"watchtower/models"
)

func GeneratePrometheus(dataPipe chan<- models.EventEnvelope, cfg *config.AppConfig) {
	fmt.Println("[Prometheus] Pabrik mulai beroperasi...")

	metricFamilies := []string{"http_request_rate", "http_error_rate", "system_saturation"}
	currentIndex := 0

	for {
		currentTime := time.Now().Unix()
		currentMetric := metricFamilies[currentIndex]

		var value float64

		labels := map[string]string{
			"service": "api-gateway",
			"zone":    "ap-southeast-1",
		}

		switch currentMetric {
		case "http_request_rate":

			value = 100.0 + rand.Float64()*400.0
			labels["method"] = "GET"
			labels["endpoint"] = "/api/users"

		case "http_error_rate":

			value = rand.Float64() * 5.0
			labels["method"] = "POST"
			labels["endpoint"] = "/api/checkout"

			if rand.Float32() < float32(cfg.Mocks.AnomalyProbability) {
				value = 50.0 + rand.Float64()*50.0
			}

		case "system_saturation":

			value = 0.30 + rand.Float64()*0.40
			labels["resource"] = "database_connection_pool"

			if rand.Float32() < float32(cfg.Mocks.AnomalyProbability) {
				value = 0.95 + rand.Float64()*0.05
			}
		}

		incomingData := models.EventEnvelope{
			Version:   "1.0",
			ID:        fmt.Sprintf("prom-%d-%d", currentTime, currentIndex),
			Source:    "prometheus",
			Timestamp: currentTime,
			Payload: map[string]interface{}{
				"metric_name": currentMetric,
				"labels":      labels,
				"value":       value,
			},
		}

		dataPipe <- incomingData

		currentIndex = (currentIndex + 1) % len(metricFamilies)

		time.Sleep(time.Duration(cfg.Mocks.EmitIntervalMs) * time.Millisecond)
	}
}
