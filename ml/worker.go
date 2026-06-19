package ml

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"watchtower/config"
	"watchtower/models"

	"github.com/minio/minio-go/v7"
)

func StartMLWorker(
	mlPipe <-chan models.EventEnvelope,
	minioClient *minio.Client,
	bucketName string,
	cfg *config.AppConfig,
	sseNotifier chan<- []byte,
) {

	detectors := make(map[string]*ZScoreDetector)
	predictors := make(map[string]*TrendPredictor)

	for data := range mlPipe {
		var metricName string
		var value float64
		var isNumerical bool

		switch data.Source {
		case "prometheus":
			metricName = fmt.Sprintf("%v", data.Payload["metric_name"])
			value, isNumerical = getFloat(data.Payload["value"])

		case "dynatrace":
			metricName = fmt.Sprintf("%v", data.Payload["metric"])
			value, isNumerical = getFloat(data.Payload["value"])

		case "riverbedtrace":

			metricName = "rtt_ms"
			value, isNumerical = getFloat(data.Payload["rtt_ms"])

		default:
			continue
		}

		if !isNumerical {
			continue
		}

		key := fmt.Sprintf("%s-%s", data.Source, metricName)

		if _, exists := detectors[key]; !exists {
			detectors[key] = NewZScoreDetector(cfg.ML.AnomalySigmaThreshold, 10)
		}

		detector := detectors[key]
		isAnomaly, expectedMean, confidence := detector.UpdateAndDetect(value)

		if isAnomaly {
			fmt.Printf("[ML - Anomali!] 🚨 %s melonjak ke %.2f! (Normal: %.2f) | Skor: %.2f\n",
				key, value, expectedMean, confidence)

			result := AnomalyResult{
				Source:        data.Source,
				Metric:        metricName,
				ObservedValue: value,
				ExpectedMean:  expectedMean,
				Confidence:    confidence,
				Timestamp:     data.Timestamp,
			}
			saveAnomalyToMinIO(result, data.ID, minioClient, bucketName)
			jsonAnomali, _ := json.Marshal(result)
			sseNotifier <- jsonAnomali
		}

		if _, exists := predictors[key]; !exists {
			predictors[key] = NewTrendPredictor(cfg.ML.RegressionWindowSize)
		}
		predictor := predictors[key]

		horizonSeconds := int64(cfg.ML.ForecastHorizonMinutes * 60)
		predVal, confLow, confUp := predictor.UpdateAndPredict(value, data.Timestamp, horizonSeconds)

		if len(predictor.historyX)%50 == 0 {
			fmt.Printf("[ML - Ramalan] 🔮 %s dlm 5 menit: %.2f (Batas Bawah: %.2f, Atas: %.2f)\n",
				key, predVal, confLow, confUp)
		}

		forecast := ForecastResult{
			Source:           data.Source,
			Metric:           metricName,
			PredictedValue:   predVal,
			ConfLowerBound:   confLow,
			ConfUpperBound:   confUp,
			HorizonTimestamp: data.Timestamp + horizonSeconds,
		}

		saveForecastToMinIO(forecast, minioClient, bucketName)
		jsonRamalan, _ := json.Marshal(forecast)
		sseNotifier <- jsonRamalan

	}
}

func saveAnomalyToMinIO(result AnomalyResult, eventID string, client *minio.Client, bucketName string) {
	ctx := context.Background()
	jsonData, err := json.Marshal(result)
	if err != nil {
		return
	}

	eventTime := time.Unix(result.Timestamp, 0).UTC()

	objectName := fmt.Sprintf("ml/anomalies/%04d/%02d/%02d/%s.json",
		eventTime.Year(), eventTime.Month(), eventTime.Day(), eventID)

	reader := bytes.NewReader(jsonData)
	_, err = client.PutObject(ctx, bucketName, objectName, reader, int64(len(jsonData)), minio.PutObjectOptions{
		ContentType: "application/json",
	})

	if err != nil {
		log.Printf("[ML] 🔴 Gagal menyimpan anomali ke MinIO: %v", err)
	}
}

func saveForecastToMinIO(result ForecastResult, client *minio.Client, bucketName string) {
	ctx := context.Background()
	jsonData, _ := json.Marshal(result)
	eventTime := time.Unix(result.HorizonTimestamp, 0).UTC()

	objectName := fmt.Sprintf("ml/forecasts/%04d/%02d/%02d/%s-%s.json",
		eventTime.Year(), eventTime.Month(), eventTime.Day(), result.Source, result.Metric)

	reader := bytes.NewReader(jsonData)
	client.PutObject(ctx, bucketName, objectName, reader, int64(len(jsonData)), minio.PutObjectOptions{ContentType: "application/json"})
}

func getFloat(unk interface{}) (float64, bool) {
	switch v := unk.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case float32:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}
