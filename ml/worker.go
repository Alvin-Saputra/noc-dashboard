package ml

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"watchtower/models"

	"github.com/minio/minio-go/v7"
)

// StartMLWorker adalah mandor yang mengurus semua prediksi Machine Learning
func StartMLWorker(
	mlPipe <-chan models.EventEnvelope,
	minioClient *minio.Client,
	bucketName string,
) {
	// Peta (Map) untuk menyimpan otak Z-Score untuk masing-masing metrik yang berbeda.
	// Contoh nama kunci: "prometheus-request_rate"
	detectors := make(map[string]*ZScoreDetector)

	for data := range mlPipe {
		// Untuk anomali angka, kita akan fokus pada data dari "prometheus"
		// (Anda bisa menyesuaikannya nanti untuk Riverbed atau Dynatrace)
		if data.Source == "prometheus" {

			// Ambil nama metrik dan nilainya dari dalam Payload
			metricName := fmt.Sprintf("%v", data.Payload["metric_name"])
			valRaw, ok := data.Payload["value"]
			if !ok {
				continue
			}

			// Lakukan konversi tipe secara aman dari JSON ke float64
			var value float64
			switch v := valRaw.(type) {
			case float64:
				value = v
			case int:
				value = float64(v)
			default:
				continue // Lewati jika bukan angka
			}

			// Buat kunci unik untuk memori
			key := fmt.Sprintf("%s-%s", data.Source, metricName)

			// Jika metrik ini belum pernah dilihat, buatkan otak (detector) baru
			if _, exists := detectors[key]; !exists {
				// Mengingat 50 data terakhir untuk mencari rata-rata
				detectors[key] = NewZScoreDetector(10)
			}

			// MASUKKAN KE DALAM MESIN ML!
			detector := detectors[key]
			isAnomaly, expectedMean, confidence := detector.UpdateAndDetect(value)

			// Jika Z-Score menyatakan ini tidak wajar (Anomali)
			if isAnomaly {
				fmt.Printf("[ML - Anomali!] 🚨 %s melonjak ke %.2f! (Normalnya: %.2f) | Skor Kepercayaan: %.2f\n",
					key, value, expectedMean, confidence)

				// Buat paket hasil prediksi
				result := AnomalyResult{
					Source:        data.Source,
					Metric:        metricName,
					ObservedValue: value,
					ExpectedMean:  expectedMean,
					Confidence:    confidence,
					Timestamp:     data.Timestamp,
				}

				// Simpan ke rak gudang MinIO
				saveAnomalyToMinIO(result, data.ID, minioClient, bucketName)
			}
		}
	}
}

// Fungsi kurir khusus untuk mengantar dokumen Anomali ke MinIO
func saveAnomalyToMinIO(result AnomalyResult, eventID string, client *minio.Client, bucketName string) {
	ctx := context.Background()
	jsonData, err := json.Marshal(result)
	if err != nil {
		return
	}

	eventTime := time.Unix(result.Timestamp, 0).UTC()

	// Sesuai Spesifikasi: /ml/anomalies/YYYY/MM/DD/<uuid>.json
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
