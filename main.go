package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"watchtower/api"
	"watchtower/config"
	"watchtower/ml"
	"watchtower/mocks"
	"watchtower/models"
	"watchtower/screening"
	"watchtower/storage"

	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Info: File .env not found")
	}

	cfg, err := config.LoadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed read config.json: %v", err)
	}
	fmt.Println("[OK] Config successfully Loaded.")

	minioClient, err := storage.InitMinio(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to MinIO: %v", err)
	}

	DataPipes := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
	RawArchivePipes := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
	ScreeningPipes := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
	ScreenedArchivePipes := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
	QuarantinePipes := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
	CleanDataPipes := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)
	MLPipes := make(chan models.EventEnvelope, cfg.Ingestion.ChannelBufferSize)

	fmt.Println("[OK] Channels Successfully Created.")

	go storage.ArchiveRawEvent(RawArchivePipes, minioClient, cfg.Storage.Bucket)
	go storage.ArchiveScreenedEvent(ScreenedArchivePipes, minioClient, cfg.Storage.Bucket)
	go storage.ArchiveQuarantineEvent(QuarantinePipes, minioClient, cfg.Storage.Bucket) // <--- KURIR BARU

	dedupCache := screening.NewDedupCache(cfg.Screening.DedupTTLSeconds)
	dedupCache.LoadSnapshot(minioClient, cfg.Storage.Bucket)
	noiseFilter := screening.NewNoiseFilter(cfg.Screening.NoiseWindowSeconds)

	defaultPolicy := screening.Policy{
		DefaultPriority: "P4",
		Rules: []screening.Rule{
			{Source: "splunktrace", Key: "severity", Value: "CRITICAL", Priority: "P1"},
		},
	}
	policyManager := screening.NewPolicyManager(defaultPolicy)

	go policyManager.WatchPolicy(minioClient, cfg.Storage.Bucket)

	screening.StartScreeningPipeline(ScreeningPipes, CleanDataPipes, QuarantinePipes, cfg.Screening.WorkerCount, dedupCache, noiseFilter, policyManager)

	sseBroker := api.NewBroker()

	var globalDropCounter atomic.Uint64

	fmt.Println("[System] Memeriksa state sebelumnya di MinIO...")
	stateObj, err := minioClient.GetObject(context.Background(), cfg.Storage.Bucket, "state/dashboard.json", minio.GetObjectOptions{})

	// Jika file ada (tidak error saat mengambil)
	if err == nil {
		var savedState map[string]interface{}
		// Coba terjemahkan isi JSON-nya
		if errDecode := json.NewDecoder(stateObj).Decode(&savedState); errDecode == nil {
			// JSON dari Golang biasanya membaca angka sebagai float64
			if val, ok := savedState["total_dropped"].(float64); ok {
				// Masukkan angka lama ke dalam penghitung kita!
				globalDropCounter.Store(uint64(val))
				fmt.Printf("[System] ✅ Berhasil memulihkan Drop Counter: %d\n", uint64(val))
			}
		}
		stateObj.Close()
	} else {
		fmt.Println("[System] ℹ️ Tidak ada state sebelumnya (Mulai Drop Counter dari 0).")
	}

	apiServer := &api.APIServer{
		PolicyManager: policyManager,
		MinioClient:   minioClient,
		BucketName:    cfg.Storage.Bucket,
		SSEBroker:     sseBroker,
		DropCounter:   &globalDropCounter, // Masukkan ke server
		WorkerCount:   cfg.Screening.WorkerCount,
	}

	go func() {
		err := apiServer.Start(cfg.Server.Port)
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	go func() {
		for data := range DataPipes {
			RawArchivePipes <- data

			payloadCopy := make(map[string]interface{})
			for key, value := range data.Payload {
				payloadCopy[key] = value
			}

			dataScreening := models.EventEnvelope{
				Version:   data.Version,
				ID:        data.ID,
				Source:    data.Source,
				Timestamp: data.Timestamp,
				Payload:   payloadCopy,
			}

			ScreeningPipes <- dataScreening
		}
	}()

	go ml.StartMLWorker(MLPipes, minioClient, cfg.Storage.Bucket, cfg, sseBroker.Notifier)

	go func() {
		for cleanData := range CleanDataPipes {

			ScreenedArchivePipes <- cleanData

			payloadCopy := make(map[string]interface{})
			for k, v := range cleanData.Payload {
				payloadCopy[k] = v
			}

			mlData := models.EventEnvelope{
				Version:   cleanData.Version,
				ID:        cleanData.ID,
				Source:    cleanData.Source,
				Timestamp: cleanData.Timestamp,
				Payload:   payloadCopy,
			}

			MLPipes <- mlData

			jsonData, err := json.Marshal(cleanData)
			if err == nil {
				sseBroker.Notifier <- jsonData
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Duration(cfg.Ingestion.DropLogIntervalMs) * time.Millisecond)
		defer ticker.Stop()
		var lastCount uint64 = 0

		for range ticker.C {
			currentCount := globalDropCounter.Load()
			if currentCount > lastCount {
				fmt.Printf("[Backpressure] Total event terbuang: %d\n", currentCount)
				lastCount = currentCount

				stateMsg := map[string]interface{}{
					"type":          "drop_update",
					"total_dropped": currentCount,
				}
				jsonData, _ := json.Marshal(stateMsg)
				sseBroker.Notifier <- jsonData
			}
		}
	}()

	go mocks.GenerateDynatrace(DataPipes, cfg, &globalDropCounter)
	go mocks.GenerateSplunk(DataPipes, cfg, &globalDropCounter)
	go mocks.GenerateRiverBed(DataPipes, cfg, &globalDropCounter)
	go mocks.GeneratePrometheus(DataPipes, cfg, &globalDropCounter)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan

	fmt.Println("\n\n[System] Stop signal received! Shutting down the application gracefully...")

	dedupCache.SaveSnapshot(minioClient, cfg.Storage.Bucket)

	fmt.Println("[System] Application Shutdown Successfully")
}
