package main

import (
	"fmt"
	"log"
	"watchtower/config"
	"watchtower/mocks"
	"watchtower/models"
	"watchtower/storage"

	"github.com/joho/godotenv"
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
	fmt.Println("[OK] Channel Successfully Created.")

	go storage.ArchiveRawEvent(DataPipes, minioClient, cfg.Storage.Bucket)

	go mocks.GenerateDynatrace(DataPipes, cfg)
	go mocks.GenerateSplunk(DataPipes, cfg)
	go mocks.GenerateRiverBed(DataPipes, cfg)
	go mocks.GeneratePrometheus(DataPipes, cfg)

	select {}
}
