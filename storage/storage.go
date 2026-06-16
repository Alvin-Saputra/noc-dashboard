package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
	"watchtower/config"
	"watchtower/models"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func InitMinio(cfg *config.AppConfig) (*minio.Client, error) {
	accessKeyID := os.Getenv("MINIO_USER")
	secretAccessKey := os.Getenv("MINIO_PASS")

	minioClient, err := minio.New(cfg.Storage.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	bucketName := cfg.Storage.Bucket
	err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: cfg.Storage.Region})
	if err != nil {

		exists, errBucketExists := minioClient.BucketExists(ctx, bucketName)
		if errBucketExists == nil && exists {
			fmt.Printf("Info: Bucket '%s' ready to use.\n", bucketName)
		} else {
			return nil, err
		}
	} else {
		fmt.Printf("Info: Bucket '%s' successfully created!\n", bucketName)
	}

	return minioClient, nil
}

func ArchiveRawEvent(dataPipe <-chan models.EventEnvelope, client *minio.Client, bucketName string) {
	ctx := context.Background()

	for data := range dataPipe {

		JSONTextResult, err := json.Marshal(data)
		if err != nil {
			log.Printf("Failed Parse to JSON: %v", err)
			continue
		}

		eventTime := time.Unix(data.Timestamp, 0).UTC()
		objectName := fmt.Sprintf("events/raw/%04d/%02d/%02d/%02d/%s.json",
			eventTime.Year(),
			eventTime.Month(),
			eventTime.Day(),
			eventTime.Hour(),
			data.ID,
		)

		reader := bytes.NewReader(JSONTextResult)
		fileSize := int64(len(JSONTextResult))

		_, err = client.PutObject(ctx, bucketName, objectName, reader, fileSize, minio.PutObjectOptions{
			ContentType: "application/json",
		})

		if err != nil {
			log.Printf("Error saving to MinIO: %v", err)
		} else {
			fmt.Printf("[Storage] Successfully Archiving Documents: %s\n", objectName)
		}
	}
}
