package screening

import (
	"testing"
	"time"
	"watchtower/models"
)

// TestValidateEvent menguji Pos 0 untuk memastikan data cacat dan basi terdeteksi
func TestValidateEvent(t *testing.T) {
	waktuSekarang := time.Now().Unix()

	// Ini adalah pola "Table-Driven Tests"
	tests := []struct {
		name           string               // Nama skenario
		data           models.EventEnvelope // Data yang diuji
		expectedValid  bool                 // Harapan: Lolos (true) atau Ditolak (false)?
		expectedReason string               // Harapan: Jika ditolak, apa alasannya?
	}{
		{
			name: "Data Valid (Lolos)",
			data: models.EventEnvelope{
				ID:        "valid-123",
				Version:   "1.0",
				Source:    "splunktrace",
				Timestamp: waktuSekarang,
				Payload:   map[string]interface{}{"pesan": "aman"},
			},
			expectedValid:  true,
			expectedReason: "",
		},
		{
			name: "Data Cacat - Tanpa ID",
			data: models.EventEnvelope{
				ID:        "", // Sengaja dikosongkan
				Version:   "1.0",
				Source:    "splunktrace",
				Timestamp: waktuSekarang,
				Payload:   map[string]interface{}{"pesan": "bahaya"},
			},
			expectedValid:  false,
			expectedReason: "missing_id",
		},
		{
			name: "Data Cacat - Tanpa Payload",
			data: models.EventEnvelope{
				ID:        "nopayload-123",
				Version:   "1.0",
				Source:    "splunktrace",
				Timestamp: waktuSekarang,
				Payload:   nil, // Sengaja dikosongkan
			},
			expectedValid:  false,
			expectedReason: "empty_payload",
		},
		{
			name: "Data Basi (Lebih dari 24 Jam yang Lalu)",
			data: models.EventEnvelope{
				ID:        "stale-123",
				Version:   "1.0",
				Source:    "splunktrace",
				Timestamp: waktuSekarang - 86405, // 24 jam + 5 detik yang lalu
				Payload:   map[string]interface{}{"pesan": "basi"},
			},
			expectedValid:  false,
			expectedReason: "stale_timestamp",
		},
		{
			name: "Data Masa Depan (Lebih dari 1 Menit ke Depan)",
			data: models.EventEnvelope{
				ID:        "future-123",
				Version:   "1.0",
				Source:    "splunktrace",
				Timestamp: waktuSekarang + 120, // 2 menit di masa depan
				Payload:   map[string]interface{}{"pesan": "time traveler"},
			},
			expectedValid:  false,
			expectedReason: "future_timestamp",
		},
	}

	// Menjalankan semua skenario di atas satu per satu
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isValid, reason := ValidateEvent(&tt.data)

			// Cek apakah status lolos/tidaknya sesuai harapan
			if isValid != tt.expectedValid {
				t.Errorf("Harapan Valid: %v, tapi mendapat: %v", tt.expectedValid, isValid)
			}

			// Cek apakah alasan penolakannya sesuai harapan
			if reason != tt.expectedReason {
				t.Errorf("Harapan Alasan: '%s', tapi mendapat: '%s'", tt.expectedReason, reason)
			}
		})
	}
}