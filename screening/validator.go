package screening

import (
	"time"
	"watchtower/models"
)


func ValidateEvent(data *models.EventEnvelope) (bool, string) {
	if data.ID == "" {
		return false, "missing_id"
	}
	if data.Version == "" {
		return false, "missing_version"
	}
	if data.Source == "" {
		return false, "unknown_source"
	}
	if len(data.Payload) == 0 {
		return false, "empty_payload"
	}

	waktuSekarang := time.Now().Unix()
	
	// Toleransi keterlambatan jam server maksimal 1 menit ke masa depan
	if data.Timestamp > waktuSekarang+60 { 
		return false, "future_timestamp"
	}
	
	// Data tidak boleh lebih tua dari 24 jam (86400 detik)
	if data.Timestamp < waktuSekarang-86400 { 
		return false, "stale_timestamp"
	}

	return true, "" // Lolos semua pengecekan
}