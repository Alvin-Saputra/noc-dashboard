package ml

import (
	"math"
)

// AnomalyResult adalah kotak paket khusus untuk dikirim ke dashboard dan MinIO
type AnomalyResult struct {
	Source        string  `json:"source"`
	Metric        string  `json:"metric"`
	ObservedValue float64 `json:"observed_value"`
	ExpectedMean  float64 `json:"expected_mean"`
	Confidence    float64 `json:"confidence"` // 0.0 sampai 1.0
	Timestamp     int64   `json:"timestamp"`
}

// ZScoreDetector adalah memori mesin kita
type ZScoreDetector struct {
	windowSize int       // Berapa banyak data historis yang diingat (misal: 50 data terakhir)
	history    []float64 // Array untuk menyimpan angka-angka historis
}

func NewZScoreDetector(size int) *ZScoreDetector {
	return &ZScoreDetector{
		windowSize: size,
		history:    make([]float64, 0, size),
	}
}

// UpdateAndDetect memasukkan nilai baru, lalu langsung mengecek apakah itu anomali
func (d *ZScoreDetector) UpdateAndDetect(value float64) (isAnomaly bool, expectedMean float64, confidence float64) {
	// 1. Jika memori masih kosong atau belum penuh, catat saja dan anggap normal
	if len(d.history) < d.windowSize {
		d.history = append(d.history, value)
		return false, value, 0.0
	}

	// 2. Hitung Rata-rata (Mean / μ)
	var sum float64
	for _, v := range d.history {
		sum += v
	}
	mean := sum / float64(len(d.history))

	// 3. Hitung Simpangan Baku (Standard Deviation / σ)
	var varianceSum float64
	for _, v := range d.history {
		varianceSum += math.Pow(v-mean, 2)
	}
	variance := varianceSum / float64(len(d.history))
	stdDev := math.Sqrt(variance)

	// 4. Hitung Z-Score! 
	// (Cegah pembagian dengan nol jika datanya tidak pernah berubah)
	zScore := 0.0
	if stdDev > 0 {
		zScore = (value - mean) / stdDev
	}

	// 5. Ubah Z-Score menjadi Confidence Score (0.0 - 1.0)
	// Semakin tinggi Z-Score, semakin mendekati 1.0
	confidence = 1.0 - math.Exp(-math.Abs(zScore)/2.0)

	// 6. Tentukan batas anomali (Misal: Z-Score lebih dari 2.5 dianggap anomali)
	isAnomaly = math.Abs(zScore) > 2.5

	// 7. Geser memori historis (Buang data paling lama, masukkan data paling baru)
	d.history = append(d.history[1:], value)

	return isAnomaly, mean, confidence
}