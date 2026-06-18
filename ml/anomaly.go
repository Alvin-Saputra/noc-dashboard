package ml

import (
	"math"
)

type AnomalyResult struct {
	Source        string  `json:"source"`
	Metric        string  `json:"metric"`
	ObservedValue float64 `json:"observed_value"`
	ExpectedMean  float64 `json:"expected_mean"`
	Confidence    float64 `json:"confidence"`
	Timestamp     int64   `json:"timestamp"`
}

type ZScoreDetector struct {
	windowSize            int
	anomalySigmaThreshold float64
	history               []float64
}

func NewZScoreDetector(anomaly_sigma_threshold float64, size int) *ZScoreDetector {
	return &ZScoreDetector{
		windowSize:            size,
		anomalySigmaThreshold: anomaly_sigma_threshold,
		history:               make([]float64, 0, size),
	}
}

func (d *ZScoreDetector) UpdateAndDetect(value float64) (isAnomaly bool, expectedMean float64, confidence float64) {

	if len(d.history) < d.windowSize {
		d.history = append(d.history, value)
		return false, value, 0.0
	}

	var sum float64
	for _, v := range d.history {
		sum += v
	}
	mean := sum / float64(len(d.history))

	var varianceSum float64
	for _, v := range d.history {
		varianceSum += math.Pow(v-mean, 2)
	}
	variance := varianceSum / float64(len(d.history))
	stdDev := math.Sqrt(variance)

	zScore := 0.0
	if stdDev > 0 {
		zScore = (value - mean) / stdDev
	}

	confidence = 1.0 - math.Exp(-math.Abs(zScore)/2.0)

	isAnomaly = math.Abs(zScore) > d.anomalySigmaThreshold

	d.history = append(d.history[1:], value)

	return isAnomaly, mean, confidence
}
