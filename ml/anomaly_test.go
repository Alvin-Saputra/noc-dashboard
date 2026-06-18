package ml

import (
	"testing"
)

func TestAnomalyDetection(t *testing.T) {
	detector := NewZScoreDetector(2.5, 5)

	normalData := []float64{40.0, 42.0, 39.0, 41.0, 40.0}
	for _, val := range normalData {
		isAnomaly, _, _ := detector.UpdateAndDetect(val)
		if isAnomaly {
			t.Errorf("Value %.2f should not be detected as anomaly", val)
		}
	}

	isAnomaly, _, _ := detector.UpdateAndDetect(99.0)
	if !isAnomaly {
		t.Error("Failed! Value 99.0 should be detected as anomaly")
	}
	
}