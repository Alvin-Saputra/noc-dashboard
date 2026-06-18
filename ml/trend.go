package ml

import (
	"math"
)

type ForecastResult struct {
	Source           string  `json:"source"`
	Metric           string  `json:"metric"`
	PredictedValue   float64 `json:"predicted_value"`
	ConfLowerBound   float64 `json:"conf_lower_bound"`
	ConfUpperBound   float64 `json:"conf_upper_bound"`
	HorizonTimestamp int64   `json:"horizon_timestamp"` 
}


type TrendPredictor struct {
	windowSize int
	historyX   []float64 
	historyY   []float64 
}

func NewTrendPredictor(size int) *TrendPredictor {
	return &TrendPredictor{
		windowSize: size,
		historyX:   make([]float64, 0, size),
		historyY:   make([]float64, 0, size),
	}
}

func (t *TrendPredictor) UpdateAndPredict(value float64, timestamp int64, horizonSeconds int64) (pred float64, confLower float64, confUpper float64) {
	if len(t.historyX) >= t.windowSize {
		t.historyX = t.historyX[1:]
		t.historyY = t.historyY[1:]
	}
	t.historyX = append(t.historyX, float64(timestamp))
	t.historyY = append(t.historyY, value)

	n := float64(len(t.historyX))

	if n < 2 {
		return value, value, value
	}

	var sumX, sumY, sumXY, sumX2 float64
	for i := 0; i < int(n); i++ {
		sumX += t.historyX[i]
		sumY += t.historyY[i]
		sumXY += t.historyX[i] * t.historyY[i]
		sumX2 += t.historyX[i] * t.historyX[i]
	}

	denominator := (n * sumX2) - (sumX * sumX)
	var m, c float64
	if denominator != 0 {
		m = ((n * sumXY) - (sumX * sumY)) / denominator
		c = (sumY - (m * sumX)) / n
	} else {
		m = 0
		c = sumY / n
	}

	targetTime := float64(timestamp + horizonSeconds)
	pred = (m * targetTime) + c

	var varianceSum float64
	for i := 0; i < int(n); i++ {
		garisPrediksi := (m * t.historyX[i]) + c
		selisih := t.historyY[i] - garisPrediksi
		varianceSum += selisih * selisih
	}
	standardError := math.Sqrt(varianceSum / n)

	margin := 2.0 * standardError
	
	if pred-margin < 0 && value >= 0 {
		confLower = 0
	} else {
		confLower = pred - margin
	}
	confUpper = pred + margin

	return pred, confLower, confUpper
}