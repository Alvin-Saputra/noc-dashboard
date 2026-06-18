package ml

import (
	"testing"
)

func TestTrendPredictor(t *testing.T) {
	predictor := NewTrendPredictor(5)

	predictor.UpdateAndPredict(10.0, 1, 0)
	predictor.UpdateAndPredict(20.0, 2, 0)
	predictor.UpdateAndPredict(30.0, 3, 0)

	pred, confLow, confUp := predictor.UpdateAndPredict(40.0, 4, 1)

	if pred < 49.0 || pred > 51.0 {
		t.Errorf("Wrong Prediction! Expected around 50.0, got %.2f", pred)
	}

	if confLow > confUp {
		t.Errorf("Lower Bound should not be greater than Upper Bound! Got %.2f and %.2f", confLow, confUp)
	}
}
