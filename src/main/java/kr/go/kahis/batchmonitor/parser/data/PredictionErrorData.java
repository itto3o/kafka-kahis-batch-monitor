package kr.go.kahis.batchmonitor.parser.data;

public record PredictionErrorData(
    String farmNumber,
    String currentPrediction,
    String previousPrediction
) {

}
