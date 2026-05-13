package kr.go.kahis.batchmonitor.domain.parser.data;

public record PredictionErrorData(
    String farmNumber,
    String currentPrediction,
    String previousPrediction
) {

}
