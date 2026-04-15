package kr.go.kahis.batchmonitor.common.enumeration;

import java.util.Arrays;
import lombok.Getter;

public enum ErrorType {

  LIVESTOCK_ANOMALY("error.livestock-anomaly", true),
  PREDICTION_ANOMALY("error.prediction-anomaly", false),
  FARM_COUNT_ANOMALY("error.farm-count-anomaly", false),
  PNU_ANOMALY("error.pnu-anomaly", false),
  FARM_COORDINATE_MISSING("error.farm-coordinate-missing", false),
  DATA_NOT_FOUND("error.data-not-found", false),
  TRAININGSET_COUNT_MISMATCH("error.trainingset-count-mismatch", false),
  CALC_ENV_ANOMALY("error.calc-env-anomaly", false),
  UNKNOWN("error.unknown", false);

  @Getter
  private final String topic;
  @Getter
  private final boolean isNeedAnalysis;

  ErrorType(String topic, Boolean isNeedAnalysis) {
    this.topic = topic;
    this.isNeedAnalysis = isNeedAnalysis;
  }

  public String[] notAnalysisTopics() {
    return Arrays.stream(ErrorType.values())
        .filter(type -> !type.isNeedAnalysis())
        .map(ErrorType::getTopic)
        .toArray(String[]::new);
  }
}
