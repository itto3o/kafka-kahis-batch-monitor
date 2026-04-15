package kr.go.kahis.batchmonitor.common.enumeration;

import lombok.Getter;

public enum ErrorType {

  LIVESTOCK_ANOMALY("error.livestock-anomaly"),
  PREDICTION_ANOMALY("error.prediction-anomaly"),
  FARM_COUNT_ANOMALY("error.data-sync-failure"),
  PNU_ANOMALY("error.data-sync-failure"),
  FARM_COORDINATE_MISSING("error.data-sync-failure"),
  DATA_NOT_FOUND("error.data-sync-failure"),
  TRAININGSET_COUNT_MISMATCH("error.data-sync-failure"),
  CALC_ENV_ANOMALY("error.data-sync-failure"),
  UNKNOWN("error.data-sync-failure");

  @Getter
  private final String topic;

  ErrorType(String topic) {
    this.topic = topic;
  }
}
