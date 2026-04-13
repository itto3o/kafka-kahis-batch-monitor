package kr.go.kahis.batchmonitor.persistence.enumeration;

/**
 * Airflow에서 발생한 배치 에러의 유형.
 *
 * <p>{@code task_id} 기반으로 라우팅되며, 에러 유형별로 검증 로직과 자동 처리 정책이 다릅니다.
 */
public enum ErrorType {

  /** 사육두수 이상감지 — {@code make_risk_data / check_tb_livestock_species_information}. HIST 자동 검증 대상. */
  LIVESTOCK_ANOMALY,

  /** 예측치 에러 — {@code make_risk_data / check_tb_prediction_result}. */
  PREDICTION_ANOMALY,

  /** ASF 배치 에러 — {@code ASF_일일프로세스} 하위 task. */
  ASF_BATCH_FAILURE,

  /** 데이터 적재 실패 — {@code copy_oracle_m2msys}의 Oracle→PostgreSQL 적재 task. */
  DATA_SYNC_FAILURE,

  /** 확진농가 HPAI 화면 표출 누락 — Airflow가 아닌 운영자 수동 등록 유형. */
  HPAI_DISPLAY_MISSING,

  /** task_id 매핑이 없는 미분류 에러. */
  UNKNOWN
}
