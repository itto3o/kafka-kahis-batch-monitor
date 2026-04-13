package kr.go.kahis.batchmonitor.persistence.enumeration;

/**
 * 자동 검증의 판단 결과.
 *
 * <p>HIST 조회 등 자동 검증을 수행한 row에서만 채워지며, 검증을 수행하지 않는 errorType은
 * 이 값이 비어 있을 수 있습니다.
 */
public enum JudgementType {

  /** 정상으로 보임 — 자동 Clear 대상. */
  LIKELY_NORMAL,

  /** 비정상으로 보임 — 운영자 수동 처리 대상. */
  LIKELY_ANOMALY,

  /** 자동 판단 불가 — 운영자 확인 필요. */
  UNKNOWN
}
