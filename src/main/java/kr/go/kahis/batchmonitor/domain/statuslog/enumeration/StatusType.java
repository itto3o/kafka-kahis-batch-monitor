package kr.go.kahis.batchmonitor.domain.statuslog.enumeration;

/**
 * 배치 에러 이벤트의 처리 상태.
 *
 * <p>{@code StatusLog}는 append-only이며, 상태가 전이될 때마다 새 row가 insert 됩니다.
 *
 * <pre>
 *                 RECEIVED
 *                    │
 *                    ▼
 *              AUTO_VERIFYING (자동 검증 가능 유형만)
 *                    │
 *           ┌────────┴────────┐
 *           ▼                 ▼
 *      AUTO_CLEARED      MANUAL_REVIEW_REQUIRED (종결, 이후 추적 없음)
 *      (종결)                  ▲
 *           ▲                  │
 *           │                  │
 *      AUTO_CLEAR_FAILED ──────┘ (Clear API 실패 시 운영자 수동 개입 필요)
 *      (종결)
 * </pre>
 */
public enum StatusType {

  /** Airflow callback 수신 직후 최초 상태. */
  RECEIVED,

  /** 자동 검증 진행 중 (HIST 조회 등). */
  AUTO_VERIFYING,

  /** 자동 검증 정상 판단 */
  AUTO_CLEARED,

  /** 자동 검증 정상 판단 → Airflow Clear API 호출 성공. (종결) */
  AUTO_CLEAR_SUCCESS,

  /** 자동 검증 정상 판단했으나 Airflow Clear API 호출 실패 → 운영자 수동 개입 필요. (종결) */
  AUTO_CLEAR_FAILED,

  /** 자동 검증 비정상 판단 또는 자동 검증 미지원 → 운영자가 Airflow UI에서 직접 처리. (종결) */
  MANUAL_REVIEW_REQUIRED
}
