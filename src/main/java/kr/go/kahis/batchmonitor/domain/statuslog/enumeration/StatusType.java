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
 *       AUTO_VERIFIED    MANUAL_REVIEW_REQUIRED (종결, 이후 추적 없음)
 *           │                 ▲
 *           ▼                 │
 *  Airflow Mark Success API   │
 *           │                 │
 *      ┌────┴────┐            │
 *      ▼         ▼            │
 *   성공      실패             │
 *      │         │            │
 *      ▼         ▼            │
 *  AUTO_MARK_  AUTO_MARK_     │
 *  SUCCESS     SUCCESS_FAILED ┘ (호출 실패 시 운영자 수동 개입 필요)
 *  (종결)      (종결)
 * </pre>
 *
 * <p>운영 SM이 Airflow UI에서 수행하는 "Mark as Success" 동작을 자동화한다.
 * task 코드를 재실행하지 않고 task instance state만 success로 전이시킨다.
 */
public enum StatusType {

  /** Airflow callback 수신 직후 최초 상태. */
  RECEIVED,

  /** 자동 검증 진행 중 (HIST 조회 등). */
  AUTO_VERIFYING,

  /** 자동 검증 정상 판단 (중간 상태 — 다음 단계로 Mark Success API 호출이 이어짐). */
  AUTO_VERIFIED,

  /** 자동 검증 정상 판단 → Airflow Mark Success API 호출 성공. (종결) */
  AUTO_MARK_SUCCESS,

  /** 자동 검증 정상 판단했으나 Airflow Mark Success API 호출 실패 → 운영자 수동 개입 필요. (종결) */
  AUTO_MARK_SUCCESS_FAILED,

  /** 자동 검증 비정상 판단 또는 자동 검증 미지원 → 운영자가 Airflow UI에서 직접 처리. (종결) */
  MANUAL_REVIEW_REQUIRED
}