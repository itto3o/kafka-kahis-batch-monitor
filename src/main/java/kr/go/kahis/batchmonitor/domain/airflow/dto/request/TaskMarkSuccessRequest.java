package kr.go.kahis.batchmonitor.domain.airflow.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TaskMarkSuccessRequest(
  // true면 실제 상태 전이를 하지 않고 영향받을 task instance 목록만 반환
  @JsonProperty("dry_run")
  Boolean dryRun,

  // 대상 task id
  @JsonProperty("task_id")
  String taskId,

  // 대상 DAG run id (execution_date와 상호배타. 이 프로젝트는 dag_run_id 사용)
  @JsonProperty("dag_run_id")
  String dagRunId,

  // 새 state. Mark as Success 자동화이므로 항상 "success"
  @JsonProperty("new_state")
  String newState,

  // upstream task까지 함께 success 처리할지 여부
  @JsonProperty("include_upstream")
  Boolean includeUpstream,

  // downstream task까지 함께 success 처리할지 여부 (자동화 시 true 필수)
  @JsonProperty("include_downstream")
  Boolean includeDownstream,

  // 동일 task의 과거 실행분도 함께 success 처리할지 여부
  @JsonProperty("include_past")
  Boolean includePast,

  // 동일 task의 미래 실행분도 함께 success 처리할지 여부
  @JsonProperty("include_future")
  Boolean includeFuture
) {

}