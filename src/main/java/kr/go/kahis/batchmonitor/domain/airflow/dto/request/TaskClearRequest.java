package kr.go.kahis.batchmonitor.domain.airflow.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record TaskClearRequest(
  // true면 실제 clear를 수행하지 않고 영향받을 task instance 목록만 반환
  @JsonProperty("dry_run")
  Boolean dryRun,

  // clear 대상 task id 목록 (비어있거나 null이면 DAG 내 전체 task 대상)
  @JsonProperty("task_ids")
  List<String> taskIds,

  // 이 시각(포함) 이후의 task instance만 clear (ISO-8601 형식)
  @JsonProperty("start_date")
  String startDate,

  // 이 시각(포함) 이전의 task instance만 clear (ISO-8601 형식)
  @JsonProperty("end_date")
  String endDate,

  // true면 failed 상태의 task instance만 clear
  @JsonProperty("only_failed")
  Boolean onlyFailed,

  // true면 해당 task가 속한 DAG run의 상태도 함께 running으로 reset
  @JsonProperty("reset_dag_runs")
  Boolean resetDagRuns,

  // true면 taskIds로 지정한 task의 downstream task들도 함께 clear (의존 chain 전체 재실행 시 필요)
  @JsonProperty("include_downstream")
  Boolean includeDownstream
) {

}
