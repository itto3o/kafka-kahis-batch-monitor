package kr.go.kahis.batchmonitor.domain.airflow.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record TaskClearRequest(
    @JsonProperty("dry_run")
    Boolean dryRun,

    @JsonProperty("task_ids")
    List<String> taskIds,

    @JsonProperty("start_date")
    String startDate,

    @JsonProperty("end_date")
    String endDate,

    @JsonProperty("only_failed")
    Boolean onlyFailed,

    @JsonProperty("reset_dag_runs")
    Boolean resetDagRuns
) {

}
