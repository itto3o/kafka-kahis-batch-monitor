package kr.go.kahis.batchmonitor.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ErrorRequest(

    @JsonProperty("dag_id")
    String dagId,

    @JsonProperty("task_id")
    String taskId,

    @JsonProperty("dag_run_id")
    String dagRunId,

    @JsonProperty("error_message")
    String errorMessage,

    @JsonProperty("try_number")
    String tryNumber
) {

}