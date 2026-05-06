package kr.go.kahis.batchmonitor.dto.request;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDate;

public record ErrorRequest(

    @JsonProperty("dag_id")
    String dagId,

    @JsonProperty("task_id")
    String taskId,

    @JsonProperty("execution_date")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    LocalDate executionDate,

    @JsonProperty("error_message")
    String errorMessage,

    @JsonProperty("try_number")
    String tryNumber
) {

}
