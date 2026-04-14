package kr.go.kahis.batchmonitor.dto;

import java.time.LocalDate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.BindParam;

public record ErrorRequest(

    @BindParam("dag_id")
    String dagId,

    @BindParam("task_id")
    String taskId,

    @BindParam("execution_date")
    @DateTimeFormat(pattern = "yyyy-MM-dd")
    LocalDate executionDate,

    @BindParam("error_message")
    String errorMessage,

    @BindParam("try_number")
    String tryNumber
) {

}
