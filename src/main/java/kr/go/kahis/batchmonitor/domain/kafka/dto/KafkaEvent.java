package kr.go.kahis.batchmonitor.domain.kafka.dto;

import java.time.LocalDateTime;
import java.util.Map;
import kr.go.kahis.batchmonitor.common.enumeration.ErrorType;

public record KafkaEvent(
    String eventId,
    String dagId,
    String taskId,
    String dagRunId,
    ErrorType errorType,
    String errorMessage,
    Map<String, String> metadata,
    LocalDateTime occurredAt
) {

}