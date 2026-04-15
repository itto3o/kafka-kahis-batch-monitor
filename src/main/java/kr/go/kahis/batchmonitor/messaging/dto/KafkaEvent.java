package kr.go.kahis.batchmonitor.messaging.dto;

import java.time.LocalDateTime;
import java.util.Map;
import kr.go.kahis.batchmonitor.common.enumeration.ErrorType;

public record KafkaEvent(
    String eventId,
    String dagId,
    String taskId,
    ErrorType errorType,
    String errorMessage,
    Map<String, String> metadata,
    LocalDateTime occurredAt
) {

}
