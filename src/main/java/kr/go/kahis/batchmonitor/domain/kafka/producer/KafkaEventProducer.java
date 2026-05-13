package kr.go.kahis.batchmonitor.domain.kafka.producer;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import kr.go.kahis.batchmonitor.common.enumeration.ErrorType;
import kr.go.kahis.batchmonitor.domain.kafka.dto.KafkaEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventProducer {

  private final KafkaTemplate<String, KafkaEvent> kafkaTemplate;

  public void publish(String eventId, String dagId, String taskId, LocalDate executionDate,
      ErrorType errorType, String errorMessage, Map<String, String> metadata) {
    String topic = errorType.getTopic();
    LocalDateTime now = LocalDateTime.now();
    String key = dagId + "-" + taskId + "-" + now.toLocalDate().toString();
    KafkaEvent event = new KafkaEvent(eventId, dagId, taskId, executionDate, errorType,
        errorMessage, metadata, now);

    kafkaTemplate.send(topic, key, event)
        .whenComplete((result, throwable) -> {
          if (throwable != null) {
            log.error("Kafka publish error: topic={}, eventId={}", topic, eventId, throwable);
            return;
          }

          log.info("Kafka publish success: topic={}, partition={}, offset={}, eventId={}", topic,
              result.getRecordMetadata().partition(), result.getRecordMetadata().offset(), eventId);
        });
  }

}
