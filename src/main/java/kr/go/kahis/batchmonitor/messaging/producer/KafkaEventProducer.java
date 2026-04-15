package kr.go.kahis.batchmonitor.messaging.producer;

import java.time.LocalDateTime;
import java.util.Map;
import kr.go.kahis.batchmonitor.common.enumeration.ErrorType;
import kr.go.kahis.batchmonitor.messaging.dto.KafkaEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventProducer {

  private final KafkaTemplate<String, KafkaEvent> kafkaTemplate;

  public void publish(String eventId, String dagId, String taskId, ErrorType errorType,
      String errorMessage, Map<String, String> metadata) {
    String topic = errorType.getTopic();
    LocalDateTime now = LocalDateTime.now();
    String key = dagId + "-" + taskId + "-" + now.toLocalDate().toString();
    KafkaEvent event = new KafkaEvent(eventId, dagId, taskId, errorType, errorMessage,
        metadata, now);

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
