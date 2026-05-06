package kr.go.kahis.batchmonitor.messaging.consumer;

import kr.go.kahis.batchmonitor.messaging.dto.KafkaEvent;
import kr.go.kahis.batchmonitor.persistence.entity.StatusLog;
import kr.go.kahis.batchmonitor.persistence.enumeration.StatusType;
import kr.go.kahis.batchmonitor.persistence.unit.StatusLogUnit;
import kr.go.kahis.batchmonitor.service.ReaderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaLivestockErrorEventConsumer implements KafkaConsumer {

  private final StatusLogUnit statusLogUnit;
  private final ReaderService readerService;

  @KafkaListener(
      topics = "#{T(kr.go.kahis.batchmonitor.common.enumeration.ErrorType).LIVESTOCK_ANOMALY.topic}",
      groupId = "${spring.kafka.consumer.group-id}"
  )
  public void consume(KafkaEvent event, Acknowledgment acknowledgment) {
    // 로그 저장
    statusLogUnit.create(StatusLog.builder()
        .eventId(event.eventId())
        .dagId(event.dagId())
        .taskId(event.taskId())
        .lsfarmId(null)
        .errorType(event.errorType())
        .errorMessage(event.errorMessage())
        .metadata(event.metadata().toString())
        .statusType(StatusType.AUTO_VERIFYING)
        .judgementType(null)
        .reason(null)
        .build());

    try {
      readerService.analysis(event, event.metadata().get("farmNumber"),
          event.metadata().get("speciesCode"), Long.parseLong(event.metadata().get("currentValue")));
    } catch (Exception e) {
      log.error("Unexpected error during livestock analysis: {}", event.eventId(), e);
    } finally {
      acknowledgment.acknowledge();
    }
  }
}
