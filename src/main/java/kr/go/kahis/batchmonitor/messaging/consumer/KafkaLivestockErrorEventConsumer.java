package kr.go.kahis.batchmonitor.messaging.consumer;

import kr.go.kahis.batchmonitor.messaging.dto.KafkaEvent;
import kr.go.kahis.batchmonitor.persistence.enumeration.StatusType;
import kr.go.kahis.batchmonitor.service.PersistenceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaLivestockErrorEventConsumer implements KafkaConsumer {

  private final PersistenceService persistenceService;

  @KafkaListener(
      topics = "#{T(kr.go.kahis.batchmonitor.common.enumeration.ErrorType).LIVESTOCK_ANOMALY.topic}",
      groupId = "${spring.kafka.consumer.group-id}"
  )
  public void consume(KafkaEvent event, Acknowledgment acknowledgment) {
    // 로그 저장
    persistenceService.save(event.eventId(), event.dagId(), event.taskId(), event.errorType(),
        event.errorMessage(), event.metadata().toString(), StatusType.AUTO_VERIFYING, null, null);

    // 분석


    // 분석 결과 로그 저장

    acknowledgment.acknowledge();
  }
}
