package kr.go.kahis.batchmonitor.messaging.consumer;

import kr.go.kahis.batchmonitor.messaging.dto.KafkaEvent;
import kr.go.kahis.batchmonitor.persistence.enumeration.JudgementType;
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
public class KafkaEventConsumer implements KafkaConsumer {

  private final PersistenceService persistenceService;

  @KafkaListener(
      topics = "#{T(kr.go.kahis.batchmonitor.common.enumeration.ErrorType).notAnalysisTopics()}",
      groupId = "${spring.kafka.consumer.group-id}"
  )
  public void consume(KafkaEvent event, Acknowledgment acknowledgment) {
    // 로그 저장
    persistenceService.save(event.eventId(), event.dagId(), event.taskId(), event.errorType(),
        event.errorMessage(), event.metadata().toString(), StatusType.MANUAL_REVIEW_REQUIRED,
        JudgementType.UNKNOWN, "운영자 검증이 필요한 에러 유형입니다.");

    // 분석 없이 공통 처리
    log.info("receive not need analysis error: eventId={}, type={}", event.eventId(),
        event.errorType());

    acknowledgment.acknowledge();
  }
}
