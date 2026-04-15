package kr.go.kahis.batchmonitor.service;

import jakarta.transaction.Transactional;
import java.time.LocalDateTime;
import kr.go.kahis.batchmonitor.dto.ErrorRequest;
import kr.go.kahis.batchmonitor.messaging.producer.KafkaEventProducer;
import kr.go.kahis.batchmonitor.parser.ParsedError;
import kr.go.kahis.batchmonitor.parser.ParserUtil;
import kr.go.kahis.batchmonitor.persistence.entity.StatusLog;
import kr.go.kahis.batchmonitor.persistence.enumeration.StatusType;
import kr.go.kahis.batchmonitor.persistence.repository.StatusLogRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PersistenceServiceImpl implements PersistenceService {

  private final StatusLogRepository statusLogRepository;
  private final KafkaEventProducer kafkaEventProducer;

  /**
   * airflow error메시지를 수신하여 kafka에 event를 publish 한다.
   *
   * @param dto dto
   */
  @Override
  @Transactional
  public void publish(ErrorRequest dto) {
    ParsedError parsed = ParserUtil.parse(dto.errorMessage());
    int nowNano = LocalDateTime.now().getNano();
    String eventId = dto.taskId() + nowNano;

    // 로그 저장
    statusLogRepository.save(StatusLog.builder()
        .eventId(eventId)
        .dagId(dto.dagId())
        .taskId(dto.taskId())
        .errorType(parsed.errorType())
        .errorMessage(dto.errorMessage())
        .metadata(parsed.metadata().toString())
        .statusType(StatusType.RECEIVED)
        .judgementType(null)
        .reason(null)
        .build());

    // publish
    kafkaEventProducer.publish(eventId, dto.dagId(), dto.taskId(), parsed.errorType(),
        dto.errorMessage(), parsed.metadata());

  }
}
