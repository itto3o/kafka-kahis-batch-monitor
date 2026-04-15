package kr.go.kahis.batchmonitor.service;

import jakarta.transaction.Transactional;
import java.time.LocalDateTime;
import kr.go.kahis.batchmonitor.common.enumeration.ErrorType;
import kr.go.kahis.batchmonitor.dto.ErrorRequest;
import kr.go.kahis.batchmonitor.messaging.producer.KafkaEventProducer;
import kr.go.kahis.batchmonitor.parser.ParsedError;
import kr.go.kahis.batchmonitor.parser.ParserUtil;
import kr.go.kahis.batchmonitor.persistence.entity.StatusLog;
import kr.go.kahis.batchmonitor.persistence.enumeration.JudgementType;
import kr.go.kahis.batchmonitor.persistence.enumeration.StatusType;
import kr.go.kahis.batchmonitor.persistence.repository.StatusLogRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PersistenceServiceImpl implements PersistenceService {

  private final StatusLogRepository statusLogRepository;
  private final KafkaEventProducer producer;

  /**
   * 로그 저장 함수
   *
   * @param eventId       이벤트 id (taskId-millisecond)
   * @param dagId         dag id
   * @param taskId        task id
   * @param errorType     에러 타입
   * @param errorMessage  에러 메시지
   * @param metadata      메타 데이터
   * @param statusType    상태 타입
   * @param judgementType 판단 타입
   * @param reason        판단 이유
   */
  @Override
  @Transactional
  public void save(String eventId, String dagId, String taskId, ErrorType errorType,
      String errorMessage, String metadata, StatusType statusType, JudgementType judgementType,
      String reason) {
    statusLogRepository.save(StatusLog.builder()
        .eventId(eventId)
        .dagId(dagId)
        .taskId(taskId)
        .errorType(errorType)
        .errorMessage(errorMessage)
        .metadata(metadata)
        .statusType(statusType)
        .judgementType(judgementType)
        .reason(reason)
        .build());
  }

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
    save(eventId, dto.dagId(), dto.taskId(), parsed.errorType(), dto.errorMessage(),
        parsed.metadata().toString(), StatusType.RECEIVED, null, null);

    // publish
    producer.publish(eventId, dto.dagId(), dto.taskId(), parsed.errorType(),
        dto.errorMessage(), parsed.metadata());

  }
}
