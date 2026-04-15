package kr.go.kahis.batchmonitor.service;

import kr.go.kahis.batchmonitor.common.enumeration.ErrorType;
import kr.go.kahis.batchmonitor.dto.ErrorRequest;
import kr.go.kahis.batchmonitor.persistence.enumeration.JudgementType;
import kr.go.kahis.batchmonitor.persistence.enumeration.StatusType;

public interface PersistenceService {

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
  void save(String eventId, String dagId, String taskId, ErrorType errorType,
      String errorMessage, String metadata, StatusType statusType, JudgementType judgementType,
      String reason);

  /**
   * airflow error메시지를 수신하여 kafka에 event를 publish 한다.
   * @param dto dto
   */
  void publish(ErrorRequest dto);
}
