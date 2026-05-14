package kr.go.kahis.batchmonitor.service;

import kr.go.kahis.batchmonitor.domain.airflow.dto.response.TaskClearResponse;
import kr.go.kahis.batchmonitor.dto.data.AnalyzeResultData;
import kr.go.kahis.batchmonitor.domain.kafka.dto.KafkaEvent;
import kr.go.kahis.batchmonitor.domain.statuslog.entity.StatusLog;
import kr.go.kahis.batchmonitor.domain.statuslog.enumeration.JudgementType;
import kr.go.kahis.batchmonitor.domain.statuslog.enumeration.StatusType;
import kr.go.kahis.batchmonitor.domain.statuslog.unit.StatusLogUnit;
import kr.go.kahis.batchmonitor.vo.LivestockHistoryAnalyzer;
import kr.go.kahis.batchmonitor.vo.LsFarmIdFinder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KahisServiceImpl implements KahisService {

  private final LivestockHistoryAnalyzer analyzer;
  private final LsFarmIdFinder finder;
  private final StatusLogUnit statusLogUnit;
  private final AirflowService airflowService;

  /**
   * 분석에 필요한 사육 두수를 조회하고, 분석한다.
   *
   * @param farmNumber 농장 일련 번호
   */
  @Override
  public void analysis(KafkaEvent event, String farmNumber, String speciesCode, long currentCount) {
    // 분석
    AnalyzeResultData analyzeResult = analyzer.analyze(farmNumber, speciesCode, currentCount);

    // lsfarmId 조회
    String lsFarmId = finder.find(farmNumber);

    // 비정상/판단불가 → 운영자 수동 처리로 종결
    if (analyzeResult.judgementType() != JudgementType.LIKELY_NORMAL) {
      saveStatusLog(event, lsFarmId, analyzeResult, StatusType.MANUAL_REVIEW_REQUIRED);
      return;
    }

    // 정상 판단 로그 저장
    saveStatusLog(event, lsFarmId, analyzeResult, StatusType.AUTO_CLEARED);

    // Airflow Clear API 호출 및 결과 로그 저장
    StatusType clearStatus;
    try {
      TaskClearResponse clearResponse = airflowService.clear(event);
      clearStatus = clearResponse.taskInstances().isEmpty()
          ? StatusType.AUTO_CLEAR_FAILED
          : StatusType.AUTO_CLEAR_SUCCESS;
    } catch (RuntimeException e) {
      log.error("Airflow Clear API 호출 실패: eventId={}", event.eventId(), e);
      clearStatus = StatusType.AUTO_CLEAR_FAILED;
    }
    saveStatusLog(event, lsFarmId, analyzeResult, clearStatus);
  }

  private void saveStatusLog(KafkaEvent event, String lsFarmId, AnalyzeResultData analyzeResult,
      StatusType statusType) {
    statusLogUnit.create(StatusLog.builder()
        .eventId(event.eventId())
        .dagId(event.dagId())
        .taskId(event.taskId())
        .lsfarmId(lsFarmId)
        .errorType(event.errorType())
        .errorMessage(event.errorMessage())
        .metadata(event.metadata().toString())
        .statusType(statusType)
        .judgementType(analyzeResult.judgementType())
        .reason(analyzeResult.reason())
        .build());
  }
}
