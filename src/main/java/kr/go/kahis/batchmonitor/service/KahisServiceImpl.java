package kr.go.kahis.batchmonitor.service;

import kr.go.kahis.batchmonitor.dto.data.AnalyzeResultData;
import kr.go.kahis.batchmonitor.domain.kafka.dto.KafkaEvent;
import kr.go.kahis.batchmonitor.domain.statuslog.entity.StatusLog;
import kr.go.kahis.batchmonitor.domain.statuslog.enumeration.JudgementType;
import kr.go.kahis.batchmonitor.domain.statuslog.enumeration.StatusType;
import kr.go.kahis.batchmonitor.domain.statuslog.unit.StatusLogUnit;
import kr.go.kahis.batchmonitor.vo.LivestockHistoryAnalyzer;
import kr.go.kahis.batchmonitor.vo.LsFarmIdFinder;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KahisServiceImpl implements KahisService {

  private final LivestockHistoryAnalyzer analyzer;
  private final LsFarmIdFinder finder;
  private final StatusLogUnit statusLogUnit;

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

    // 로그 저장
    if (analyzeResult.judgementType() == JudgementType.LIKELY_NORMAL) {
      statusLogUnit.create(StatusLog.builder()
          .eventId(event.eventId())
          .dagId(event.dagId())
          .taskId(event.taskId())
          .lsfarmId(lsFarmId)
          .errorType(event.errorType())
          .errorMessage(event.errorMessage())
          .metadata(event.metadata().toString())
          .statusType(StatusType.AUTO_CLEARED)
          .judgementType(analyzeResult.judgementType())
          .reason(analyzeResult.reason())
          .build());
    } else {
      statusLogUnit.create(StatusLog.builder()
          .eventId(event.eventId())
          .dagId(event.dagId())
          .taskId(event.taskId())
          .lsfarmId(lsFarmId)
          .errorType(event.errorType())
          .errorMessage(event.errorMessage())
          .metadata(event.metadata().toString())
          .statusType(StatusType.MANUAL_REVIEW_REQUIRED)
          .judgementType(analyzeResult.judgementType())
          .reason(analyzeResult.reason())
          .build());
    }

    // TODO:  clear면 clear. 운영 환경에서 신뢰성 확보 후 airflow clear
  }
}
