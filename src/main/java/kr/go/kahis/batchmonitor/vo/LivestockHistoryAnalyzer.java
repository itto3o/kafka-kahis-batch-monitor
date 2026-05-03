package kr.go.kahis.batchmonitor.vo;

import java.util.Comparator;
import java.util.List;
import kr.go.kahis.batchmonitor.dto.data.AnalyzeResultData;
import kr.go.kahis.batchmonitor.persistence.enumeration.JudgementType;
import kr.go.kahis.batchmonitor.reader.dto.MobileBreedingLivestockHistoryDto;
import kr.go.kahis.batchmonitor.reader.mapper.FarmMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class LivestockHistoryAnalyzer {

  private final FarmMapper mapper;

  /**
   * 사육두수 히스토리 데이터 분석
   */
  public AnalyzeResultData analyze(String farmId, String speciesCode, long currentCount) {
    // 1. 사육두수 히스토리 조회
    List<MobileBreedingLivestockHistoryDto> history = mapper.selectMobileBreedingLivestockHistory(
        farmId, speciesCode);

    // history가 없으면 신규 농장으로 판단. UNKNOWN으로 추정 후 종료
    if (history.isEmpty()) {
      return new AnalyzeResultData(JudgementType.UNKNOWN, "HIST 부재");
    }

    // 히스토리 파악
    List<Long> counts = history.stream().map(MobileBreedingLivestockHistoryDto::brdHadCo).toList();

    // 히스토리가 없으면 UNKNOWN으로 추정 후 종료
    if (counts.isEmpty()) {
      return new AnalyzeResultData(JudgementType.UNKNOWN, "HIST 부재");
    }

    // tolerance
    double low = currentCount * 0.5;
    double high = currentCount * 2.0;

    List<Long> matched = counts.stream()
        .filter(count -> count >= low && count <= high)
        .toList();

    if (!matched.isEmpty()) {
      long mostRecent = matched.get(0);
      long closest = matched.stream()
          // 당일값과 가장 적게 차이나는 수 중에서 min(가장 근사치)
          .min(Comparator.comparingLong(close -> Math.abs(close - currentCount)))
          .orElseThrow();
      return new AnalyzeResultData(
          JudgementType.LIKELY_NORMAL,
          "HIST 정상 — 당일값 " + currentCount + ", " + "허용범위(×0.5 ~ ×2.0) 내 매칭값 존재 (최근= "
              + mostRecent + ", 최근접= " + closest + ")");
    }

    return new AnalyzeResultData(
        JudgementType.LIKELY_ANOMALY,
        "HIST 정상 — 당일값 " + currentCount + ", " + "허용범위(×0.5 ~ ×2.0) 내 매칭값 미존재");
  }
}
