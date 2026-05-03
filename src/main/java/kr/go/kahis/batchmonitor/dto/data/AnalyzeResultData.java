package kr.go.kahis.batchmonitor.dto.data;

import kr.go.kahis.batchmonitor.persistence.enumeration.JudgementType;

public record AnalyzeResultData(
  JudgementType judgementType,
  String reason
) {

}
