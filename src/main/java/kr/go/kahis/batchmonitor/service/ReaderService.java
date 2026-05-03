package kr.go.kahis.batchmonitor.service;

import kr.go.kahis.batchmonitor.messaging.dto.KafkaEvent;

public interface ReaderService {

  /**
   * 분석에 필요한 사육 두수를 조회하고, 분석한다.
   *
   * @param farmNumber 농장 일련 번호
   */
  void analysis(KafkaEvent event, String farmNumber, String speciesCode, long currentCount);
}
