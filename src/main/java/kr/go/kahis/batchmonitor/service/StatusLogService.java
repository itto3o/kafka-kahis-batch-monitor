package kr.go.kahis.batchmonitor.service;

import kr.go.kahis.batchmonitor.dto.request.ErrorRequest;

public interface StatusLogService {

  /**
   * airflow error메시지를 수신하여 kafka에 event를 publish 한다.
   *
   * @param dto dto
   */
  void publish(ErrorRequest dto);
}
