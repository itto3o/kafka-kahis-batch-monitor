package kr.go.kahis.batchmonitor.service;

import kr.go.kahis.batchmonitor.domain.airflow.dto.response.TaskClearResponse;
import kr.go.kahis.batchmonitor.domain.kafka.dto.KafkaEvent;

public interface AirflowService {

  TaskClearResponse clear(KafkaEvent event);
}
