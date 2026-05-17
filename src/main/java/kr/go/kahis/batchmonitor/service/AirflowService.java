package kr.go.kahis.batchmonitor.service;

import kr.go.kahis.batchmonitor.domain.airflow.dto.response.TaskMarkSuccessResponse;
import kr.go.kahis.batchmonitor.domain.kafka.dto.KafkaEvent;

public interface AirflowService {

  TaskMarkSuccessResponse markSuccess(KafkaEvent event);
}