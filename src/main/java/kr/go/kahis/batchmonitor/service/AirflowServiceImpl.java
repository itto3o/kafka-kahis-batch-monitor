package kr.go.kahis.batchmonitor.service;

import kr.go.kahis.batchmonitor.domain.airflow.client.AirflowClient;
import kr.go.kahis.batchmonitor.domain.airflow.dto.request.TaskMarkSuccessRequest;
import kr.go.kahis.batchmonitor.domain.airflow.dto.response.TaskMarkSuccessResponse;
import kr.go.kahis.batchmonitor.domain.kafka.dto.KafkaEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class AirflowServiceImpl implements AirflowService {

  private final AirflowClient client;

  @Override
  public TaskMarkSuccessResponse markSuccess(KafkaEvent event) {
    TaskMarkSuccessRequest request = new TaskMarkSuccessRequest(
        false,
        event.taskId(),
        event.dagRunId(),
        "success",
        false,
        false,
        false,
        false);

    return client.markSuccess(event.dagId(), request);
  }
}