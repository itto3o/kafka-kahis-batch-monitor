package kr.go.kahis.batchmonitor.service;

import java.time.ZoneOffset;
import java.util.List;
import kr.go.kahis.batchmonitor.domain.airflow.client.AirflowClient;
import kr.go.kahis.batchmonitor.domain.airflow.dto.request.TaskClearRequest;
import kr.go.kahis.batchmonitor.domain.airflow.dto.response.TaskClearResponse;
import kr.go.kahis.batchmonitor.domain.kafka.dto.KafkaEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class AirflowServiceImpl implements AirflowService {

  private final AirflowClient client;

  @Override
  public TaskClearResponse clear(KafkaEvent event) {
    TaskClearRequest request = new TaskClearRequest(
        false,
        List.of(event.taskId()),
        event.executionDate().atStartOfDay(ZoneOffset.UTC).toInstant().toString(),
        event.executionDate().plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toString(),
        true,
        true,
        true);

    return client.clear(event.dagId(), request);
  }
}
