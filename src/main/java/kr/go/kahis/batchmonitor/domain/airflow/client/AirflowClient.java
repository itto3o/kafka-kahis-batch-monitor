package kr.go.kahis.batchmonitor.domain.airflow.client;

import kr.go.kahis.batchmonitor.common.config.AirflowClientConfig;
import kr.go.kahis.batchmonitor.domain.airflow.dto.request.TaskMarkSuccessRequest;
import kr.go.kahis.batchmonitor.domain.airflow.dto.response.TaskMarkSuccessResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(
    name = "airflowClient",
    url = "${airflow.api.url}",
    configuration = AirflowClientConfig.class
)
public interface AirflowClient {
  @PostMapping(value = "/dags/{dag_id}/updateTaskInstancesState", consumes = MediaType.APPLICATION_JSON_VALUE)
  TaskMarkSuccessResponse markSuccess(@PathVariable("dag_id") String dagId,
      @RequestBody TaskMarkSuccessRequest request);
}