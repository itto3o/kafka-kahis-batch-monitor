package kr.go.kahis.batchmonitor.domain.airflow.client;

import kr.go.kahis.batchmonitor.domain.airflow.dto.request.TaskClearRequest;
import kr.go.kahis.batchmonitor.domain.airflow.dto.response.TaskClearResponse;
import kr.go.kahis.batchmonitor.common.config.OpenFeignConfig;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(
    name = "airflowClient",
    url = "${airflow.url}",
    configuration = OpenFeignConfig.class
)
public class AirflowClient {

  @PostMapping(value = "/dags/{dag_id}/clearTaskInstances", consumes = MediaType.APPLICATION_JSON_VALUE)
  TaskClearResponse clear(@PathVariable("dag_id") String dagId,
      @RequestBody TaskClearRequest request) {

    return null;
  }
}
