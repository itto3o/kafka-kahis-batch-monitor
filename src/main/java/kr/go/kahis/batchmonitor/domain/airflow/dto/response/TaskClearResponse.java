package kr.go.kahis.batchmonitor.domain.airflow.dto.response;

import java.util.List;
import java.util.Map;

public record TaskClearResponse(
    List<Map<String, Object>> taskInstances
) {

}
