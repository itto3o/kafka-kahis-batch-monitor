package kr.go.kahis.batchmonitor.domain.airflow.dto.response;

import java.util.List;
import java.util.Map;

/**
 * Airflow {@code updateTaskInstancesState} API의 응답.
 *
 * <p>상태 전이된 task instance 목록을 반환. 빈 리스트면 매칭된 task가 없어 실제로 아무 것도 전이되지 않은 것.
 */
public record TaskMarkSuccessResponse(
    List<Map<String, Object>> taskInstances
) {

}