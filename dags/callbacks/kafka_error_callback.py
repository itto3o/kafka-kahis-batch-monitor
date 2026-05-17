"""
Kafka 배치 에러 알림 공통 콜백 모듈

[사용법]
1. 이 파일을 dags/callbacks/ 폴더에 배치합니다.
2. DAG 파일에서 아래와 같이 import 후 사용합니다.

    === 방법 1: DAG 전체에 적용 (default_args) ===

    from callbacks.kafka_error_callback import on_task_failure

    with DAG(
        dag_id="make_risk_data",
        default_args={"on_failure_callback": on_task_failure},
        ...
    ) as dag:
        ...

    === 방법 2: 특정 Task에만 적용 ===

    from callbacks.kafka_error_callback import on_task_failure

    check_task = PythonOperator(
        task_id="check_tb_livestock_species_information",
        python_callable=some_function,
        on_failure_callback=on_task_failure,
    )

[설정]
    Airflow Variable에 아래 값을 등록해야 합니다.
    - kafka_error_callback_url: Spring Boot API URL (예: http://spring-boot-host:8080/api/v1/errors)

    등록 방법 (Airflow UI):
    Admin > Variables > + 버튼
    Key: kafka_error_callback_url
    Val: http://spring-boot-host:8080/api/v1/errors
"""

import json
import logging
from datetime import datetime

import requests
from airflow.models.variable import Variable

logger = logging.getLogger(__name__)

# Spring Boot API URL (Airflow Variable에서 읽음)
_CALLBACK_URL = None


def _get_callback_url():
    global _CALLBACK_URL
    if _CALLBACK_URL is None:
        _CALLBACK_URL = Variable.get(
            "kafka_error_callback_url",
            default_var="http://localhost:8080/api/v1/errors",
        )
    return _CALLBACK_URL


def on_task_failure(context):
    """
    Task 실패 시 Spring Boot API로 에러 정보를 전송하는 공통 콜백.

    Airflow context에서 dag_id, task_id, dag_run_id, exception 정보를 추출하여
    Spring Boot → Kafka 토픽으로 에러 이벤트를 발행합니다.
    """
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    dag_run = context.get("dag_run")

    payload = {
        "dag_id": context["dag"].dag_id,
        "task_id": task_instance.task_id,
        "dag_run_id": dag_run.run_id,
        "error_message": str(exception) if exception else "",
        "try_number": task_instance.try_number,
        "task_state": str(task_instance.state),
        "log_url": task_instance.log_url,
    }

    url = _get_callback_url()

    try:
        response = requests.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        logger.info(
            "[kafka_error_callback] 에러 전송 성공: %s/%s (status=%s)",
            payload["dag_id"],
            payload["task_id"],
            response.status_code,
        )
    except requests.exceptions.RequestException as e:
        # 콜백 실패가 원래 Task 실패에 영향을 주지 않도록 예외를 삼킴
        logger.error(
            "[kafka_error_callback] 에러 전송 실패: %s/%s - %s",
            payload["dag_id"],
            payload["task_id"],
            str(e),
        )