"""
E2E: 사육두수 비교 에러 시나리오 — Spring Boot의 자동 Mark Success 흐름을 끝까지 검증.

세 시나리오를 TaskGroup으로 분리:
  - normal:       farm 20418398 / 415002, 어제 30 / 오늘 3500
                  → check가 100배 초과로 ValueError raise → callback이 Spring 호출
                  → KahisServiceImpl이 HIST [3500,3000,4000]에서 매칭 발견 → LIKELY_NORMAL
                  → AirflowService.markSuccess() 호출 → after_check가 자동 재평가되어 성공
  - anomaly:      farm 00129932 / 412002, 어제 50 / 오늘 8000
                  → check raise → Spring → HIST [88,90,92] 매칭 없음 → LIKELY_ANOMALY
                  → MANUAL_REVIEW_REQUIRED 종결, Mark Success 호출 안 함 → after_check는 upstream_failed로 멈춤
  - chain_normal: normal 시나리오의 downstream 구조를 확장한 실험군.
                  목적: include_downstream=false인데도 자동 재평가가 어디까지 전파되는지 확인.
                    (a) 긴 체인 (s1→s2→s3): upstream_failed가 연쇄적으로 살아나는지
                    (b) trigger_rule='all_done' 비교군: 이미 success로 끝난 task는 재실행되지 않음을 확인
                        (자동 재평가는 'upstream_failed' 상태에만 작동한다는 가설 검증)

세 번째 시나리오(AUTO_MARK_SUCCESS_FAILED)는 normal 케이스를 실행하면서 `docker stop kahis-batch-monitor-airflow-webserver`
한 상태로 trigger하면 Spring의 Feign이 ConnectException을 던져 자동으로 검증된다. e2e-test-guide.md 참고.
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from callbacks.kafka_error_callback import on_task_failure
from ai_project.check_tb_livestock_species_information import (
    check_tb_livestock_species_information,
)

POSTGRES_CONN_ID = "postgres_local"
NORMAL_STD_DT = "2026-05-10"
ANOMALY_STD_DT = "2026-05-11"
CHAIN_STD_DT = "2026-05-12"


def setup_tables(**_):
    # geoai_logs.tb_livestock_species_information은 check 함수의 DatabaseHandler가 자동 생성하지만,
    # NORMAL/ANOMALY 두 task가 동시 실행 시 race condition으로 pg_type 인덱스 충돌이 발생한다.
    # 미리 생성해 두면 동시 실행되어도 IF NOT EXISTS가 NOP으로 떨어진다.
    sql = """
        CREATE SCHEMA IF NOT EXISTS geoai_mt;
        CREATE SCHEMA IF NOT EXISTS geoai_logs;
        CREATE TABLE IF NOT EXISTS geoai_mt.tb_livestock_species_information (
            standard_date VARCHAR(10),
            farm_serial_no VARCHAR(20),
            livestock_species_class_code VARCHAR(10),
            present_breeding_livestock_count NUMERIC,
            present_breeding_livestock_count_average NUMERIC
        );
        CREATE TABLE IF NOT EXISTS geoai_logs.tb_livestock_species_information (
            id SERIAL PRIMARY KEY,
            logLv VARCHAR,
            filename VARCHAR,
            lineno VARCHAR,
            message VARCHAR,
            create_dt TIMESTAMP DEFAULT now()
        );
    """
    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        conn.cursor().execute(sql)
        conn.commit()
    finally:
        conn.close()


def seed_scenario(std_dt, farm_no, species_code, prev_count, curr_count, **_):
    prev_dt = (pendulum.parse(std_dt) - pendulum.duration(days=1)).format("YYYY-MM-DD")
    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM geoai_mt.tb_livestock_species_information "
            "WHERE standard_date IN (%s, %s) AND farm_serial_no = %s",
            (std_dt, prev_dt, farm_no),
        )
        cur.execute(
            "INSERT INTO geoai_mt.tb_livestock_species_information VALUES (%s, %s, %s, %s, 0)",
            (prev_dt, farm_no, species_code, prev_count),
        )
        cur.execute(
            "INSERT INTO geoai_mt.tb_livestock_species_information VALUES (%s, %s, %s, %s, 0)",
            (std_dt, farm_no, species_code, curr_count),
        )
        conn.commit()
    finally:
        conn.close()


def run_check(std_dt, **_):
    # PostgresHook.get_uri()는 'postgresql+psycopg2://...' 를 반환하는데 psycopg2.connect는 그 prefix를 모름
    db_uri = (
        PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        .get_uri()
        .replace("postgresql+psycopg2", "postgresql")
    )
    check_tb_livestock_species_information(std_dt, db_uri)


def after_check(scenario, **_):
    print(
        f"=== '{scenario}' downstream task 실행됨 — Spring이 Airflow Mark Success API를 호출해 재평가되었음을 의미합니다 ==="
    )


def chain_step(label, **_):
    print(f"=== chain step '{label}' 실행됨 ===")


with DAG(
    dag_id="e2e_make_risk_data",
    description="E2E: 사육두수 비교 에러 시나리오 (NORMAL + ANOMALY)",
    default_args={"on_failure_callback": on_task_failure},
    start_date=pendulum.datetime(2026, 5, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["e2e", "test", "livestock"],
) as dag:
    setup = PythonOperator(
        task_id="setup_postgres_tables",
        python_callable=setup_tables,
    )

    with TaskGroup(group_id="normal") as normal_group:
        seed_n = PythonOperator(
            task_id="seed_data",
            python_callable=seed_scenario,
            op_kwargs={
                "std_dt": NORMAL_STD_DT,
                "farm_no": "20418398",
                "species_code": "415002",
                "prev_count": 30,
                "curr_count": 3500,
            },
        )
        check_n = PythonOperator(
            task_id="check_tb_livestock_species_information",
            python_callable=run_check,
            op_kwargs={"std_dt": NORMAL_STD_DT},
        )
        after_n = PythonOperator(
            task_id="after_check",
            python_callable=after_check,
            op_kwargs={"scenario": "NORMAL"},
        )
        seed_n >> check_n >> after_n

    with TaskGroup(group_id="anomaly") as anomaly_group:
        seed_a = PythonOperator(
            task_id="seed_data",
            python_callable=seed_scenario,
            op_kwargs={
                "std_dt": ANOMALY_STD_DT,
                "farm_no": "00129932",
                "species_code": "412002",
                "prev_count": 50,
                "curr_count": 8000,
            },
        )
        check_a = PythonOperator(
            task_id="check_tb_livestock_species_information",
            python_callable=run_check,
            op_kwargs={"std_dt": ANOMALY_STD_DT},
        )
        after_a = PythonOperator(
            task_id="after_check",
            python_callable=after_check,
            op_kwargs={"scenario": "ANOMALY"},
        )
        seed_a >> check_a >> after_a

    with TaskGroup(group_id="chain_normal") as chain_group:
        seed_c = PythonOperator(
            task_id="seed_data",
            python_callable=seed_scenario,
            op_kwargs={
                "std_dt": CHAIN_STD_DT,
                "farm_no": "20418398",
                "species_code": "415002",
                "prev_count": 30,
                "curr_count": 3500,
            },
        )
        check_c = PythonOperator(
            task_id="check_tb_livestock_species_information",
            python_callable=run_check,
            op_kwargs={"std_dt": CHAIN_STD_DT},
        )
        # (a) 긴 체인: check Mark Success 후 s1→s2→s3가 연쇄적으로 자동 재평가되는지
        step1 = PythonOperator(
            task_id="step1",
            python_callable=chain_step,
            op_kwargs={"label": "step1"},
        )
        step2 = PythonOperator(
            task_id="step2",
            python_callable=chain_step,
            op_kwargs={"label": "step2"},
        )
        step3 = PythonOperator(
            task_id="step3",
            python_callable=chain_step,
            op_kwargs={"label": "step3"},
        )
        # (b) 비교군: trigger_rule='all_done'은 check 실패와 무관하게 실행되어 이미 success로 끝난다.
        # Mark Success해도 재실행되지 않는 게 정상 — 자동 재평가는 upstream_failed 상태에만 작동함을 검증.
        all_done_observer = PythonOperator(
            task_id="all_done_observer",
            python_callable=chain_step,
            op_kwargs={"label": "all_done_observer"},
            trigger_rule="all_done",
        )
        seed_c >> check_c >> step1 >> step2 >> step3
        check_c >> all_done_observer

    setup >> [normal_group, anomaly_group, chain_group]