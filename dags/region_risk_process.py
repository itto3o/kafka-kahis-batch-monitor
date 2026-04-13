from __future__ import annotations

import datetime
import os
import psycopg2
from psycopg2.extras import execute_values
from time import time
import json
import pendulum

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from airflow.decorators import dag, task
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from region_risk_project.region_risk_nonscaled_train_airflow import insert_region_risk_nonscaled_train

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "region_risk_process"
oracle_conn_id = "M2MSYS"
postgresql_conn_id = "bv_postgresql"

local_tz = pendulum.timezone("Asia/Seoul")
with DAG(
    dag_id=DAG_ID,
    description='지역 위험도 생성 플로우',
    start_date=datetime.datetime(2023, 7, 24, tzinfo=local_tz),
    #schedule='0 7 * * *',
    schedule=None,
    catchup=False,
) as dag:
    CONF_STD = "{{ dag_run.conf.get('standard_date', (data_interval_end - macros.timedelta(days=1)).in_timezone('Asia/Seoul').format('YYYY-MM-DD')) }}"
    @task
    def region_risk_nonscaled_train(standard_date):
        hook = PostgresHook(postgres_conn_id=postgresql_conn_id)
        with hook.get_conn() as conn:
            with conn.cursor() as pg_cursor:
                partiton_date = standard_date.replace('-','')
                pg_cursor.execute(f"call geoai_mt.generate_std_dt_partition_table('geoai_monthly_report', 'tb_region_risk_nonscaled_train', 'geoai_monthly_report_partition', '{partiton_date}', '{partiton_date}');")
        insert_region_risk_nonscaled_train(std_dt=standard_date, db_conn_str=hook.get_uri())

    # 지역 위험도 기본 필요 데이터
    with TaskGroup(group_id='make_region_risk_data') as make_region_risk_data:
        # A
        call_geoai_monthly_report_sp_insert_tb_train_farm_information = PostgresOperator(task_id='call_geoai_monthly_report_sp_insert_tb_train_farm_information',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_monthly_report.sp_insert_tb_train_farm_information" + f"""('{CONF_STD}');""")

        # D
        call_geoai_monthly_report_sp_insert_tb_calculation_region_migratorybird_count = PostgresOperator(task_id='call_geoai_monthly_report_sp_insert_tb_calculation_region_migratorybird_count',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_monthly_report.sp_insert_tb_calculation_region_migratorybird_count" + f"""('{CONF_STD}');""")
        # 변경사항 X
        call_geoai_monthly_report_sp_insert_tb_calculation_farm_around_virus_count = PostgresOperator(task_id='call_geoai_monthly_report_sp_insert_tb_calculation_farm_around_virus_count',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_monthly_report.sp_insert_tb_calculation_farm_around_virus_count" + f"""('{CONF_STD}');""")
        # 변경사항 X
        call_geoai_monthly_report_sp_insert_tb_train_visit_information = PostgresOperator(task_id='call_geoai_monthly_report_sp_insert_tb_train_visit_information',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_monthly_report.sp_insert_tb_train_visit_information" + f"""('{CONF_STD}');""")
        # E 프로시저 콜 변경 필요
        call_geoai_monthly_report_sp_insert_tb_eupmyundong_nearby_infection_region = PostgresOperator(task_id='call_geoai_monthly_report_sp_insert_tb_eupmyundong_nearby_infection_region',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_monthly_report.sp_insert_tb_eupmyundong_nearby_infection_region" + f"""('{CONF_STD}');""")
        # F 프로시저 콜 변경 필요 (수정: _region_buffer -> _farm)
        call_geoai_monthly_report_sp_insert_tb_eupmyundong_nearby_infection_farm = PostgresOperator(task_id='call_geoai_monthly_report_sp_insert_tb_eupmyundong_nearby_infection_farm',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_monthly_report.sp_insert_tb_eupmyundong_nearby_infection_farm" + f"""('{CONF_STD}');""")
        # 신규 변수(nearby_infection_mindist) 프로시저 콜
        call_geoai_monthly_report_sp_insert_tb_eupmyundong_nearby_infection_mindist = PostgresOperator(task_id='call_geoai_monthly_report_sp_insert_tb_eupmyundong_nearby_infection_mindist',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_monthly_report.sp_insert_tb_eupmyundong_nearby_infection_mindist" + f"""('{CONF_STD}');""")
        # (251212 수정) tb_region_risk_scaled_pred 파티션 생성 프로시저 콜
        call_geoai_monthly_report_sp_insert_tb_region_risk_scaled_pred = PostgresOperator(task_id='call_geoai_monthly_report_sp_insert_tb_region_risk_scaled_pred',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_monthly_report.sp_insert_tb_region_risk_scaled_pred" + f"""('{CONF_STD}');""")


    train_task = region_risk_nonscaled_train(CONF_STD)

    call_api = SimpleHttpOperator(
            task_id="call_region_risk_train",
            http_conn_id="kahis_flask_region_risk",
            method="POST",
            endpoint="/predict",
            data=json.dumps({
                "std_dt" : CONF_STD,
                'is_test': True,
                'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri(),
                'mlflow_uri': Variable.get("mlflow_uri")
            }),
                headers={"Content-Type": "application/json"},
                dag=dag,
            )


    execute_car_risk_dag = TriggerDagRunOperator(
        task_id='execute_view_report_dag',
        trigger_dag_id='make_view_report_process_back',
        execution_date="{{ data_interval_end }}",
        conf={"standard_date": CONF_STD},
        reset_dag_run=True,
        wait_for_completion=False
    )
    execute_asf_risk_dag = TriggerDagRunOperator(
        task_id='execute_asf_risk_dag',
        trigger_dag_id='ASF_일일프로세스',
        execution_date="{{ data_interval_end }}",
        conf={"standard_date": CONF_STD},
        reset_dag_run=True,
        wait_for_completion=False
    )
    #execute_car_risk_dag = TriggerDagRunOperator(
    #    task_id="execute_car_risk_dag",
    #    trigger_dag_id="차량_GPS_위험도",
    #    execution_date="{{ ds }}",
    #    conf={"standard_date": CONF_STD},
    #    trigger_run_id="car_risk__{{ ds_nodash }}",
    #    reset_dag_run=True,
    #    wait_for_completion=False,
    #)

    make_region_risk_data >> train_task >> call_api >> execute_car_risk_dag >> execute_asf_risk_dag