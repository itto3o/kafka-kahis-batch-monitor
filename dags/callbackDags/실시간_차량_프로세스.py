from __future__ import annotations
from callbacks.kafka_error_callback import on_task_failure

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


@task()
def something(data_interval_start=None, data_interval_end=None):
    print(f'테스트 data_interval_start:{data_interval_start}, data_interval_end:{data_interval_end}')


postgresql_conn_id = "bv_postgresql"
local_tz = pendulum.timezone("Asia/Seoul")
with DAG(
    dag_id='실시간_차량_프로세스',
    description='실시간_차량_프로세스',
    default_args={"on_failure_callback": on_task_failure},
    start_date=pendulum.datetime(2025, 11, 4, tz="Asia/Seoul"),
    #schedule='30 7 * * *',
    schedule=None,
    catchup=False,
) as dag:
    call_realtime_car_risk = SimpleHttpOperator(
                    task_id = f"call_realtime_car_risk",
                    http_conn_id = "kahis_flask_realtime_car_risk",
                    method="POST",
                    endpoint="/api/batch/event",
                    data = json.dumps({
                        "timestamp" : "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
                        

                    }),
                    headers={"Content-Type": "application/json"},
                    dag=dag
    )
    call_realtime_car_risk