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
from airflow.utils.trigger_rule import TriggerRule


@task()
def something(data_interval_start=None, data_interval_end=None):
    print(f'테스트 data_interval_start:{data_interval_start}, data_interval_end:{data_interval_end}')


postgresql_conn_id = "bv_postgresql"
local_tz = pendulum.timezone("Asia/Seoul")
@dag(
    dag_id='차량_GPS_위험도',
    description='차량 GPS 위험도 프로세스',
    default_args={"on_failure_callback": on_task_failure},
    #timezone=local_tz,
    #schedule='0 7 * * *',
    schedule=None,
    tags = ['daily', 'batch'],
    start_date=datetime.datetime(2025, 9, 15, tzinfo=local_tz),
    catchup=False,
)
def 차량_GPS_위험도():
    CONF_STD = "{{ dag_run.conf.get('standard_date', (data_interval_end - macros.timedelta(days=1)).in_timezone('Asia/Seoul').format('YYYY-MM-DD')) }}"
    
    sp_insert_tb_gps_stop_move_information = PostgresOperator(task_id='sp_insert_tb_gps_stop_move_information',
                                                     postgres_conn_id='geoai_mt2',
                                                     sql=f"""call geoai_mt.sp_insert_tb_gps_stop_move_information('{CONF_STD}')""")
    
    sp_insert_tb_farm_region_geom = PostgresOperator(task_id='sp_insert_tb_farm_region_geom',
                                                     postgres_conn_id='geoai_mt2',
                                                     sql=f"""call geoai_polygon.sp_insert_tb_farm_region_geom('{CONF_STD}')""")
    
    sp_insert_tb_farm_range_1km_visit_information = PostgresOperator(task_id='sp_insert_tb_farm_range_1km_visit_information_enrichment',
                                                     postgres_conn_id='geoai_mt2',
                                                     trigger_rule=TriggerRule.ALL_SUCCESS,
                                                     sql=f"""call geoai_mt.sp_insert_tb_farm_range_1km_visit_information_enrichment('{CONF_STD}')""")
    
    sp_insert_tb_car_gps_registration_no_information = PostgresOperator(task_id='sp_insert_tb_car_gps_registration_no_information',
                                                     postgres_conn_id='geoai_mt2',
                                                     sql=f"""call geoai_mt.sp_insert_tb_car_gps_registration_no_information('{CONF_STD}')""")
    
    sp_insert_tb_farm_information_change_history = PostgresOperator(task_id='sp_insert_tb_farm_information_change_history',
                                                     postgres_conn_id='geoai_mt2',
                                                     sql=f"""call geoai_mt.sp_insert_tb_farm_information_change_history('{CONF_STD}')""")
    
    sp_insert_tb_farm_information = PostgresOperator(task_id='sp_insert_tb_farm_information',
                                                     postgres_conn_id='geoai_mt2',
                                                     trigger_rule=TriggerRule.ALL_SUCCESS,
                                                     sql=f"""call geoai_mt.sp_insert_tb_farm_information('{CONF_STD}')""")
    
    sp_insert_tb_operation_farm = PostgresOperator(task_id='sp_insert_tb_operation_farm',
                                                     postgres_conn_id='geoai_mt2',
                                                     sql=f"""call geoai_mt.sp_insert_tb_operation_farm('{CONF_STD}')""")
    
    sp_insert_tb_farm_visit_information = PostgresOperator(task_id='sp_insert_tb_farm_visit_information',
                                                     postgres_conn_id='geoai_mt2',
                                                     sql=f"""call geoai_mt.sp_insert_tb_farm_visit_information('{CONF_STD}')""")
    
    sp_insert_tb_farm_visit_information_kahis = PostgresOperator(task_id='sp_insert_tb_farm_visit_information_kahis',
                                                     postgres_conn_id='geoai_mt2',
                                                     trigger_rule=TriggerRule.ALL_SUCCESS,
                                                     sql=f"""call geoai_mt.sp_insert_tb_farm_visit_information_kahis('{CONF_STD}')""")
    
    sp_insert_tb_habitat_visit_information = PostgresOperator(task_id='sp_insert_tb_habitat_visit_information',
                                                     postgres_conn_id='geoai_mt2',
                                                     sql=f"""call geoai_mt.sp_insert_tb_habitat_visit_information('{CONF_STD}')""")
    
    sp_insert_tb_car_risk_information = PostgresOperator(task_id='sp_insert_tb_car_risk_information',
                                                     postgres_conn_id='geoai_mt2',
                                                     trigger_rule=TriggerRule.ALL_SUCCESS,
                                                     sql=f"""call geoai_mt.sp_insert_tb_car_risk_information('{CONF_STD}')""")
    
    sp_insert_tb_fulltime_farm_decay_retention_risk_statistics = PostgresOperator(task_id='sp_insert_tb_fulltime_farm_decay_retention_risk_statistics',
                                                     postgres_conn_id='geoai_mt2',
                                                     sql=f"""call geoai_mt.sp_insert_tb_fulltime_farm_decay_retention_risk_statistics('{CONF_STD}')""")
    
    sp_insert_tb_fulltime_farm_retention_risk_statistics = PostgresOperator(task_id='sp_insert_tb_fulltime_farm_retention_risk_statistics',
                                                     postgres_conn_id='geoai_mt2',
                                                     sql=f"""call geoai_mt.sp_insert_tb_fulltime_farm_retention_risk_statistics('{CONF_STD}')""")
    
    
    execute_fulltime_risk_dag = TriggerDagRunOperator(
        task_id='execute_fulltime_risk_dag',
        trigger_dag_id='fulltime_risk_process',
        execution_date="{{ data_interval_end }}",
        conf={"standard_date": CONF_STD},
        reset_dag_run=True,
        wait_for_completion=False
    )

    execute_trigger_view_process = TriggerDagRunOperator(
        task_id='execute_trigger_view_process',
        trigger_dag_id='geoai_vehicle_risk_view_process',
        execution_date="{{ data_interval_end }}",
        conf={"standard_date": CONF_STD},
        reset_dag_run=True,
        wait_for_completion=False
    )
    
    #execute_fulltime_risk_dag = TriggerDagRunOperator(
    #    task_id="execute_fulltime_risk_dag",
    #    trigger_dag_id="fulltime_risk_process",
    #    execution_date="{{ ds }}",
    #    conf={"standard_date": CONF_STD},   
    #    trigger_run_id="fulltime_risk__{{ ds_nodash }}",
    #    reset_dag_run=True,
    #    wait_for_completion=False,
    #)
    
    sp_insert_tb_gps_stop_move_information >> sp_insert_tb_habitat_visit_information >> sp_insert_tb_farm_range_1km_visit_information
    
    sp_insert_tb_gps_stop_move_information >> sp_insert_tb_farm_visit_information
    
    sp_insert_tb_farm_information_change_history >> sp_insert_tb_farm_information >> sp_insert_tb_operation_farm >> sp_insert_tb_farm_visit_information_kahis >> sp_insert_tb_farm_range_1km_visit_information >> sp_insert_tb_car_risk_information >> sp_insert_tb_fulltime_farm_decay_retention_risk_statistics >> sp_insert_tb_fulltime_farm_retention_risk_statistics
    
    sp_insert_tb_car_gps_registration_no_information >> sp_insert_tb_farm_information >> sp_insert_tb_farm_visit_information >> sp_insert_tb_farm_visit_information_kahis
    
    sp_insert_tb_farm_region_geom >> [sp_insert_tb_farm_range_1km_visit_information, sp_insert_tb_farm_visit_information_kahis] >> sp_insert_tb_car_risk_information
    
    sp_insert_tb_farm_information_change_history >> sp_insert_tb_farm_information >> sp_insert_tb_operation_farm 
    
    sp_insert_tb_fulltime_farm_retention_risk_statistics >> [execute_fulltime_risk_dag, execute_trigger_view_process]
    
차량_GPS_위험도()