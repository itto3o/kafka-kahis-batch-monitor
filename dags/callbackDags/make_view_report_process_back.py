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
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "make_view_report_process_back"
oracle_conn_id = "M2MSYS"
postgresql_conn_id = "bv_postgresql"

local_tz = pendulum.timezone("Asia/Seoul")
with DAG(
    dag_id=DAG_ID,
    description='화면, 보고서 데이터 프로세스',
    default_args={"on_failure_callback": on_task_failure},
    start_date=datetime.datetime(2023, 7, 24, tzinfo=local_tz),
    #schedule='30 7 * * *',
    schedule=None,
    catchup=False,
) as dag:
    CONF_STD = "{{ dag_run.conf.get('standard_date', (data_interval_end - macros.timedelta(days=1)).in_timezone('Asia/Seoul').format('YYYY-MM-DD')) }}"

    call_geoai_view_sp_insert_tb_statutorydong_code = PostgresOperator(task_id='call_geoai_view_sp_insert_tb_statutorydong_code',
                                                                              postgres_conn_id=postgresql_conn_id,
                                                                              sql="call geoai_view.sp_insert_tb_statutorydong_code" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")

    call_geoai_view_sp_insert_tb_update_standard_date = PostgresOperator(task_id='call_geoai_view_sp_insert_tb_update_standard_date',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_view.sp_insert_tb_update_standard_date" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")

    with TaskGroup(group_id='call_geoai_view_sp1') as call_geoai_view_sp1:
        sp_list = [
            'sp_insert_tb_bird_information',
            'sp_insert_tb_car_information',
            'sp_insert_tb_quarantine_card_information',
            'sp_insert_tb_statutorydong_coordinate_information',
            'sp_insert_tb_farm_detail_risk_information',
            'sp_insert_tb_farm_ai_information',
            'sp_insert_tb_farm_livestock_species_sum'
        ]
        for sp in sp_list:
            PostgresOperator(
                    task_id='call_geoai_view_' + sp,
                    postgres_conn_id=postgresql_conn_id,
                    sql="call geoai_view." + sp + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');"""
                )
    
    with TaskGroup(group_id='call_geoai_view_sp2') as call_geoai_view_sp2:
        sp_list = [
            'sp_insert_tb_risk_train_result_geom',
            'sp_insert_tb_category_xai_result',
            'sp_insert_tb_farm_risk_train_result',
            'sp_insert_tb_farm_monthly_risk_change',
            'sp_insert_tb_species_information',
            'sp_insert_tb_variable_xai_result',
            'sp_insert_tb_district_risk_information'
        ]
        for sp in sp_list:
            PostgresOperator(
                    task_id='call_geoai_view_' + sp,
                    postgres_conn_id=postgresql_conn_id,
                    sql="call geoai_view." + sp + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');"""
                )
            
    with TaskGroup(group_id='call_geoai_view_sp3') as call_geoai_view_sp3:
        sp_list = [
            'sp_insert_tb_migratorybird_census',
            'sp_insert_tb_farm_car_risk_information'
        ]
        for sp in sp_list:
            PostgresOperator(
                    task_id='call_geoai_view_' + sp,
                    postgres_conn_id=postgresql_conn_id,
                    sql="call geoai_view." + sp + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');"""
                )

    sp_insert_tb_farm_information = PostgresOperator(
                                        task_id='call_geoai_view' + '_sp_insert_tb_farm_information',
                                        postgres_conn_id=postgresql_conn_id,
                                        sql="""call geoai_view.sp_insert_tb_farm_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');"""
                                    )

    with TaskGroup(group_id='call_geoai_report_sp') as call_geoai_report_sp:
        sp_list = [
            'sp_insert_tb_confirmed_farm',
            'sp_insert_tb_wild_confirmed'
        ]
        for sp in sp_list:
            PostgresOperator(
                    task_id='call_geoai_report_' + sp,
                    postgres_conn_id=postgresql_conn_id,
                    sql="call geoai_report." + sp + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');"""
                )
        
        
    call_sp_insert_tb_farm_species_scope_count = PostgresOperator(
                    task_id='call_geoai_view_' + 'sp_insert_tb_farm_species_scope_count',
                    postgres_conn_id=postgresql_conn_id,
                    sql="call geoai_view." + 'sp_insert_tb_farm_species_scope_count' + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');"""
                )
    with TaskGroup(group_id='make_risk_rank') as make_risk_rank:
        call_geoai_view_sp_insert_tb_fulltime_farm_risk_rank = PostgresOperator(
            task_id='call_geoai_view_sp_insert_tb_fulltime_farm_risk_rank',
            postgres_conn_id=postgresql_conn_id,
            sql="call geoai_view.sp_insert_tb_fulltime_farm_risk_rank" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_view_sp_insert_tb_region_risk_rank = PostgresOperator(
            task_id='call_geoai_view_sp_insert_tb_region_risk_rank',
            postgres_conn_id=postgresql_conn_id,
            sql="call geoai_view.sp_insert_tb_region_risk_rank" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_view_sp_insert_tb_farm_risk_rank = PostgresOperator(
            task_id='call_geoai_view_sp_insert_tb_farm_risk_rank',
            postgres_conn_id=postgresql_conn_id,
            sql="call geoai_view.sp_insert_tb_farm_risk_rank" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")

    with TaskGroup(group_id='make_fulltime_farm_view') as make_fulltime_farm_view:
        call_geoai_view_sp_insert_tb_farm_cluster = PostgresOperator(task_id='call_geoai_view_sp_insert_tb_farm_cluster',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_view.sp_insert_tb_farm_cluster" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_view_sp_insert_tb_farm_cluster_confirmed = PostgresOperator(task_id='call_geoai_view_sp_insert_tb_farm_cluster_confirmed',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_view.sp_insert_tb_farm_cluster_confirmed" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")

        call_geoai_view_sp_insert_tb_fulltime_category_xai_result = PostgresOperator(task_id='call_geoai_view_sp_insert_tb_fulltime_category_xai_result',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_view.sp_insert_tb_fulltime_category_xai_result" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_view_sp_insert_tb_fulltime_farm_risk_train_result = PostgresOperator(task_id='call_geoai_view_sp_insert_tb_fulltime_farm_risk_train_result',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_view.sp_insert_tb_fulltime_farm_risk_train_result" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_view_sp_insert_tb_fulltime_quarantine_card_information = PostgresOperator(task_id='call_geoai_view_sp_insert_tb_fulltime_quarantine_card_information',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_view.sp_insert_tb_fulltime_quarantine_card_information" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_view_sp_insert_tb_fulltime_variable_xai_result = PostgresOperator(task_id='call_geoai_view_sp_insert_tb_fulltime_variable_xai_result',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_view.sp_insert_tb_fulltime_variable_xai_result" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_view_sp_insert_tb_farm_cluster >> call_geoai_view_sp_insert_tb_farm_cluster_confirmed >> call_geoai_view_sp_insert_tb_fulltime_category_xai_result >> call_geoai_view_sp_insert_tb_fulltime_farm_risk_train_result >> call_geoai_view_sp_insert_tb_fulltime_quarantine_card_information >> call_geoai_view_sp_insert_tb_fulltime_variable_xai_result
        
    with TaskGroup(group_id='make_fulltime_risk_report') as make_fulltime_risk_report:
        call_geoai_report_sp_insert_tb_oz_fulltime_farm_risk_level_status = PostgresOperator(
            task_id='call_geoai_report_sp_insert_tb_oz_fulltime_farm_risk_level_status',
            postgres_conn_id=postgresql_conn_id,
            sql="call geoai_report.sp_insert_tb_oz_fulltime_farm_risk_level_status" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_report_sp_insert_tb_oz_fulltime_farm_risk_level_detail = PostgresOperator(task_id='call_geoai_report_sp_insert_tb_oz_fulltime_farm_risk_level_detail',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_report.sp_insert_tb_oz_fulltime_farm_risk_level_detail" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_report_sp_insert_tb_oz_fulltime_farm_risk_level_prediction_result = PostgresOperator(task_id='call_geoai_report_sp_insert_tb_oz_fulltime_farm_risk_level_prediction_result',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_report.sp_insert_tb_oz_fulltime_farm_risk_level_prediction_result" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_report_sp_insert_tb_oz_fulltime_farm_risk_level_present_brding_lvstck = PostgresOperator(task_id='call_geoai_report_sp_insert_tb_oz_fulltime_farm_risk_level_present_brding_lvstck',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_report.sp_insert_tb_oz_fulltime_farm_risk_level_present_brding_lvstck" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_report_sp_insert_tb_oz_fulltime_farm_risk_level_status >> call_geoai_report_sp_insert_tb_oz_fulltime_farm_risk_level_detail >> call_geoai_report_sp_insert_tb_oz_fulltime_farm_risk_level_prediction_result >> call_geoai_report_sp_insert_tb_oz_fulltime_farm_risk_level_present_brding_lvstck

    with TaskGroup(group_id='make_region_risk_report') as make_region_risk_report:
        call_geoai_report_sp_insert_tb_oz_region_risk_level_status = PostgresOperator(
            task_id='call_geoai_report_sp_insert_tb_oz_region_risk_level_status',
            postgres_conn_id=postgresql_conn_id,
            sql="call geoai_report.sp_insert_tb_oz_region_risk_level_status" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_report_sp_insert_tb_oz_region_risk_level_detail = PostgresOperator(
            task_id='call_geoai_report_sp_insert_tb_oz_region_risk_level_detail',
            postgres_conn_id=postgresql_conn_id,
            sql="call geoai_report.sp_insert_tb_oz_region_risk_level_detail" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_report_sp_insert_tb_oz_region_risk_level_prediction_result = PostgresOperator(
            task_id='call_geoai_report_sp_insert_tb_oz_region_risk_level_prediction_result',
            postgres_conn_id=postgresql_conn_id,
            sql="call geoai_report.sp_insert_tb_oz_region_risk_level_prediction_result" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_report_sp_insert_tb_oz_region_risk_level_status >> call_geoai_report_sp_insert_tb_oz_region_risk_level_detail >> call_geoai_report_sp_insert_tb_oz_region_risk_level_prediction_result

    call_geoai_report_sp_insert_tb_oz_risk_level_prediction_result_title = PostgresOperator(
        task_id='call_geoai_report_sp_insert_tb_oz_risk_level_prediction_result_title',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_report.sp_insert_tb_oz_risk_level_prediction_result_title" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")

    with TaskGroup(group_id='make_region_risk_view') as make_region_risk_view:
        call_geoai_view_sp_insert_tb_region_category_xai_result = PostgresOperator(task_id='call_geoai_view_sp_insert_tb_region_category_xai_result',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_view.sp_insert_tb_region_category_xai_result" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        call_geoai_view_sp_insert_tb_region_variable_xai_result = PostgresOperator(task_id='call_geoai_view_sp_insert_tb_region_variable_xai_result',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_view.sp_insert_tb_region_variable_xai_result" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        sp_insert_tb_statutorydong_coordinate_information_dl = PostgresOperator(task_id='call_geoai_view_sp_insert_tb_statutorydong_coordinate_information_dl',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_view.sp_insert_tb_statutorydong_coordinate_information_dl" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
       
    
    call_sp_insert_tb_section_farm_risk_top_rank = PostgresOperator(
                    task_id='call_geoai_view_' + 'sp_insert_tb_section_farm_risk_top_rank',
                    postgres_conn_id=postgresql_conn_id,
                    sql="call geoai_view." + 'sp_insert_tb_section_farm_risk_top_rank' + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');"""
                )


    call_sp_insert_tb_section_farm_risk_top_rank_information = PostgresOperator(
                    task_id='call_geoai_view_' + 'sp_insert_tb_section_farm_risk_top_rank_information',
                    postgres_conn_id=postgresql_conn_id,
                    sql="call geoai_view." + 'sp_insert_tb_section_farm_risk_top_rank_information' + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');"""
                )
    
    execute_qa_dag = TriggerDagRunOperator(
        task_id='execute_qa_dag',
        trigger_dag_id='report_data_qa_test',
        execution_date="{{ data_interval_end }}",
        conf={"standard_date": CONF_STD},
        reset_dag_run=True,
        wait_for_completion=False
    )

    trigger_bangyeok_process = TriggerDagRunOperator(
        task_id='trigger_bangyeok_process',
        trigger_dag_id='방역권역_프로세스',
        execution_date="{{ data_interval_end }}",
        conf={"standard_date": CONF_STD},
        reset_dag_run=True,
        wait_for_completion=False
    )
    
    # 두 DAG 모두 끝나야 실행됨 (AND 조건)
    call_geoai_view_sp_insert_tb_statutorydong_code >> make_risk_rank
    make_risk_rank >> call_geoai_view_sp1 >> call_geoai_view_sp2 >> call_geoai_view_sp3
    make_risk_rank >> make_region_risk_view >> make_fulltime_farm_view >> make_fulltime_risk_report >> call_geoai_report_sp_insert_tb_oz_risk_level_prediction_result_title
    make_region_risk_view >> make_region_risk_report >> call_geoai_report_sp_insert_tb_oz_risk_level_prediction_result_title >> call_geoai_view_sp_insert_tb_update_standard_date
    call_geoai_view_sp3 >> sp_insert_tb_farm_information >> call_geoai_report_sp >> call_sp_insert_tb_farm_species_scope_count
    call_sp_insert_tb_farm_species_scope_count >> call_sp_insert_tb_section_farm_risk_top_rank >> call_sp_insert_tb_section_farm_risk_top_rank_information >> make_fulltime_risk_report >> call_geoai_view_sp_insert_tb_update_standard_date
    call_geoai_view_sp_insert_tb_update_standard_date >> [execute_qa_dag, trigger_bangyeok_process]