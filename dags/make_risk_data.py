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
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ai_project.check_tb_diseasecontrol_status_information import check_tb_diseasecontrol_status_information
from ai_project.check_tb_farm_information import check_tb_farm_information
from ai_project.check_tb_livestock_species_information import check_tb_livestock_species_information
from ai_project.check_tb_car_visit_information import check_tb_car_visit_information
from ai_project.check_calculation_environment_information import check_calculation_environment_information
from ai_project.check_tb_prediction_result import check_tb_prediction_result
from ai_project.calculation_diseasecontrol_status_information import make_calculation_diseasecontrol_status_information

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "make_risk_data"
oracle_conn_id = "M2MSYS"
postgresql_conn_id = "bv_postgresql"

local_tz = pendulum.timezone("Asia/Seoul")

def check_operatable_month(std_dt):
    return std_dt.split('-')[1] not in ['06', '07', '08']

with DAG(
    dag_id=DAG_ID,
    description='기본 위험도 데이터 생성',
    start_date=datetime.datetime(2025, 9, 16, tzinfo=local_tz),
    #schedule='0 7 * * *',
    schedule=None,
    catchup=False,
) as dag:
    with TaskGroup(group_id='make_data_mart') as make_data_mart:
        call_sp_insert_tb_diseasecontrol_status_information = PostgresOperator(task_id='call_sp_insert_tb_diseasecontrol_status_information',
                                                       postgres_conn_id=postgresql_conn_id,
                                                       sql="""call geoai_mt.sp_insert_tb_diseasecontrol_status_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")


        call_sp_insert_tb_farm_information = PostgresOperator(task_id='call_sp_insert_tb_farm_information',
                                                       postgres_conn_id=postgresql_conn_id,
                                                       sql="""call geoai_mt.sp_insert_tb_farm_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        # tb_fulltime_farm_list 프로시저 콜 부분 필요
        call_geoai_mt_sp_insert_tb_fulltime_farm_list = PostgresOperator(task_id='call_geoai_mt_sp_insert_tb_fulltime_farm_list',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_mt.sp_insert_tb_fulltime_farm_list" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
        
        call_sp_insert_tb_livestock_species_information = PostgresOperator(task_id='call_sp_insert_tb_livestock_species_information',
                                                       postgres_conn_id=postgresql_conn_id,
                                                       sql="""call geoai_mt.sp_insert_tb_livestock_species_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        call_sp_insert_tb_car_information = PostgresOperator(task_id='call_sp_insert_tb_car_information',
                                                        postgres_conn_id=postgresql_conn_id,
                                                        sql="""call geoai_mt.sp_insert_tb_car_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        call_sp_insert_tb_ai_information = PostgresOperator(task_id='call_sp_insert_tb_ai_information',
                                                     postgres_conn_id=postgresql_conn_id,
                                                     sql="""call geoai_mt.sp_insert_tb_ai_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
        
        call_sp_insert_tb_poultry_farm_information = PostgresOperator(task_id='call_sp_insert_tb_poultry_farm_information',
                                                     postgres_conn_id=postgresql_conn_id,
                                                     sql="""call geoai_mt.sp_insert_tb_poultry_farm_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
        
        call_sp_insert_tb_specimen_serial = PostgresOperator(task_id='call_sp_insert_tb_specimen_serial',
                                                     postgres_conn_id=postgresql_conn_id,
                                                     sql="""call geoai_mt.sp_insert_tb_specimen_serial('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
        
        call_sp_insert_tb_mobile_gps_information = PostgresOperator(task_id='call_sp_insert_tb_mobile_gps_information',
                                                     postgres_conn_id=postgresql_conn_id,
                                                     sql="""call geoai_mt.sp_insert_tb_mobile_gps_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
        
        call_sp_insert_tb_driver_information = PostgresOperator(task_id='call_sp_insert_tb_driver_information',
                                                     postgres_conn_id=postgresql_conn_id,
                                                     sql="""call geoai_mt.sp_insert_tb_driver_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        call_sp_insert_tb_farm_internal_facility_numbers = PostgresOperator(task_id='call_sp_insert_tb_farm_internal_facility_numbers',
                                                                postgres_conn_id=postgresql_conn_id,
                                                                sql="""call geoai_mt.sp_insert_tb_farm_internal_facility_numbers('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
    
        call_sp_insert_tb_diseasediagnosis_result_information = PostgresOperator(task_id='call_sp_insert_tb_diseasediagnosis_result_information',
                                                                postgres_conn_id=postgresql_conn_id,
                                                                sql="""call geoai_mt.sp_insert_tb_diseasediagnosis_result_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
        
        call_sp_insert_tb_fulltime_diseasecontrol_status_information = PostgresOperator(task_id='call_sp_insert_tb_fulltime_diseasecontrol_status_information',
                                                                postgres_conn_id=postgresql_conn_id,
                                                                sql="""call geoai_mt.sp_insert_tb_fulltime_diseasecontrol_status_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
        
        
        check_tb_diseasecontrol_status_information = PythonOperator(task_id='check_tb_diseasecontrol_status_information',
                                                provide_context=True,
                                                python_callable=check_tb_diseasecontrol_status_information,
                                                op_kwargs={'std_dt': '{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}',
                                                          'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
                                                dag=dag)

        check_tb_farm_information = PythonOperator(task_id='check_tb_farm_information',
                                                provide_context=True,
                                                python_callable=check_tb_farm_information,
                                                op_kwargs={'std_dt': '{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}',
                                                          'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
                                                dag=dag)

        check_tb_livestock_species_information = PythonOperator(task_id='check_tb_livestock_species_information',
                                                provide_context=True,
                                                python_callable=check_tb_livestock_species_information,
                                                op_kwargs={'std_dt': '{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}',
                                                          'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
                                                dag=dag)

        check_tb_car_visit_information = PythonOperator(task_id='check_tb_car_visit_information',
                                                provide_context=True,
                                                python_callable=check_tb_car_visit_information,
                                                op_kwargs={'std_dt': '{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}',
                                                          'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
                                                dag=dag)
        
        call_sp_insert_tb_diseasecontrol_status_information >> call_sp_insert_tb_farm_information
        call_sp_insert_tb_livestock_species_information >> call_sp_insert_tb_farm_information
        call_sp_insert_tb_farm_information >> call_sp_insert_tb_poultry_farm_information >> call_geoai_mt_sp_insert_tb_fulltime_farm_list
        call_sp_insert_tb_diseasecontrol_status_information >> check_tb_diseasecontrol_status_information
        call_sp_insert_tb_farm_information >> check_tb_farm_information 
        call_sp_insert_tb_livestock_species_information >> check_tb_livestock_species_information
        call_sp_insert_tb_car_information >> check_tb_car_visit_information
        
    
    call_sp_insert_pr_virus_dstc = PostgresOperator(
            task_id='call_sp_insert_tb_calculation_virus_distance',
            postgres_conn_id=postgresql_conn_id,
            sql="""call geoai_mt.sp_insert_tb_calculation_virus_distance('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}', interval '21 days')"""
        )
    
    execute_calculation_environment_information_script = SimpleHttpOperator(
        task_id="execute_calculation_environment_information_script",
        http_conn_id="pr_env_info",
        method="GET",
        endpoint="/calculation_environment_information",
        data={"std_dt": """{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format('YYYY-MM-DD') }}""",
              'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
        headers={"Content-Type": "application/json"},
        dag=dag,
    )

    execute_calculation_infection_farm_distance_script = SimpleHttpOperator(
            task_id="execute_calculation_infection_farm_distance_script",
            http_conn_id="pr_env_info",
            method="GET",
            endpoint="/calculation_infection_farm_distance",
            data={"std_dt": """{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format('YYYY-MM-DD') }}""",
              'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
            headers={"Content-Type": "application/json"},
            dag=dag,
        )
    
    execute_calculation_diseasecontrol_status_information = PythonOperator(task_id='execute_calculation_diseasecontrol_status_information',
                                                provide_context=True,
                                                python_callable=make_calculation_diseasecontrol_status_information,
                                                op_kwargs={'std_dt': '{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}',
                                                          'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
                                                dag=dag)
    check_calculation_environment_information = PythonOperator(task_id='check_calculation_environment_information',
                                                provide_context=True,
                                                python_callable=check_calculation_environment_information,
                                                op_kwargs={'std_dt': '{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}',
                                                          'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
                                                dag=dag)
    call_sp_insert_tb_farm_definitediagnosis_occurrence_information = PostgresOperator(task_id='sp_insert_tb_farm_definitediagnosis_occurrence_information',
                                                     postgres_conn_id=postgresql_conn_id,
                                                     sql="""call geoai_mt.sp_insert_tb_farm_definitediagnosis_occurrence_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
    
    call_sp_insert_tb_wild_definitediagnosis_occurrence_information = PostgresOperator(task_id='sp_insert_tb_wild_definitediagnosis_occurrence_information',
                                                     postgres_conn_id=postgresql_conn_id,
                                                     sql="""call geoai_mt.sp_insert_tb_wild_definitediagnosis_occurrence_information()""")
    
    with TaskGroup(group_id='make_polygon_data') as make_polygon_data:
        call_geoai_polygon_sp_create_tb_farm_radius_3km_geom= PostgresOperator(task_id='call_geoai_polygon_sp_create_tb_farm_radius_3km_geom',
                                                                              postgres_conn_id=postgresql_conn_id,
                                                                              sql="call geoai_polygon.sp_create_tb_farm_radius_3km_geom" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")


        call_geoai_polygon_sp_insert_farm_environment_farmland = PostgresOperator(task_id='call_geoai_polygon_sp_insert_farm_environment_farmland',
                                                                              postgres_conn_id=postgresql_conn_id,
                                                                              sql="call geoai_polygon.sp_insert_farm_environment_farmland('new')")

        call_geoai_polygon_sp_insert_farm_environment_forest= PostgresOperator(task_id='call_geoai_polygon_sp_insert_farm_environment_forest',
                                                                              postgres_conn_id=postgresql_conn_id,
                                                                              sql="call geoai_polygon.sp_insert_farm_environment_forest('new')")

        call_geoai_polygon_sp_insert_farm_environment_habitat= PostgresOperator(task_id='call_geoai_polygon_sp_insert_farm_environment_habitat',
                                                                              postgres_conn_id=postgresql_conn_id,
                                                                              sql="call geoai_polygon.sp_insert_farm_environment_habitat('new')")

        call_geoai_polygon_sp_insert_farm_environment_lake= PostgresOperator(task_id='call_geoai_polygon_sp_insert_farm_environment_lake',
                                                                              postgres_conn_id=postgresql_conn_id,
                                                                              sql="call geoai_polygon.sp_insert_farm_environment_lake('new')")

        call_geoai_polygon_sp_insert_farm_environment_tidalflat= PostgresOperator(task_id='call_geoai_polygon_sp_insert_farm_environment_tidalflat',
                                                                              postgres_conn_id=postgresql_conn_id,
                                                                              sql="call geoai_polygon.sp_insert_farm_environment_tidalflat('new')")

        call_geoai_polygon_sp_insert_farm_environment_river= PostgresOperator(task_id='call_geoai_polygon_sp_insert_farm_environment_river',
                                                                              postgres_conn_id=postgresql_conn_id,
                                                                              sql="call geoai_polygon.sp_insert_farm_environment_river('new')")

        call_geoai_polygon_sp_insert_tb_specimen_serial_geom = PostgresOperator(task_id='call_geoai_polygon_sp_insert_tb_specimen_serial_geom',
                                                                              postgres_conn_id=postgresql_conn_id,
                                                                              sql="call geoai_polygon.sp_insert_tb_specimen_serial_geom()")
               
        call_geoai_polygon_sp_insert_tb_calculation_habitat_nearest_distance = PostgresOperator(task_id='call_geoai_polygon_sp_insert_tb_calculation_habitat_nearest_distance',
                                                                              postgres_conn_id=postgresql_conn_id,
                                                                              sql="call geoai_polygon.sp_insert_tb_calculation_habitat_nearest_distance" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")

        call_geoai_polygon_sp_create_tb_farm_radius_3km_geom >> call_geoai_polygon_sp_insert_farm_environment_farmland
        call_geoai_polygon_sp_create_tb_farm_radius_3km_geom >> call_geoai_polygon_sp_insert_farm_environment_forest
        call_geoai_polygon_sp_create_tb_farm_radius_3km_geom >> call_geoai_polygon_sp_insert_farm_environment_habitat
        call_geoai_polygon_sp_create_tb_farm_radius_3km_geom >> call_geoai_polygon_sp_insert_farm_environment_lake
        call_geoai_polygon_sp_create_tb_farm_radius_3km_geom >> call_geoai_polygon_sp_insert_farm_environment_tidalflat
        call_geoai_polygon_sp_create_tb_farm_radius_3km_geom >> call_geoai_polygon_sp_insert_farm_environment_river        
    
    call_geoai_polygon_sp_insert_tb_calculation_region_risk_score = PostgresOperator(task_id='call_geoai_polygon_sp_insert_tb_calculation_region_risk_score',
                                                                              postgres_conn_id=postgresql_conn_id,
                                                                              sql="call geoai_polygon.sp_insert_tb_calculation_region_risk_score" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
    
    make_train_data = PostgresOperator(
            task_id='call_sp_insert_tb_trainingset',
            postgres_conn_id=postgresql_conn_id,
            sql="""call geoai_mt.sp_insert_tb_trainingset('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')"""
        )
    
    check_operatable_month_task = ShortCircuitOperator(task_id='check_operatable_month',
                                      python_callable=check_operatable_month,
                                      op_kwargs={
                                          'std_dt': '{{ data_interval_end.in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'})
    
    call_ml_api = SimpleHttpOperator(
        task_id="call_ml_api",
        http_conn_id="ml_server",
        method="POST",
        endpoint="/predict",
        data=json.dumps({
        "target_date": """{{ (data_interval_end - macros.timedelta(days=1)).in_timezone('Asia/Seoul').format('YYYYMMDD') }}""",
        "std_dt": """{{ (data_interval_end - macros.timedelta(days=1)).in_timezone('Asia/Seoul').format('YYYY-MM-DD') }}""",
              'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri(),
            'mlflow_uri': Variable.get("mlflow_uri")
    }),
        headers={"Content-Type": "application/json"},
        dag=dag,
    )

    with TaskGroup(group_id='make_pr_data') as make_pr_data:
       # execute_district_danger_script = SimpleHttpOperator(
       #     task_id="execute_district_danger_script",
       #     http_conn_id="pr_env_info",
       #     method="GET",
       #     endpoint="/district_danger",
       #     data={"std_dt": """{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format('YYYY-MM-DD') }}""",
       #       'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
       #     headers={"Content-Type": "application/json"},
       #     dag=dag,
       # )

        execute_region_danger_script = SimpleHttpOperator(
            task_id="execute_region_danger_script",
            http_conn_id="pr_env_info",
            method="GET",
            endpoint="/region_danger",
            data={"std_dt": """{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format('YYYY-MM-DD') }}""",
              'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
            headers={"Content-Type": "application/json"},
            dag=dag,
        )

        execute_make_risk_region_risk_score = SimpleHttpOperator(
            task_id="execute_make_risk_region_risk_score",
            http_conn_id="pr_env_info",
            method="GET",
            endpoint="/make_risk_region_risk_score",
            data={"std_dt": """{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format('YYYY-MM-DD') }}""",
              'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
            headers={"Content-Type": "application/json"},
            dag=dag,
        )

        execute_make_calculation_region_risk_score = SimpleHttpOperator(
            task_id="execute_make_calculation_region_risk_score",
            http_conn_id="pr_env_info",
            method="GET",
            endpoint="/make_calculation_region_risk_score",
            data={"std_dt": """{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format('YYYY-MM-DD') }}""",
              'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
            headers={"Content-Type": "application/json"},
            dag=dag,
        )

    check_tb_prediction_result = PythonOperator(task_id='check_tb_prediction_result',
                                                provide_context=True,
                                                python_callable=check_tb_prediction_result,
                                                op_kwargs={'standard_date': '{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}',
                                                          'db_conn_str': PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()},
                                                dag=dag)
    
    STD = "{{ (data_interval_end - macros.timedelta(days=1)).in_timezone('Asia/Seoul').format('YYYY-MM-DD') }}"

    #trigger_region_risk = TriggerDagRunOperator(
    #    task_id="trigger_region_risk",
    #    trigger_dag_id="region_risk_process",
    #    execution_date="{{ ds }}",          # 논리 날짜는 그대로 전달
    #    conf={"standard_date": STD},        # ★ 기준일을 conf로 전달
    #    trigger_run_id="region_risk__{{ ds_nodash }}",
    #    reset_dag_run=True,
    #    wait_for_completion=False,
    #)
    
    trigger_region_risk = TriggerDagRunOperator(
        task_id='execute_car_risk_dag',
        trigger_dag_id='차량_GPS_위험도',
        execution_date="{{ data_interval_end }}",
        conf={"standard_date": STD},
        reset_dag_run=True,
        wait_for_completion=False
    )

    
    make_data_mart >> make_polygon_data 
    make_data_mart >> [call_sp_insert_pr_virus_dstc, execute_calculation_environment_information_script, execute_calculation_infection_farm_distance_script, execute_calculation_diseasecontrol_status_information, call_sp_insert_tb_farm_definitediagnosis_occurrence_information, call_sp_insert_tb_wild_definitediagnosis_occurrence_information] >> check_calculation_environment_information
    check_calculation_environment_information >> make_train_data >> check_operatable_month_task >> call_ml_api >> make_pr_data
    call_ml_api >> check_tb_prediction_result
    execute_make_risk_region_risk_score >> call_geoai_polygon_sp_insert_tb_calculation_region_risk_score 
    make_pr_data >> trigger_region_risk