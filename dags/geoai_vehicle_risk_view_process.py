from __future__ import annotations

import datetime
import pendulum

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag

local_tz = pendulum.timezone("Asia/Seoul")


@dag(
    dag_id='geoai_vehicle_risk_view_process',
    description='축산차량_위험도_View_적재',
    schedule=None,  # Trigger 방식으로 실행 (앞단 DAG 완료 후 실행됨)
    tags=['geoai', 'view', 'vehicle_risk'],
    start_date=datetime.datetime(2025, 11, 1, tzinfo=local_tz),
    catchup=False,
)
def geoai_vehicle_risk_view_process():
    CONF_STD = "{{ dag_run.conf.get('standard_date', (data_interval_end - macros.timedelta(days=1)).in_timezone('Asia/Seoul').format('YYYY-MM-DD')) }}"

    sp_insert_tb_car_gps_registration_no_information_test = PostgresOperator(
        task_id='sp_insert_tb_car_gps_registration_no_information_test',
        postgres_conn_id='geoai_mt2',
        sql=f"""call geoai_mt.sp_insert_tb_car_gps_registration_no_information_test('{CONF_STD}')"""
    )

    sp_insert_tb_farm_information_test = PostgresOperator(
        task_id='sp_insert_tb_farm_information_test',
        postgres_conn_id='geoai_mt2',
        sql=f"""call geoai_mt.sp_insert_tb_farm_information_test('{CONF_STD}')"""
    )

    sp_insert_livestock_vehicle_risk_vehicle = PostgresOperator(
        task_id='sp_insert_livestock_vehicle_risk_vehicle',
        postgres_conn_id='geoai_mt2',
        sql=f"""call geoai_view.sp_insert_livestock_vehicle_risk_vehicle('{CONF_STD}')"""
    )

    analyze_source_tables = PostgresOperator(
        task_id='analyze_source_tables',
        postgres_conn_id='geoai_mt2',
        sql="""
            ANALYZE geoai_mt.tb_car_risk_information;
            ANALYZE geoai_mt.tb_car_gps_registration_no_information_test;
            ANALYZE geoai_mt.tb_farm_information_test;
            ANALYZE geoai_view.livestock_vehicle_risk_vehicle;
        """
    )

    sp_insert_livestock_vehicle_visit = PostgresOperator(
        task_id='sp_insert_livestock_vehicle_visit',
        postgres_conn_id='geoai_mt2',
        sql=f"""call geoai_view.sp_insert_livestock_vehicle_visit('{CONF_STD}')"""
    )

    # sp_insert_livestock_vehicle_risk_vehicle_test = PostgresOperator(
    #     task_id='sp_insert_livestock_vehicle_risk_vehicle_test',
    #     postgres_conn_id='geoai_mt2',
    #     sql=f"""call geoai_view.sp_insert_livestock_vehicle_risk_vehicle_test('{CONF_STD}')"""
    # )
    #
    # sp_insert_livestock_vehicle_visit_test = PostgresOperator(
    #     task_id='sp_insert_livestock_vehicle_visit_test',
    #     postgres_conn_id='geoai_mt2',
    #     sql=f"""call geoai_view.sp_insert_livestock_vehicle_visit_test('{CONF_STD}')"""
    # )

    [sp_insert_tb_car_gps_registration_no_information_test,
     sp_insert_tb_farm_information_test] >> sp_insert_livestock_vehicle_risk_vehicle

    sp_insert_livestock_vehicle_risk_vehicle >> analyze_source_tables >> sp_insert_livestock_vehicle_visit

geoai_vehicle_risk_view_process_dag = geoai_vehicle_risk_view_process()