from __future__ import annotations

import datetime
import os
import pendulum

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

# 검역본부 ASF DB CONNECT_ID 값 설정
postgresql_conn_id = "geoai_mt3"

local_tz = pendulum.timezone("Asia/Seoul")
with DAG(
    dag_id='make_asf_view_process',
    description='화면, 보고서 데이터 프로세스',
    start_date=datetime.datetime(2023, 7, 24, tzinfo=local_tz),
    schedule=None,
    catchup=False
) as dag:

    # 1. 법정동 좌표 정보
    call_sp_insert_statutorydong_coordinate = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_statutorydong_coordinate',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_statutorydong_coordinate();"
    )

    # 2. 법정동 코드
    call_sp_insert_tb_statutorydong_code = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_statutorydong_code',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_statutorydong_code" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 3. 차량 정보
    call_sp_insert_tb_car_information = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_car_information',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_car_information" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 4. 농가 상세 위험 정보
    call_sp_insert_tb_farm_detail_risk_information = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_farm_detail_risk_information',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_farm_detail_risk_information" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 5. 법정동 정보
    call_sp_insert_tb_statutorydong_information = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_statutorydong_information',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_statutorydong_information();"
    )

    # 6. 농가 위험 등급
    call_sp_insert_tb_farm_risk_rank = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_farm_risk_rank',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_farm_risk_rank" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 7. 농가 위험 학습 결과
    call_sp_insert_tb_farm_risk_train_result = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_farm_risk_train_result',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_farm_risk_train_result" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 8. 축종 정보
    call_sp_insert_tb_species_information = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_species_information',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_species_information" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 9. 카테고리 XAI 결과
    call_sp_insert_tb_category_xai_result_asf = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_category_xai_result_asf',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_category_xai_result_asf" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 10. 변수 XAI 결과
    call_sp_insert_tb_variable_xai_result_asf = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_variable_xai_result_asf',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_variable_xai_result_asf" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 11. 농가 정보 ASF
    call_sp_insert_tb_farm_information = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_farm_information',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_farm_information" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 12. 농가 가축 축종 합계
    call_sp_insert_tb_farm_livestock_species_sum = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_farm_livestock_species_sum',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_farm_livestock_species_sum" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 13. 농가 축종 범위 카운트
    call_sp_insert_tb_farm_species_scope_count = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_farm_species_scope_count',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_farm_species_scope_count" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 15. 법정동 좌표 정보 DL
    call_sp_insert_tb_statutorydong_coordinate_information_dl = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_statutorydong_coordinate_information_dl',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_statutorydong_coordinate_information_dl" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 16. 확진 농가
    call_sp_insert_tb_confirmed_farm = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_confirmed_farm',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_confirmed_farm" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 17. 야생 확진
    call_sp_insert_tb_wild_confirmed = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_wild_confirmed',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_wild_confirmed" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 18. 주변 환경 지역
    call_sp_insert_tb_around_environment_area = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_around_environment_area',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_around_environment_area();"
    )

    # 19. 방역 카드 정보
    call_sp_insert_tb_quarantine_card_information = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_quarantine_card_information',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_quarantine_card_information" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 20. 업데이트 기준일
    call_sp_insert_tb_update_standard_date_asf = PostgresOperator(
        task_id='call_geoai_view_asf_sp_insert_tb_update_standard_date_asf',
        postgres_conn_id=postgresql_conn_id,
        sql="call geoai_view_asf.sp_insert_tb_update_standard_date_asf" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}'::date);"""
    )

    # 실행 순서 정의 (순차 실행)
    call_sp_insert_statutorydong_coordinate >> call_sp_insert_tb_statutorydong_code >> call_sp_insert_tb_car_information >> call_sp_insert_tb_farm_detail_risk_information >> call_sp_insert_tb_statutorydong_information >> call_sp_insert_tb_farm_risk_rank >> call_sp_insert_tb_farm_risk_train_result >> call_sp_insert_tb_species_information >> call_sp_insert_tb_category_xai_result_asf >> call_sp_insert_tb_variable_xai_result_asf >> call_sp_insert_tb_farm_information >> call_sp_insert_tb_farm_livestock_species_sum >> call_sp_insert_tb_farm_species_scope_count >> call_sp_insert_tb_statutorydong_coordinate_information_dl >> call_sp_insert_tb_confirmed_farm >> call_sp_insert_tb_around_environment_area >> call_sp_insert_tb_quarantine_card_information >> call_sp_insert_tb_wild_confirmed >> call_sp_insert_tb_update_standard_date_asf
