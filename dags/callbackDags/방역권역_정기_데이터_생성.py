from __future__ import annotations
from callbacks.kafka_error_callback import on_task_failure

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.baseoperator import cross_downstream
   
#  a. 기간 : 2026-03-02 (새벽 수행) -> 올해는 4월 2일로 임시 변경
#    -> 산출 데이터(2025-10 ~ 2026-03)
#  b. 평시기간 : 2026-10-02 (새벽 수행)
#    -> 산출 데이터(2025-03 ~ 2025-09)
#  c. 1년기간 : 2026-01-02 (새벽 수행)
#    -> 산출 데이터(2025-01 ~ 2025-12)

# 한국 시간대 설정
local_tz = pendulum.timezone("Asia/Seoul")

# DAG 정의
with DAG(
    dag_id='방역권역_정기_데이터_생성',
    description='매년 1, 3, 10월 2일에 실행되어 기간별 데이터를 생성하는 배치 (2026년 4월 임시 추가)',
    default_args={"on_failure_callback": on_task_failure},
    schedule='0 4 2 1,3,4,10 *',  # 💡 원복 포인트 1: 나중에 '4,' 제거
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    catchup=False,
    tags=['seasonal', 'batch', 'geoai'],
) as dag:

    # 1. 실행일(Logical Date)을 기준으로 데이터 대상 기간 계산
    @task(multiple_outputs=True)
    def calculate_target_period(logical_date=None):
        exec_date = logical_date.in_timezone("Asia/Seoul")
        year = exec_date.year
        month = exec_date.month
        
        # 💡 원복 포인트 2: 이 블록을 나중에 전체 삭제
        # [임시 하드코딩] 2026년 4월 2일 실행 시 (2025-10 ~ 2026-03)
        if month == 4:
            start_dt = pendulum.datetime(2025, 10, 1, tz=local_tz)
            end_dt = pendulum.datetime(2026, 3, 1, tz=local_tz).end_of('month')

        # a. 특방기간 : 기존 로직 (내년 3월을 위해 그대로 둠)
        elif month == 3:
            start_dt = pendulum.datetime(year - 1, 10, 1, tz=local_tz)
            end_dt = pendulum.datetime(year, 2, 1, tz=local_tz).end_of('month')
            
        # b. 평시기간 : 10월 2일 수행 -> (올해 3월 ~ 올해 9월)
        elif month == 10:
            start_dt = pendulum.datetime(year, 3, 1, tz=local_tz)
            end_dt = pendulum.datetime(year, 9, 1, tz=local_tz).end_of('month')
            
        # c. 1년기간 : 1월 2일 수행 -> (전년 1월 ~ 전년 12월)
        elif month == 1:
            start_dt = pendulum.datetime(year - 1, 1, 1, tz=local_tz)
            end_dt = pendulum.datetime(year - 1, 12, 1, tz=local_tz).end_of('month')
            
        else:
            # (테스트용 예외처리) 정의되지 않은 월에 수동 실행 시 전월로 설정
            start_dt = exec_date.subtract(months=1).start_of('month')
            end_dt = exec_date.subtract(months=1).end_of('month')

        return {
            'start_date_full': start_dt.format('YYYY-MM-DD'),
            'end_date_full': end_dt.format('YYYY-MM-DD'),
            'start_month': start_dt.format('YYYY-MM'),
            'end_month': end_dt.format('YYYY-MM')
        }

    period_info = calculate_target_period()

    # 2. 원천 데이터 생성 (Source Generation)
    # 2-1. 축산차량 이동 네트워크
    sp_insert_tb_livestock_car_movement_network = PostgresOperator(
        task_id='sp_insert_tb_livestock_car_movement_network',
        postgres_conn_id='geoai_mt3',
        sql="""
            call geoai_view.sp_insert_tb_livestock_car_movement_network(
                '{{ task_instance.xcom_pull(task_ids='calculate_target_period', key='start_date_full') }}'::date, 
                '{{ task_instance.xcom_pull(task_ids='calculate_target_period', key='end_date_full') }}'::date
            );
        """
    )

    # 2-2. 도축장 출하 현황
    sp_insert_tb_slaughterhouse_shipment_status = PostgresOperator(
        task_id='sp_insert_tb_slaughterhouse_shipment_status',
        postgres_conn_id='geoai_mt3',
        sql="""
            call geoai_view.sp_insert_tb_slaughterhouse_shipment_status(
                '{{ task_instance.xcom_pull(task_ids='calculate_target_period', key='start_date_full') }}'::date, 
                '{{ task_instance.xcom_pull(task_ids='calculate_target_period', key='end_date_full') }}'::date
            );
        """
    )

    # 2-3. 우제류 이동 결과
    sp_insert_tb_artiodactyla_movement_result = PostgresOperator(
        task_id='sp_insert_tb_artiodactyla_movement_result',
        postgres_conn_id='geoai_mt3',
        sql="""
            call geoai_view.sp_insert_tb_artiodactyla_movement_result(
                '{{ task_instance.xcom_pull(task_ids='calculate_target_period', key='start_date_full') }}'::date, 
                '{{ task_instance.xcom_pull(task_ids='calculate_target_period', key='end_date_full') }}'::date
            );
        """
    )
    
    source_tasks = [
        sp_insert_tb_livestock_car_movement_network, 
        sp_insert_tb_slaughterhouse_shipment_status,
        sp_insert_tb_artiodactyla_movement_result
    ]

    # 3. 분석/집계 데이터 생성 (Analysis) - 12개 프로시저

    analysis_procs = [
        'sp_insert_batch_data_region_summary',
        'sp_insert_batch_data_count_rate',
        'sp_insert_batch_data_livestock_area_move',
        'sp_insert_batch_data_adjent_area_move',
        'sp_insert_batch_data_area_detail_move',
        'sp_insert_batch_data_livestock_car_gps_history',
        'sp_insert_batch_data_adjent_area_summary',
        'sp_insert_batch_data_facility_to_area',
        'sp_insert_batch_data_facility_departure_arrival',
        'sp_insert_batch_data_car_by_daily',
        'sp_insert_batch_data_monthly_to_area',
        'sp_insert_batch_data_slaughter_area_rate'
    ]

    analysis_tasks = []
    
    for proc_name in analysis_procs:
        sql_query = f"""
            call geoai_view.{proc_name}(
                '{{{{ task_instance.xcom_pull(task_ids='calculate_target_period', key='start_month') }}}}', 
                '{{{{ task_instance.xcom_pull(task_ids='calculate_target_period', key='end_month') }}}}'
            );
        """
        
        task = PostgresOperator(
            task_id=proc_name,
            postgres_conn_id='geoai_mt3',
            sql=sql_query
        )
        analysis_tasks.append(task)

    # 4. 의존성 설정 (Dependency)
    
    period_info >> source_tasks

    cross_downstream(source_tasks, analysis_tasks)