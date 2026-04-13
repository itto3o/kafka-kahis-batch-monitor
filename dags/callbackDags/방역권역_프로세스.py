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
from airflow.models.baseoperator import chain
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task()
def something(data_interval_start=None, data_interval_end=None):
    print(f'테스트 data_interval_start:{data_interval_start}, data_interval_end:{data_interval_end}')

def oracle_to_postgres(postgresql_table, oracle_table, std_dt, where_sql=None, row_check=True):
    size = 100000
    oracle_table = oracle_table.upper()
   
    with PostgresHook(postgres_conn_id=postgresql_conn_id).get_conn() as pg_connection:
        with OracleHook(oracle_conn_id=oracle_conn_id).get_conn() as orcl_connection:
            start = time()
            cnt = 0

            pg_curl = pg_connection.cursor()
            pg_curl.itersize = size
           
            orcl_cur = orcl_connection.cursor()
            orcl_cur.arraysize = size
           
            #pg_curl.execute(f"delete from {postgresql_table} where std_dt = '{std_dt}'")
           
            oracle_cols = [col[0] for col in orcl_cur.execute(f"""select COLUMN_NAME
                                                from ALL_TAB_COLUMNS
                                                where OWNER = '{oracle_table.split(".")[0]}'
                                                and TABLE_NAME = '{oracle_table.split(".")[1]}'
                                                ORDER BY COLUMN_ID""").fetchall()]
            cols_str = ','.join(oracle_cols)
            cols_str = cols_str.replace('AG_GEOM', 'NULL').replace('GEOM', 'NULL')
            sql = f'''SELECT {cols_str} FROM {oracle_table} '''
            if where_sql:
                sql = sql + where_sql
            oracle_cols = cols_str.replace('NULL,','').split(',')
           
            # 테이블 데이터 이관 시에 데이터가 정상적으로 옮겨졌는지 데이터 갯수 확인 (전체 데이터 + 컬럼별)
            oracle_total_count = orcl_cur.execute(f"select count(*), {','.join([('count(a.' + col + ')') for col in oracle_cols])} from ({sql}) a").fetchall()[0]
           

            orcl_cur.execute(sql)
            while True:
                rows = orcl_cur.fetchmany()
                if not rows: break

                # data 앞에 std_dt 붙여 주는 작업
                res = []
                if std_dt:
                    for r in rows:
                        res.append((std_dt,) + r)
                else:
                    res = rows


                execute_values(pg_curl, f"insert into {postgresql_table} values %s", res)
                cnt += len(rows)
                print(f'{cnt}, {cnt / (time() - start)}/s')
            pg_curl.execute(f"select count(*), {','.join([('count(a.' + db_system_column_name(col) + ')') for col in oracle_cols])} from (select * from {postgresql_table} where std_dt = '{std_dt}') a")
            postgresql_total_count = pg_curl.fetchall()[0]
           
            # 테이블 데이터 오라클과 postgresql 갯수 확인 갯수 차이가 있을 시에 ValueError
            if row_check and oracle_total_count != postgresql_total_count:
                raise ValueError(f"{oracle_table} 테이블 데이터 이관 실패(oracle:{oracle_total_count}, postgresql:{postgresql_total_count})")
            pg_connection.commit()

def oracle_dblink_to_postgres(postgresql_table, oracle_table, db_link, std_dt, where_sql=None, row_check=True):
    size = 100000
    oracle_table = oracle_table.upper()

    with PostgresHook(postgres_conn_id=postgresql_conn_id).get_conn() as pg_connection:
        with OracleHook(oracle_conn_id=oracle_conn_id).get_conn() as orcl_connection:
            start = time()
            cnt = 0

            pg_curl = pg_connection.cursor()
            pg_curl.itersize = size
            
            orcl_cur = orcl_connection.cursor()
            orcl_cur.arraysize = size
            
            # 1. Oracle 컬럼 정보 가져오기
            oracle_cols_raw = orcl_cur.execute(f"""select COLUMN_NAME
                                                from ALL_TAB_COLUMNS@{db_link}
                                                where OWNER = '{oracle_table.split(".")[0]}'
                                                and TABLE_NAME = '{oracle_table.split(".")[1]}'
                                                ORDER BY COLUMN_ID""").fetchall()
            
            oracle_cols = [col[0] for col in oracle_cols_raw]

            # GEOM 컬럼 처리 등 기존 로직
            cols_str = ','.join(oracle_cols)
            cols_str = cols_str.replace('AG_GEOM', 'NULL').replace('GEOM', 'NULL')
            
            sql = f'''SELECT {cols_str} FROM {oracle_table}@{db_link} '''
            if where_sql:
                sql = sql + where_sql
            
            orcl_cur.execute(sql)
            
            # [핵심 수정 사항] 
            # 데이터를 넣을 타겟(Postgres) 컬럼명 리스트를 명시적으로 생성합니다.
            insert_target_cols = [db_system_column_name(col) for col in oracle_cols]
            
            # std_dt(파티션 날짜)가 있다면 맨 앞에 컬럼명 추가
            if std_dt:
                insert_target_cols.insert(0, 'std_dt') 
            
            # 컬럼명들을 콤마로 연결 (예: "std_dt, shipmnt_de, ...")
            insert_columns_str = ','.join(insert_target_cols)

            while True:
                rows = orcl_cur.fetchmany()
                if not rows: break

                res = []
                if std_dt:
                    for r in rows:
                        res.append((std_dt,) + r)
                else:
                    res = rows

                # [수정] 컬럼명을 명시하여 INSERT 실행
                # 기존: insert into table values ...
                # 변경: insert into table (col1, col2...) values ...
                execute_values(pg_curl, f"insert into {postgresql_table} ({insert_columns_str}) values %s", res)
                
                cnt += len(rows)
                print(f'{cnt}, {cnt / (time() - start)}/s')
            
            pg_connection.commit()
           
           
def oracle_to_postgres_no_check_column(postgresql_table, oracle_table, sql, std_dt):
    size = 100000

    with PostgresHook(postgres_conn_id=postgresql_conn_id).get_conn() as pg_connection:
        with OracleHook(oracle_conn_id=oracle_conn_id).get_conn() as orcl_connection:
            start = time()
            cnt = 0

            pg_curl = pg_connection.cursor()
            pg_curl.itersize = size

            orcl_cur = orcl_connection.cursor()
            orcl_cur.arraysize = size
           
            #pg_curl.execute(f"delete from {postgresql_table} where std_dt = '{std_dt}'")
           
            # 테이블 데이터 이관 시에 데이터가 정상적으로 옮겨졌는지 데이터 갯수 확인 (전체 데이터)
            oracle_total_count = orcl_cur.execute(f"select count(*) from ({oracle_table}) a").fetchall()[0][0]

            orcl_cur.execute(sql)
            while True:
                rows = orcl_cur.fetchmany()
                if not rows: break

                # data 앞에 std_dt 붙여 주는 작업
                res = []
                if std_dt:
                    for r in rows:
                        res.append((std_dt,) + r)
                else:
                    res = rows

                execute_values(pg_curl, f"insert into {postgresql_table} values %s", res)
                cnt += len(rows)
                print(f'{cnt}, {cnt / (time() - start)}/s')
            pg_curl.execute(f"select count(*) from (select * from {postgresql_table} where std_dt = '{std_dt}') a")
            postgresql_total_count = pg_curl.fetchall()[0][0]
           
            # 테이블 데이터 오라클과 postgresql 갯수 확인 갯수 차이가 있을 시에 ValueError
            #if oracle_total_count != postgresql_total_count:
             #   raise ValueError(f"{oracle_table} 테이블 데이터 이관 실패")
            pg_connection.commit()
                 
def db_system_column_name(col_name):
    # postgresql에서 xmin xmax 컬럼 명을 사용중이므로 완벽하게 오라클 디비와 컬럼명을 맞추지 못하여 만든 함수
    if col_name == 'XMIN':
        return 'xmin_'
    if col_name == 'XMAX':
        return 'xmax_'
    return col_name

@task(task_id='create_partition_table')
def create_partition_table(table_name: str, data_interval_start=None):
    with PostgresHook(postgres_conn_id=postgresql_conn_id).get_conn() as pg_connection:
        partition_date = data_interval_start.in_timezone("Asia/Seoul").format("YYYYMMDD")
        pg_connection.cursor().execute(f""" call geoai_mt.generate_std_dt_partition_table('m2msys', '{table_name}', 'm2msys_partition', '{partition_date}', '{partition_date}'); """)
    return f'{table_name}_{partition_date}'    
   
@task(task_id='truncate_table')
def truncate_table(table_name: str):
    with PostgresHook(postgres_conn_id=postgresql_conn_id).get_conn() as pg_connection:
        pg_connection.cursor().execute(f""" truncate table m2msys_partition.{table_name} """)
       
@task(task_id='delete_data_table')
def delete_data_table(table_name: str, data_interval_start=None):
    with PostgresHook(postgres_conn_id=postgresql_conn_id).get_conn() as pg_connection:
        partition_date = data_interval_start.in_timezone("Asia/Seoul").format("YYYYMMDD")
        pg_connection.cursor().execute(f""" delete from m2msys.{table_name} where std_dt = '{partition_date}' """)

@task(task_id='insert_data')
def insert_data(table_name: str, date_column=None, row_check=True, data_interval_start=None, data_interval_end=None):
    postgres_table_name = f'm2msys.{table_name}'
    oracle_table_name = postgres_table_name
    partition_start_date = data_interval_start.in_timezone("Asia/Seoul").format("YYYY-MM-DD")
    partition_end_date = data_interval_end.in_timezone("Asia/Seoul").format("YYYY-MM-DD")
   
    where_sql = None
    if date_column:
        date_column = date_column.upper()
        where_sql = f""" where {date_column} >= TO_DATE('{partition_start_date}', 'YY-MM-DD') and {date_column} < TO_DATE('{partition_end_date}', 'YY-MM-DD')"""
    oracle_to_postgres(postgres_table_name, postgres_table_name, partition_start_date, where_sql, row_check)

@task(task_id='insert_data_from_db_link')
def insert_data_from_db_link(table_name: str, oracle_schema: str, db_link_name: str, date_column=None, data_interval_start=None, data_interval_end=None):
    postgres_table_name = f'm2msys.{table_name}'
    oracle_table_name = f'{oracle_schema}.{table_name}'
    db_link_name = db_link_name.upper()
    partition_start_date = data_interval_start.in_timezone("Asia/Seoul").format("YYYY-MM-DD")
    partition_end_date = data_interval_end.in_timezone("Asia/Seoul").format("YYYY-MM-DD")
   
    where_sql = None
    if date_column:
        date_column = date_column.upper()
        where_sql = f""" where {date_column} >= TO_DATE('{partition_start_date}', 'YY-MM-DD') and {date_column} < TO_DATE('{partition_end_date}', 'YY-MM-DD')"""
   
    oracle_dblink_to_postgres(postgres_table_name, oracle_table_name, db_link_name, partition_start_date, where_sql)

oracle_conn_id = "M2MSYS"
postgresql_conn_id = "bv_postgresql"
local_tz = pendulum.timezone("Asia/Seoul")

@dag(
    dag_id='방역권역_프로세스',
    description='방역권역_프로세스',
    default_args={"on_failure_callback": on_task_failure},
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 4, tz="Asia/Seoul"),
    catchup=False,
    tags=['daily', 'batch'],
)
def 방역권역_데이터_생성():
    last_tasks = []
    db_link_table_list = ['tn_slauhouse_shipmnt_sttus']
    for table in db_link_table_list:
        with TaskGroup(group_id=f'copy_{table}') as tg:
            create_partition_table_task = create_partition_table.override(task_id=f'tn_slauhouse_shipmnt_sttus_create_partition_table')(table)
            truncate_table_task = truncate_table.override(task_id=f'{table}_truncate_table')(create_partition_table_task)
            insert_data_task = insert_data_from_db_link.override(task_id=f'{table}_insert_data_from_db_link')(table, 'DPL', 'DL_DPL_KAHIS')
            chain(create_partition_table_task, truncate_table_task, insert_data_task)
           
    last_tasks.append(insert_data_task)
   
    sp_insert_tb_farm_integration_information = PostgresOperator(task_id='sp_insert_tb_farm_integration_information',
                                                    postgres_conn_id='geoai_mt3',
                                                    sql="""call geoai_mt.sp_insert_tb_farm_integration_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
   
    sp_insert_tb_livestock_species_integration_information = PostgresOperator(task_id='sp_insert_tb_livestock_species_integration_information',
                                                    postgres_conn_id='geoai_mt3',
                                                    sql="""call geoai_mt.sp_insert_tb_livestock_species_integration_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

    sp_insert_tb_car_visit_information = PostgresOperator(task_id='sp_insert_tb_car_visit_integration_information',
                                                   postgres_conn_id='geoai_mt3',
                                                   sql="""call geoai_mt.sp_insert_tb_car_visit_integration_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

    sp_insert_tb_livestock_car_movement_history = PostgresOperator(task_id='sp_insert_tb_livestock_car_movement_history',
                                                     postgres_conn_id='geoai_mt3',
                                                     sql="""call geoai_view.sp_insert_tb_livestock_car_movement_history('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
   
    sp_insert_tb_slaughterhouse_shipment_history = PostgresOperator(task_id='sp_insert_tb_slaughterhouse_shipment_history',
                                                     postgres_conn_id='geoai_mt3',
                                                     sql="""call geoai_mt.sp_insert_tb_slaughterhouse_shipment_history('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
   

    # [추가됨] 질병 관련 농가 정보 TaskGroup
    with TaskGroup(group_id='disease_farm_info_group') as disease_farm_info_group:
        sp_insert_tb_asf_farm_information = PostgresOperator(
            task_id='sp_insert_tb_asf_farm_information',
            postgres_conn_id='geoai_mt3',
            sql="""call geoai_view.sp_insert_tb_asf_farm_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')"""
        )

        sp_insert_tb_fmd_farm_information = PostgresOperator(
            task_id='sp_insert_tb_fmd_farm_information',
            postgres_conn_id='geoai_mt3',
            sql="""call geoai_view.sp_insert_tb_fmd_farm_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')"""
        )

        sp_insert_tb_hpai_farm_information = PostgresOperator(
            task_id='sp_insert_tb_hpai_farm_information',
            postgres_conn_id='geoai_mt3',
            sql="""call geoai_view.sp_insert_tb_hpai_farm_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')"""
        )

        sp_insert_tb_lsd_farm_information = PostgresOperator(
            task_id='sp_insert_tb_lsd_farm_information',
            postgres_conn_id='geoai_mt3',
            sql="""call geoai_view.sp_insert_tb_lsd_farm_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')"""
        )

    # 가장 마지막 task
    sp_insert_common_standard = PostgresOperator(task_id='sp_insert_common_standard',
                                                     postgres_conn_id='geoai_mt3',
                                                     sql="""call geoai_view.sp_insert_common_standard('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
   

    # 기존 흐름 >> 신규 그룹 >> 최종 Task
    tg >> sp_insert_tb_farm_integration_information >> sp_insert_tb_livestock_species_integration_information >> sp_insert_tb_car_visit_information >> sp_insert_tb_livestock_car_movement_history >> sp_insert_tb_slaughterhouse_shipment_history >> sp_insert_tb_asf_farm_information >> sp_insert_tb_fmd_farm_information >> sp_insert_tb_hpai_farm_information >> sp_insert_tb_lsd_farm_information >> sp_insert_common_standard

   
방역권역_데이터_생성()