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
            
            #pg_curl.execute(f"delete from {postgresql_table} where std_dt = '{std_dt}'")
            
            oracle_cols = [col[0] for col in orcl_cur.execute(f"""select COLUMN_NAME 
                                                from ALL_TAB_COLUMNS@{db_link} 
                                                where OWNER = '{oracle_table.split(".")[0]}' 
                                                  and TABLE_NAME = '{oracle_table.split(".")[1]}'
                                                ORDER BY COLUMN_ID""").fetchall()]
            cols_str = ','.join(oracle_cols)
            cols_str = cols_str.replace('AG_GEOM', 'NULL').replace('GEOM', 'NULL')
            sql = f'''SELECT {cols_str} FROM {oracle_table}@{db_link} '''
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
            if oracle_total_count != postgresql_total_count:
                raise ValueError(f"{oracle_table} 테이블 데이터 이관 실패")
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
    from psycopg2 import errors

    conn = PostgresHook(postgres_conn_id=postgresql_conn_id).get_conn()
    conn.autocommit = False  # 트랜잭션 수동 관리 시작
    cursor = conn.cursor()
    lock_timeout_seconds = 60
    try:
        timeout_query = f"SET LOCAL lock_timeout = '{lock_timeout_seconds}s';"
        cursor.execute(timeout_query)
        print(f"🔒 세션 lock_timeout을 {lock_timeout_seconds}초로 설정했습니다.")

        truncate_query = f"TRUNCATE TABLE m2msys_partition.{table_name}"
        print(f"🔪 TRUNCATE 시도 중: {truncate_query}")

        cursor.execute(truncate_query)
        conn.commit()
        print(f"✅ TRUNCATE 성공: {table_name} 테이블을 빠르게 비웠습니다.")

    except errors.LockNotAvailable as e:
        if 'lock timeout' in str(e):
            print(f"⚠️ 경고: {lock_timeout_seconds}초 안에 잠금 획득 실패. TRUNCATE 롤백 및 DELETE 대체 시작.")
            conn.rollback()

            try:
                delete_query = f"DELETE FROM m2msys_partition.{table_name}"
                print(f"🗑️ DELETE 대체 실행 중: {delete_query}")

                cursor.execute(delete_query)

                conn.commit()
                print(f"✅ DELETE 성공: {table_name} 테이블을 비웠습니다.")

            except Exception as delete_e:
                conn.rollback()
                print(f"❌ 최종 실패: DELETE 시도 중에도 에러 발생: {delete_e}")
                raise
        else:
            conn.rollback()
            raise
    except Exception as e:
        conn.rollback()
        print(f"❌ 예상치 못한 에러 발생: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


        
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
@dag(
    dag_id='copy_oracle_m2msys',
    schedule='0 5 * * *',
    #schedule=None,
    start_date=pendulum.datetime(2025, 8, 4, tz="Asia/Seoul"),
    catchup=False,
    tags = ['daily', 'batch'],
    description='검역본부 원천데이터 Load(oracle to postgres)',
    default_args={"on_failure_callback": on_task_failure},
)
def copy_oracle_m2msys():
    CONF_STD = "{{ (data_interval_end - macros.timedelta(days=1)).in_timezone('Asia/Seoul').format('YYYY-MM-DD') }}"
    last_tasks = []
    
    basic_table_list = ['tn_mobile_blvstck', 'tn_mobile_frmhs_info', 'tn_vhcle_inout', 'tn_dsnfc_manage_frmhs_info', 
                       'tn_diss_occrrnc_frmhs', 'tn_wildbird_info', 'tn_mobile_subscriber', 'tn_vhcle_drvr', 
                       'tn_dsnfc_afflts_manage', 'nvrqs_mobile_farms', 'tn_mobile_frmhs_cord_new', 'tn_mobile_frmhs_info_map', 'tn_mobile_frmhs_cord']    
    for table in basic_table_list:
        with TaskGroup(group_id=f'copy_{table}') as tg:
            create_partition_table_task = create_partition_table.override(task_id=f'{table}_create_partition_table')(table)
            truncate_table_task = truncate_table.override(task_id=f'{table}_truncate_table')(create_partition_table_task)
            insert_data_task = insert_data.override(task_id=f'{table}_insert_data')(table)
            chain(create_partition_table_task, truncate_table_task, insert_data_task)
            last_tasks.append(insert_data_task)
    
    db_link_table_list = ['tn_stkrs_frmhs']
    for table in db_link_table_list:
        with TaskGroup(group_id=f'copy_{table}') as tg:
            create_partition_table_task = create_partition_table.override(task_id=f'{table}_create_partition_table')(table)
            truncate_table_task = truncate_table.override(task_id=f'{table}_truncate_table')(create_partition_table_task)
            insert_data_task = insert_data_from_db_link.override(task_id=f'{table}_insert_data_from_db_link')(table, 'DPL', 'DL_DPL_KAHIS')
            chain(create_partition_table_task, truncate_table_task, insert_data_task)
            last_tasks.append(insert_data_task)
        
    change_dt_table_list = ['tn_mobile_blvstck_hist', 'tn_mobile_frmhs_info_hist']
    for table in change_dt_table_list:
        with TaskGroup(group_id=f'copy_{table}') as tg:
            create_partition_table_task = create_partition_table.override(task_id=f'{table}_create_partition_table')(table)
            truncate_table_task = truncate_table.override(task_id=f'{table}_truncate_table')(create_partition_table_task)
            insert_data_task = insert_data.override(task_id=f'{table}_insert_data')(table, 'change_dt')
            chain(create_partition_table_task, truncate_table_task, insert_data_task)
            last_tasks.append(insert_data_task)
        
    frst_creat_dt_db_link_table_list = ['tn_dissch_result', 'tn_dissch_result_detail']
    for table in frst_creat_dt_db_link_table_list:
        with TaskGroup(group_id=f'copy_{table}') as tg:
            create_partition_table_task = create_partition_table.override(task_id=f'{table}_create_partition_table')(table)
            truncate_table_task = truncate_table.override(task_id=f'{table}_truncate_table')(create_partition_table_task)
            insert_data_task = insert_data_from_db_link.override(task_id=f'{table}_insert_data_from_db_link')(table, 'DPL', 'DL_DPL_KAHIS', 'frst_creat_dt')
            chain(create_partition_table_task, truncate_table_task, insert_data_task)
            last_tasks.append(insert_data_task)
        
    frst_creat_dt_table_list = ['tn_visit_info', 'tn_visit_info_new']
    for table in frst_creat_dt_table_list:
        with TaskGroup(group_id=f'copy_{table}') as tg:
            create_partition_table_task = create_partition_table.override(task_id=f'{table}_create_partition_table')(table)
            truncate_table_task = truncate_table.override(task_id=f'{table}_truncate_table')(create_partition_table_task)
            insert_data_task = insert_data.override(task_id=f'{table}_insert_data')(table, 'frst_creat_dt')
            chain(create_partition_table_task, truncate_table_task, insert_data_task)
            last_tasks.append(insert_data_task)
        
    pstng_dt_table_list = ['tn_rltm_mntrng']
    for table in pstng_dt_table_list:
        with TaskGroup(group_id=f'copy_{table}') as tg:
            create_partition_table_task = create_partition_table.override(task_id=f'{table}_create_partition_table')(table)
            truncate_table_task = truncate_table.override(task_id=f'{table}_truncate_table')(create_partition_table_task)
            insert_data_task = insert_data.override(task_id=f'{table}_insert_data')(table, 'pstng_dt', row_check=False)
            chain(create_partition_table_task, truncate_table_task, insert_data_task)
            last_tasks.append(insert_data_task)
        
   # execute_make_ml_data_new_dag = TriggerDagRunOperator(
    #    task_id='execute_make_ml_data_new_dag',
     #   trigger_dag_id='make_ml_data_new',
      #  wait_for_completion=False
    #)
    call_geoai_mt_sp_insert_tb_car_enter_information = PostgresOperator(
            task_id='call_geoai_mt_sp_insert_tb_car_enter_information',
            postgres_conn_id=postgresql_conn_id,
            sql="call geoai_mt.sp_insert_tb_car_enter_information" + """('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}');""")
    
    execute_위험도데이터준비_dag = TriggerDagRunOperator(
        task_id='execute_위험도데이터준비_dag',
        trigger_dag_id='make_risk_data',
        execution_date="{{ data_interval_end }}",
        conf={"standard_date": CONF_STD},
        reset_dag_run=True,
        wait_for_completion=False
    )
    
    last_tasks >> call_geoai_mt_sp_insert_tb_car_enter_information >>execute_위험도데이터준비_dag
    #last_tasks >> execute_차량_GPS_위험도_dag
    
copy_oracle_m2msys()