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
            sql = f'''SELECT * FROM {oracle_table} '''
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
            print(oracle_cols)
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
local_tz = pendulum.timezone("Asia/Seoul")
with DAG(
    dag_id='ASF_일일프로세스',
    description='ASF_프로세스2',
    start_date=pendulum.datetime(2025, 11, 4, tz="Asia/Seoul"),
    #schedule='30 7 * * *',
    schedule=None,
    catchup=False,
) as dag:
    CONF_STD = "{{ dag_run.conf.get('standard_date', (data_interval_end - macros.timedelta(days=1)).in_timezone('Asia/Seoul').format('YYYY-MM-DD')) }}"

    basic_table_list = ['tn_aph_dsnfc_manage_frmhs_info']
    for table in basic_table_list:
        with TaskGroup(group_id=f'copy_{table}') as tg:
            create_partition_table_task = create_partition_table.override(task_id=f'{table}_create_partition_table')(table)
            truncate_table_task = truncate_table.override(task_id=f'{table}_truncate_table')(create_partition_table_task)
            insert_data_task = insert_data.override(task_id=f'{table}_insert_data')(table)
            chain(create_partition_table_task, truncate_table_task, insert_data_task)

            
    db_link_table_list = ['tn_diss_occ_ntcn_info']
    for table in db_link_table_list:
        with TaskGroup(group_id=f'copy_{table}') as tg2:
            create_partition_table_task = create_partition_table.override(task_id=f'{table}_create_partition_table')(table)
            truncate_table_task = truncate_table.override(task_id=f'{table}_truncate_table')(create_partition_table_task)
            insert_data_task = insert_data_from_db_link.override(task_id=f'{table}_insert_data_from_db_link')(table, 'DPL', 'DL_DPL_KAHIS')
            chain(create_partition_table_task, truncate_table_task, insert_data_task)
    
    with TaskGroup(group_id='make_asf_data') as make_asf_data:
        sp_insert_tb_farm_information_asf = PostgresOperator(task_id='sp_insert_tb_farm_information_asf',
                                                        postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_farm_information_asf('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        sp_insert_tb_mother_pig_information = PostgresOperator(task_id='sp_insert_tb_mother_pig_information',
                                                        postgres_conn_id='geoai_mt3',
                                                       sql="""call geoai_mt.sp_insert_tb_mother_pig_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        sp_insert_tb_diseasecontrol_status_information_asf = PostgresOperator(task_id='sp_insert_tb_diseasecontrol_status_information_asf',
                                                       postgres_conn_id='geoai_mt3',
                                                        sql="""call geoai_mt.sp_insert_tb_diseasecontrol_status_information_asf('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        sp_insert_tb_farm_candidate_asf = PostgresOperator(task_id='sp_insert_tb_farm_candidate_asf',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_farm_candidate_asf('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        sp_insert_tb_farm_land_cover_asf = PostgresOperator(task_id='sp_insert_tb_farm_land_cover_asf',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_farm_land_cover_asf('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}','2025','24-25')""")

        sp_insert_tb_farm_elevation_asf = PostgresOperator(task_id='sp_insert_tb_farm_elevation_asf',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_farm_elevation_asf('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        sp_insert_tb_farm_forest_distance_8way = PostgresOperator(task_id='sp_insert_tb_farm_forest_distance_8way',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_farm_forest_distance_8way('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        sp_insert_tb_farm_slope_8way = PostgresOperator(task_id='sp_insert_tb_farm_slope_8way',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_farm_slope_8way('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        sp_insert_tb_farm_environment_type = PostgresOperator(task_id='sp_insert_tb_farm_environment_type',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_farm_environment_type('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        sp_insert_tb_calculation_wild_virus_distance_asf = PostgresOperator(task_id='sp_insert_tb_calculation_wild_virus_distance_asf',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_calculation_wild_virus_distance_asf('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        sp_insert_tb_calculation_farm_virus_distance_asf = PostgresOperator(task_id='sp_insert_tb_calculation_farm_virus_distance_asf',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_calculation_farm_virus_distance_asf('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        sp_insert_tb_car_visit_count_21day_asf = PostgresOperator(task_id='sp_insert_tb_car_visit_count_21day_asf',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_car_visit_count_21day_asf('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")

        sp_insert_tb_asf_information = PostgresOperator(task_id='sp_insert_tb_asf_information',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_asf_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
        
        
        sp_insert_tb_farm_habitat_possibility_asf = PostgresOperator(task_id='sp_insert_tb_farm_habitat_possibility_asf',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_farm_habitat_possibility_asf('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
        
        sp_insert_tb_wild_asf_information = PostgresOperator(task_id='sp_insert_tb_wild_asf_information',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_mt.sp_insert_tb_wild_asf_information('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
        
        sp_insert_tb_asf_farm_risk_nonscaled_train = PostgresOperator(task_id='sp_insert_tb_asf_farm_risk_nonscaled_train',
                                                         postgres_conn_id='geoai_mt3',
                                                         sql="""call geoai_prediction.sp_insert_tb_asf_farm_risk_nonscaled_train('{{ (data_interval_end - macros.timedelta(days=1)).in_timezone("Asia/Seoul").format("YYYY-MM-DD") }}')""")
        
      
        sp_insert_tb_farm_information_asf >>  [sp_insert_tb_farm_land_cover_asf, sp_insert_tb_farm_habitat_possibility_asf, sp_insert_tb_farm_elevation_asf, sp_insert_tb_car_visit_count_21day_asf] >> sp_insert_tb_farm_forest_distance_8way >> sp_insert_tb_asf_farm_risk_nonscaled_train
        sp_insert_tb_farm_information_asf >> sp_insert_tb_mother_pig_information >> sp_insert_tb_asf_farm_risk_nonscaled_train
        sp_insert_tb_farm_information_asf >> sp_insert_tb_diseasecontrol_status_information_asf >> sp_insert_tb_farm_candidate_asf >> sp_insert_tb_farm_slope_8way >> sp_insert_tb_farm_environment_type  >> sp_insert_tb_asf_information >> sp_insert_tb_wild_asf_information>>[sp_insert_tb_calculation_wild_virus_distance_asf, sp_insert_tb_calculation_farm_virus_distance_asf] >> sp_insert_tb_asf_farm_risk_nonscaled_train

    call_api_predict = SimpleHttpOperator(
                    task_id = f"call_api_predict",
                    http_conn_id = "kahis_flask_asf_risk",
                    method="POST",
                    endpoint="/asf_predict",
                    data = json.dumps({
                        "standard_date" : "{{ (data_interval_end - macros.timedelta(days=1)).in_timezone('Asia/Seoul').format('YYYY-MM-DD') }}",
                        

                    }),
                    headers={"Content-Type": "application/json"},
                    dag=dag
    )
    execute_realtime_risk_dag = TriggerDagRunOperator(
        task_id='execute_realtime_risk_dag',
        trigger_dag_id='실시간_차량_프로세스',
        execution_date="{{ data_interval_end }}",
        conf={"standard_date": CONF_STD},
        reset_dag_run=True,
        wait_for_completion=False
    )
    make_asf_view_process_dag = TriggerDagRunOperator(
        task_id='make_asf_view_process_dag',
        trigger_dag_id='make_asf_view_process',
        execution_date="{{ data_interval_end }}",
        conf={"standard_date": CONF_STD},
        reset_dag_run=True,
        wait_for_completion=False
    )
    

    tg >> tg2  >> make_asf_data >> call_api_predict >> [execute_realtime_risk_dag,make_asf_view_process_dag]