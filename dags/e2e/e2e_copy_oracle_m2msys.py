"""
E2E: Oracle M2MSYS.TN_MOBILE_BLVSTCK_HIST → PostgreSQL m2m_data.tn_mobile_blvstck_hist 복제

운영 copy_oracle_m2msys보다 훨씬 단순화. 사육두수 검증 흐름 확인에 필요한 HIST 한 테이블만 복제.
실제 e2e 시나리오에서 Spring 측은 application-local.yml로 Oracle DPL PDB에 접속해 @M2MSYS DB Link로 직접
HIST를 조회하므로 이 복제 결과를 Spring이 사용하지는 않는다. "Oracle→Postgres 파이프라인이 한 번 정상 동작"을
확인하는 용도.
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

ORACLE_CONN_ID = "oracle_m2m"
POSTGRES_CONN_ID = "postgres_local"


def copy_blvstck_hist(**_):
    pg_conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        pg_cur = pg_conn.cursor()
        pg_cur.execute("CREATE SCHEMA IF NOT EXISTS m2m_data;")
        pg_cur.execute(
            """
            CREATE TABLE IF NOT EXISTS m2m_data.tn_mobile_blvstck_hist (
                frmhs_no VARCHAR(20),
                lstksp_cl VARCHAR(10),
                brd_had_co NUMERIC,
                last_change_dt TIMESTAMP
            );
            """
        )
        pg_cur.execute("TRUNCATE TABLE m2m_data.tn_mobile_blvstck_hist;")

        orcl_conn = OracleHook(oracle_conn_id=ORACLE_CONN_ID).get_conn()
        try:
            orcl_cur = orcl_conn.cursor()
            orcl_cur.execute(
                "SELECT FRMHS_NO, LSTKSP_CL, BRD_HAD_CO, LAST_CHANGE_DT FROM TN_MOBILE_BLVSTCK_HIST"
            )
            rows = orcl_cur.fetchall()
        finally:
            orcl_conn.close()

        for row in rows:
            pg_cur.execute(
                "INSERT INTO m2m_data.tn_mobile_blvstck_hist VALUES (%s, %s, %s, %s)",
                row,
            )
        pg_conn.commit()
        print(f"복제 완료: {len(rows)} rows")
    finally:
        pg_conn.close()


with DAG(
    dag_id="e2e_copy_oracle_m2msys",
    description="E2E: Oracle 사육두수 HIST → PostgreSQL 복제 (간소화)",
    start_date=pendulum.datetime(2026, 5, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["e2e", "test"],
) as dag:
    PythonOperator(
        task_id="copy_tn_mobile_blvstck_hist",
        python_callable=copy_blvstck_hist,
    )