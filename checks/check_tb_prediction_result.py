import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import psycopg2
import sys
from datetime import datetime, timedelta
import sqlalchemy
from sqlalchemy import create_engine

class DatabaseHandler(logging.Handler):
    def __init__(self, log_name, db_conn_str):
        logging.Handler.__init__(self)
        self.conn_str = db_conn_str
        self.conn = psycopg2.connect(self.conn_str)
        self.cur = self.conn.cursor()
        self.log_name = log_name

        mk_table_query = f'''
            create table if not exists geoai_logs.{self.log_name}(
                id SERIAL primary key,
                logLv varchar,
                filename varchar,
                lineno varchar,
                message varchar,
                create_dt timestamp default now()
            );
        '''
        self.cur.execute(mk_table_query)
        self.conn.commit()

    def emit(self, record):
        self.format(record)
        insert_db_query = f'''
            insert into geoai_logs.{self.log_name}
            ''' + '''(loglv, filename, lineno, message) VALUES
            ('{}', '{}', '{}', '{}')
        '''.format(record.levelname, record.filename, record.lineno, record.message)

        self.cur.execute(insert_db_query)
        self.conn.commit()

    def __del__(self):
        try:
            self.conn.close()
        except:
            pass


def check_tb_prediction_result(standard_date, db_conn_str):

    # 로깅 설정
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [tb_prediction_result] - %(message)s')
    logger = logging.getLogger()
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - [tb_prediction_result] - %(message)s')

    # 로그 파일 핸들러 설정
    file_handler = RotatingFileHandler('app_tb_prediction_result.log', mode='a', maxBytes=100*1024*1024, backupCount=2)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    # 로그 DB 핸들러 설정
    db_handler = DatabaseHandler('tb_prediction_result', db_conn_str)
    db_handler.setFormatter(log_format)
    logger.addHandler(db_handler)

    # conn = psycopg2.connect(db_conn_str)
    # cur = conn.cursor()

    with create_engine(db_conn_str).connect() as conn:
        results = pd.read_sql(f"""select * from geoai_mt.tb_prediction_result where standard_date = '{ standard_date }';""", conn)
        train = pd.read_sql(f"""select * from geoai_mt.tb_trainingset where standard_date = '{ standard_date }';""", conn)
        results_prev = pd.read_sql(f"""select * from geoai_mt.tb_prediction_result where standard_date = to_date('{standard_date}') - interval '1 days';""", conn)

        if len(results) != len(train):
            message = f"{ standard_date }의 tb_prediction_result 데이터수가 tb_trainingset의 데이터수와 일치하지 않습니다."
            logger.error(message)

        df_comparison = (
            pd.merge(results, results_prev, how='left', on=['farm_serial_no'], suffixes=('', '_prev'))
            .dropna(subset=["standard_date_prev"])  # 전날 데이터 없는 농가 삭제
        )

        mask = (df_comparison['infection_risk_rank'] >= 0.95) | (df_comparison['infection_risk_rank_prev'] >= 0.95)

        for index, row in df_comparison[mask].iterrows():

            if abs((row['infection_risk_rank'] - row['infection_risk_rank_prev']) > 0.1):
                error_message = f"{row['farm_serial_no']}의 예측값이 크게 차이납니다. 당일 예측치: {row['infection_risk_rank']}, 전일 예측치: {row['infection_risk_rank_prev']}"
                logger.error(error_message)
                raise ValueError(error_message)

        logger.info(f"{standard_date}의 감염 예측이 성공적으로 분석되었습니다.")


if __name__ == '__main__':
    standard_date = '2023-12-18'
    db_conn_str = 'postgresql://geoai:1234@10.10.12.84:15432/geoai'
    check_tb_prediction_result(standard_date, db_conn_str)