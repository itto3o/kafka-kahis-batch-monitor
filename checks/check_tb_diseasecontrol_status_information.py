import logging
from logging.handlers import RotatingFileHandler

import pandas as pd
import psycopg2


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


def check_tb_diseasecontrol_status_information(std_dt, db_conn_str):
    # 로깅 설정
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [tb_diseasecontrol_status_information] - %(message)s')
    logger = logging.getLogger()
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - [tb_diseasecontrol_status_information] - %(message)s')

    # 로그 파일 핸들러 설정
    file_handler = RotatingFileHandler('app_tb_diseasecontrol_status_information.log', mode='a', maxBytes=100 * 1024 * 1024,
                                       backupCount=2)  # 메가 설정
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - [tb_diseasecontrol_status_information] - %(message)s'))
    logger.addHandler(file_handler)

    # 로그 DB 핸들러 설정
    db_handler = DatabaseHandler('tb_diseasecontrol_status_information', db_conn_str)
    db_handler.setFormatter(log_format)
    logger.addHandler(db_handler)

    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor()

    def get_table(std_dt, cur):
        try:
            cur.execute(f"""select distinct * from geoai_mt.tb_diseasecontrol_status_information where standard_date = '{std_dt}';""")
            df = cur.fetchall()
            ret = pd.DataFrame(df, dtype='str')

            if ret.empty:
                logger.error(f"{std_dt}의 방역카드 정보가 존재하지 않습니다.")

            ret.columns = [desc[0] for desc in cur.description]

            # ret.drop_duplicates(subset=['farms_no'], inplace=True)

            return ret

        except ValueError as e:

            logger.error(f'Error occurred: {e}')
            raise

    df = get_table(std_dt, cur)

    def get_count(std_dt):

        try:
            cur.execute(
                f"""select count(distinct standard_date), count(farm_serial_no) from geoai_mt.tb_diseasecontrol_status_information 
                where standard_date < to_date('{std_dt}') and standard_date >= to_date('{std_dt}') - interval '30 days';""")
            results = cur.fetchall()
            cnt_avg = (results[0][1]) / (results[0][0])

            return cnt_avg

        except ValueError as e:
            logger.error(f'Error occurred: {e}')
            raise

    cnt_avg = get_count(std_dt)


    # 한달 간의 평균과 기준일의 농가수가 10% 이상 차이날 때, 오류 발생

    if abs(len(df) - cnt_avg) > (cnt_avg * 0.1):
        error_message = f"농장 정보의 개수가 한달 평균 개수와 10% 이상 차이납니다. 확인이 필요합니다."
        logger.error(error_message)
        raise ValueError(error_message)


    logger.info(f"{std_dt}의 방역카드 정보가 성공적으로 로드되었습니다.")

    conn.close()


if __name__ == '__main__':
    std_dt = '2023-12-19'
    db_conn_str = 'postgresql://geoai:1234@10.10.12.84:15432/geoai'
    check_tb_diseasecontrol_status_information(std_dt, db_conn_str)
