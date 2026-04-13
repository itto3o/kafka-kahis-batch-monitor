import logging
from datetime import datetime, timedelta
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


def check_tb_livestock_species_information(std_dt, db_conn_str):
    # 로깅 설정
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [tb_livestock_species_information] - %(message)s')
    logger = logging.getLogger()
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - [tb_livestock_species_information] - %(message)s')

    # 로그 파일 핸들러 설정
    file_handler = RotatingFileHandler('app_tb_livestock_species_information.log', mode='a', maxBytes=100 * 1024 * 1024, backupCount=2)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    # 로그 DB 핸들러 설정
    db_handler = DatabaseHandler('tb_livestock_species_information', db_conn_str)
    db_handler.setFormatter(log_format)
    logger.addHandler(db_handler)

    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor()

    def get_table(std_dt):
        try:
            cur.execute(f"""select * from geoai_mt.tb_livestock_species_information where standard_date = '{std_dt}';""")
            df = cur.fetchall()
            ret = pd.DataFrame(df, dtype='str')

            if ret.empty:
                message = f"{std_dt}의 축종정보가 존재하지 않습니다."
                logger.error(message)

            ret.columns = [desc[0] for desc in cur.description]

            ret["present_breeding_livestock_count"] = ret["present_breeding_livestock_count"].astype('float')
            ret["present_breeding_livestock_count_average"] = ret["present_breeding_livestock_count_average"].astype('float')
            ret = ret[ret["farm_serial_no"] != '        ']
            return ret

        except ValueError as e:

            logger.error(f'Error occurred: {e}')
            raise

    df = get_table(std_dt)

    date_obj = datetime.strptime(std_dt, '%Y-%m-%d')
    prev_date = date_obj - timedelta(days=1)
    prev_date_string = prev_date.strftime('%Y-%m-%d')
    prev_df = get_table(prev_date_string)

    df_comparison = (
        pd.merge(df, prev_df, how='left', on=['farm_serial_no', 'livestock_species_class_code'], suffixes=('', '_prev'))
        .dropna(subset=["standard_date_prev"])  # 전날 데이터 없는 농가 삭제
    )
    mask = (df_comparison['present_breeding_livestock_count'] > 1000) & (df_comparison['present_breeding_livestock_count'] != 0) & (df_comparison['present_breeding_livestock_count_prev'] != 0)
    for index, row in df_comparison[mask].iterrows():

        if row['present_breeding_livestock_count'] > 100 * row['present_breeding_livestock_count_prev'] or row['present_breeding_livestock_count'] < 0.01 * row['present_breeding_livestock_count_prev']:
            error_message = f"{row['farm_serial_no']}의 {row['livestock_species_class_code']} 사육두수 비교에 이상이 감지되었습니다. 당일 사육두수: {row['present_breeding_livestock_count']}, 전일 사육두수: {row['present_breeding_livestock_count_prev']}"
            logger.error(error_message)
            raise ValueError(error_message)

        elif row['present_breeding_livestock_count'] > 10 * row['present_breeding_livestock_count_prev'] or row['present_breeding_livestock_count'] < 0.1 * row['present_breeding_livestock_count_prev']:
            error_message = f"{row['farm_serial_no']}의 {row['livestock_species_class_code']} 사육두수 비교에 이상이 감지되었습니다. 당일 사육두수: {row['present_breeding_livestock_count']}, 전일 사육두수: {row['present_breeding_livestock_count_prev']}"
            logger.warning(error_message)

    logger.info(f"{std_dt}의 축종정보가 성공적으로 로드되었습니다.")
    conn.close()


if __name__ == '__main__':
    std_dt = '2023-12-19'
    db_conn_str = 'postgresql://geoai:1234@10.10.12.84:15432/geoai'
    check_tb_livestock_species_information(std_dt, db_conn_str)
