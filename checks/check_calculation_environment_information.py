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


def check_calculation_environment_information(std_dt, db_conn_str):
    # 로깅 설정
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [calculation_environment_information] - %(message)s')
    logger = logging.getLogger()
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - [calculation_environment_information] - %(message)s')

    # 로그 파일 핸들러 설정
    file_handler = RotatingFileHandler('app_calculation_environment_information.log', mode='a', maxBytes=100 * 1024 * 1024, backupCount=2)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    # 로그 DB 핸들러 설정
    db_handler = DatabaseHandler('calculation_environment_information', db_conn_str)
    db_handler.setFormatter(log_format)
    logger.addHandler(db_handler)

    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor()

    #  농가 정보
    def get_table(std_dt):
        try:
            cur.execute(f"""select distinct * from geoai_mt.tb_calculation_environment_information where standard_date = '{std_dt}';""")
            df = cur.fetchall()
            ret = pd.DataFrame(df, dtype='str')

            if ret.empty:
                logger.error(f"{std_dt}의 데이터가 존재하지 않습니다.")

            ret.columns = [desc[0] for desc in cur.description]
            return ret

        except ValueError as e:
            logger.error(f'Error occurred: {e}')
            raise

    df = get_table(std_dt)

    df.loc[:, "radius_3km_farmland_area_rate":"radius_3km_migratorybird_habitat_area_rate"] = df.loc[:, "radius_3km_farmland_area_rate":"radius_3km_migratorybird_habitat_area_rate"].apply(pd.to_numeric,
                                                                                                    errors='coerce')

    mask = (df.loc[:, "radius_3km_farmland_area_rate":"radius_3km_migratorybird_habitat_area_rate"] > 1).any(axis=1)
    mask2 = (df.loc[:, "radius_3km_farmland_area_rate":"radius_3km_migratorybird_habitat_area_rate"] == 0).all(axis=1)

    if mask.any():
        error_message = f"1.0 초과의 ratio가 존재합니다. 확인이 필요합니다."
        logger.warning(error_message)  # 수정
        # raise ValueError(error_message)

    elif mask2.any():
        error_message = f"농가 주변환경의 비율이 모두 0인 농가가 존재합니다. 확인이 필요합니다."
        logger.warning(error_message)  # 수정
        # raise ValueError(error_message)

    else:
        logger.info(f"{std_dt}의 데이터가 성공적으로 로드되었습니다.")

    conn.close()


if __name__ == '__main__':
    std_dt = '2023-12-19'
    db_conn_str = 'postgresql://geoai:1234@10.10.12.84:15432/geoai'
    check_calculation_environment_information(std_dt, db_conn_str)
