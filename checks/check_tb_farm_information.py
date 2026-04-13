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


def check_tb_farm_information(std_dt, db_conn_str):
    # 로깅 설정
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [tb_farm_information] - %(message)s')
    logger = logging.getLogger()
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - [tb_farm_information] - %(message)s')

    # 로그 파일 핸들러 설정
    file_handler = RotatingFileHandler('app_tb_farm_information.log', mode='a', maxBytes=100 * 1024 * 1024, backupCount=2)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    # 로그 DB 핸들러 설정
    db_handler = DatabaseHandler('tb_farm_information', db_conn_str)
    db_handler.setFormatter(log_format)
    logger.addHandler(db_handler)

    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor()

    #  농가 정보
    def get_table(std_dt):
        try:
            cur.execute(f"""select distinct * from geoai_mt.tb_farm_information where standard_date = '{std_dt}';""")
            df = cur.fetchall()
            ret = pd.DataFrame(df, dtype='str')

            if ret.empty:
                logger.error(f"{std_dt}의 농장정보가 존재하지 않습니다.")

            ret.columns = [desc[0] for desc in cur.description]
            return ret

        except ValueError as e:
            logger.error(f'Error occurred: {e}')
            raise

    df = get_table(std_dt)

    # 기준일부터의 한달동안의 count 평균 수 산출

    def get_count(std_dt):

        try:
            cur.execute(
                f"""select count(distinct standard_date), count(farm_serial_no) from geoai_mt.tb_farm_information where standard_date < to_date('{std_dt}') and standard_date >= to_date('{std_dt}') - interval '30 days';""")
            results = cur.fetchall()
            cnt_avg = (results[0][1]) / (results[0][0])

            return cnt_avg

        except ValueError as e:
            logger.error(f'Error occurred: {e}')
            raise

    cnt_avg = get_count(std_dt)

    # 좌표 정보 없는 농가 로그 기록
    loc_null_lst = []

    mask = (df.farm_operation_status_code.isin(['1', '2']))
    for index, row in df[mask].iterrows():
        if pd.isna(row['farm_latitude']) or pd.isna(row['farm_longitude']):
            loc_null_lst.append(row['farm_serial_no'])

    if len(loc_null_lst) != 0:
        # print(f"좌표 정보가 없는 농가가 존재합니다. : {loc_null_lst}")
        loc_null_lst_formatting = '[' + ', '.join([str(x) for x in loc_null_lst]) + ']'
        error_message = f"좌표 정보가 없는 농장이 존재합니다. {len(loc_null_lst)}개  농장 : {loc_null_lst_formatting}"
        logger.info(error_message)

    # PNU 체계가 변경되어 데이터에 반영되었을 시 오류 발생

    list_sido = ['47', '51', '41', '46', '48', '43', '44', '50', '52', '30', '31', '28', '36', '27', '11', '29', '26']
    # list_std_dt = df['farms_pnu'].dropna(axis=0).str[0:2].unique().tolist() #원천 수정 후 사용
    list_std_dt = (df[~df['farm_serial_no'].isin(['80004496', '80004495'])]['farm_pnu']
                   .dropna(axis=0).str[0:2]
                   .unique().tolist())  # 원천 수정까지 임시 코드

    for i in list_std_dt:
        if i not in list_sido:
            error_message = f"PNU 시도 코드가 표준 코드와 다른 농장이 존재합니다. 확인이 필요합니다. PNU : {i}"
            logger.error(error_message)
            raise ValueError(error_message)

    # 한달 간의 평균과 기준일의 농가수가 10% 이상 차이날 때, 오류 발생

    if abs(len(df) - cnt_avg) > (cnt_avg * 0.1):
        error_message = f"전날과 농장 정보의 갯수 차이가 오차범위 이상입니다. 확인이 필요합니다."
        logger.error(error_message)
        raise ValueError(error_message)

    else:
        logger.info(f"{std_dt}의 농장정보가 성공적으로 로드되었습니다.")

    conn.close()


if __name__ == '__main__':
    std_dt = '2023-12-19'
    db_conn_str = 'postgresql://geoai:1234@10.10.12.84:15432/geoai'
    check_tb_farm_information(std_dt, db_conn_str)
