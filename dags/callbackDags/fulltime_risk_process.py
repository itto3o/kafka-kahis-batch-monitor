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
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import numpy as np
from scipy.fft import fft, fftfreq
from tqdm import tqdm
from shapely.geometry import Point
from shapely import wkb
import geopandas as gpd

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "fulltime_risk_process"
oracle_conn_id = "M2MSYS"
postgresql_conn_id = "bv_postgresql"

# C
def make_tb_farm_migratorybird_information(**context):
    std_dt = context["templates_dict"]["std_dt"]
    print(f"✅ 실행 기준일: {std_dt}")
    train_query = f"""
    WITH farm_first_infection as (
       SELECT  f.farm_serial_no,
               f.standard_date
       FROM    geoai_mt.tb_fulltime_farm_list   f
       WHERE   f.fulltime_yn = 'Y'
       AND   f.standard_date      = '{std_dt}'   -- ← 내가 넘기는 날짜들
    )
    SELECT t1.*, t2.xmin_, t2.ymin
    FROM farm_first_infection t1
    join
    (select distinct farms_no as farm_serial_no, xmin_, ymin from m2msys.nvrqs_mobile_farms where std_dt = '{std_dt}') t2
    on t1.farm_serial_no = t2.farm_serial_no
    """

    hook = PostgresHook(postgres_conn_id=postgresql_conn_id)
    with hook.get_conn() as conn:
        train_df = pd.read_sql_query(train_query, conn)
    train_df['standard_date'] = pd.to_datetime(train_df['standard_date'])
    


    ##########################################


    # 1) 농장 포인트 준비
    farm_points = (
       train_df[['standard_date', 'farm_serial_no', 'xmin_', 'ymin']]
       .sort_values(['standard_date', 'farm_serial_no'], ascending=False)
       .assign(std_dt=lambda d: d['standard_date'].astype(str).str[:7] + '-01')
       .drop_duplicates(['farm_serial_no', 'xmin_', 'ymin', 'std_dt'])
    )
    farm_points['geometry'] = farm_points.apply(lambda r: Point(r.xmin_, r.ymin), axis=1)
    farm_points = gpd.GeoDataFrame(farm_points, geometry='geometry', crs='4326').to_crs(5179)
    

    # 2) 철새 링 폴리곤 로드
   
    ring_geom_sql = """
    with latest as (
        select standard_date
        from geoai_polygon.tb_migratorybird_density_ring_geom
        order by standard_date desc
        limit 1
    )
    select distinct migratorybird_habitat_name as hbtt_nm,
        buffer,
        geom as geometry
    from geoai_polygon.tb_migratorybird_density_ring_geom g
    join latest l on g.standard_date = l.standard_date;
    """

    hook = PostgresHook(postgres_conn_id=postgresql_conn_id)
    with hook.get_conn() as conn:
        ring_geom_df = pd.read_sql_query(ring_geom_sql, con=conn)

    ring_geom_df['geometry'] = ring_geom_df['geometry'].apply(wkb.loads)
    ring_geom_df = gpd.GeoDataFrame(ring_geom_df, geometry='geometry', crs='5179')


    # 3) 농장 × 링 지오메트리 공간 매칭 (membership) -> 아직 철새 수 없음
    farm_ring_map = (
       farm_points[['standard_date', 'farm_serial_no', 'geometry']]
       .sjoin(ring_geom_df, how='inner')
       .reset_index(drop=True)
       .drop(columns=['geometry'])
    )
    farm_ring_map['std_dt'] = farm_ring_map['standard_date'].astype(str).str[:7] + '-01'
    farm_ring_map = farm_ring_map.drop_duplicates(['farm_serial_no', 'hbtt_nm', 'buffer', 'std_dt'])
    
    
    # 4) 철새 월별 지표 로드 및 집계
    bird_sql = """
    with latest_two as (
        select standard_date
        from geoai_polygon.tb_migratorybird_density_ring_geom
        group by standard_date
        order by standard_date desc
        limit 1
    )
    select 
        g.standard_date as std_dt,
        g.migratorybird_habitat_name as hbtt_nm,
        g.buffer,
        g.migratorybird_middle_class as middle_class,
        g.present_month_distance_ring_logic_value as distance_ring_logic_value,
        g.month1_before_distance_ring_logic_value as distance_ring_logic_value_lst
    from geoai_polygon.tb_migratorybird_density_ring_geom g
    join latest_two l 
    on g.standard_date = l.standard_date
    order by g.standard_date desc;
    """
    
    hook = PostgresHook(postgres_conn_id=postgresql_conn_id)
    with hook.get_conn() as conn:
        bird_df = pd.read_sql_query(bird_sql, con=conn)

    bird_df['std_dt'] = bird_df['std_dt'].astype(str)
    bird_df['std_dt'] = farm_ring_map['std_dt'].values[0]
    bird_grouped = (
       bird_df.groupby(['std_dt', 'hbtt_nm', 'buffer', 'middle_class'], as_index=False)
              .sum(numeric_only=True)
    )

    
    # 5) 농장 membership + 철새 지표 결합 -> 이제 철새 수 생김
    farm_bird_long = (
       farm_ring_map.merge(
           bird_grouped,
           on=['hbtt_nm', 'buffer', 'std_dt'],
           how='left'
       )
    )
    # 6) 넓은 테이블 피벗
    farm_bird_wide = (
       farm_bird_long.pivot_table(
           index=['std_dt', 'farm_serial_no'],
           columns='middle_class',
           values=[
               'distance_ring_logic_value',
               'distance_ring_logic_value_lst'
           ],
           aggfunc='sum',
           fill_value=0
       )
       .reset_index()
    )
    ## 컬럼명 변경, 현월 철새 = 철새명, 전월 철새 = 철새명_lst
    if isinstance(farm_bird_wide.columns, pd.MultiIndex):
        
        new_cols = []
        for top, sub in farm_bird_wide.columns:
            if sub == '':  # index columns from reset_index()
                new_cols.append(top)  # keep 'std_dt', 'farm_serial_no'
            else:
                new_cols.append(sub if top == 'distance_ring_logic_value' else f'{sub}_lst')
        farm_bird_wide.columns = new_cols
    else:
        farm_bird_wide.columns = list(farm_bird_wide.columns)
    
    print('farm_bird_wide.columns after:', farm_bird_wide.columns)

    # 7) 원본 train_df와 병합
    train_df['standard_date'] = train_df['standard_date'].astype(str)
    train_df['std_dt'] = train_df['standard_date'].str[:7] + '-01'


    train_with_bird = (
       train_df.merge(
           farm_bird_wide,
           on=['std_dt', 'farm_serial_no'],
           how='left'
       )
       .fillna(0)
    )

    print(train_with_bird.columns)
    # train_with_bird = train_with_bird.merge(
    #    ai_farms_list[['farm_serial_no', 'ai_occurrence_date', 'ai_occurrence_yn']],
    #    left_on=['farm_serial_no', 'standard_date'],
    #    right_on=['farm_serial_no', 'ai_occurrence_date'],
    #    how='left'
    # ).drop(columns=['ai_occurrence_date'])

    train_with_bird['ai_occurrence_yn'] = 0
    train_with_bird.sort_values(by=['farm_serial_no', 'standard_date'], inplace=True)


    # 9) 수치형 캐스팅
    exclude_cols = ['standard_date', 'std_dt', 'farm_serial_no', 'xmin_', 'ymin', 'ai_occurrence_yn']
    bird_cols = [c for c in train_with_bird.columns if c not in exclude_cols]
    train_with_bird[bird_cols] = train_with_bird[bird_cols].astype(float)


    #print('감염 여부 합계:', train_with_bird['ai_occurrence_yn'].sum())
    
    print(train_with_bird.head(5))
    print(train_with_bird.columns)
    train_with_bird = train_with_bird.drop(columns = 'std_dt')
    
    
    bird_columns = ['standard_date','farm_serial_no','xmin_','ymin','가마우지류','갈매기류','고니류','기러기류','기타물새류','기타산새류',
    '논병아리류','도요물떼새류','두루미류','따오기류','뜸부기류','맹금류','바다오리류','백로류','아비류','오리류','저어새류','황새류',
    '가마우지류_lst', '갈매기류_lst', '고니류_lst', '기러기류_lst', '기타물새류_lst', '기타산새류_lst', '논병아리류_lst', '도요물떼새류_lst',
                                      '두루미류_lst', '따오기류_lst', '뜸부기류_lst', '맹금류_lst', '바다오리류_lst', '백로류_lst', '아비류_lst',
                                      '오리류_lst', '저어새류_lst', '황새류_lst', 'ai_occurrence_yn']

    diff_cols = list(set(bird_columns) - set(train_with_bird.columns))
    for cols in diff_cols:
        train_with_bird[cols] = 0
    
    ##컬럼 정렬(확인)
    train_with_bird = train_with_bird[['standard_date','farm_serial_no','xmin_','ymin','가마우지류','갈매기류','고니류','기러기류','기타물새류','기타산새류',
    '논병아리류','도요물떼새류','두루미류','따오기류','뜸부기류','맹금류','바다오리류','백로류','아비류','오리류','저어새류','황새류',
    '가마우지류_lst', '갈매기류_lst', '고니류_lst', '기러기류_lst', '기타물새류_lst', '기타산새류_lst', '논병아리류_lst', '도요물떼새류_lst',
                                      '두루미류_lst', '따오기류_lst', '뜸부기류_lst', '맹금류_lst', '바다오리류_lst', '백로류_lst', '아비류_lst',
                                      '오리류_lst', '저어새류_lst', '황새류_lst', 'ai_occurrence_yn']]


    ##농장 - 새 - 현월수 - 전월수 형태의 key-value 형태로 변경
    id_vars = ['standard_date','farm_serial_no','xmin_','ymin','ai_occurrence_yn']
    value_vars_cur = ['가마우지류','갈매기류','고니류','기러기류','기타물새류','기타산새류',
                     '논병아리류','도요물떼새류','두루미류','따오기류','뜸부기류','맹금류',
                     '바다오리류','백로류','아비류','오리류','저어새류','황새류']
    value_vars_prev = [c+'_lst' for c in value_vars_cur]


    # 당월
    df_cur = train_with_bird.melt(
       id_vars=id_vars,
       value_vars=value_vars_cur,
       var_name="bird_name",
       value_name="value"
    )


    # 전월 (_lst)
    df_prev = train_with_bird.melt(
       id_vars=id_vars,
       value_vars=value_vars_prev,
       var_name="bird_name",
       value_name="value_lst"
    )
    df_prev['bird_name'] = df_prev['bird_name'].str.replace('_lst','',regex=False)


    # 병합
    long_df = pd.merge(
       df_cur, df_prev,
       on=id_vars + ['bird_name'],
       how='left'
    ).drop(columns = ['xmin_', 'ymin', 'ai_occurrence_yn'])


    long_df.columns = ['standard_date', 'farm_serial_no', 'migratorybird_name', 'present_month_migratorybird_count_by_middle_class', 'month1_before_migratorybird_count_by_middle_class']
    with hook.get_conn() as conn:
        with conn.cursor() as pg_cursor:
            partition_date = std_dt.replace('-','')
            print(partition_date)
            pg_cursor.execute(f"call geoai_mt.generate_std_dt_partition_table('geoai_mt', 'tb_farm_migratorybird_information', 'geoai_mt_partition', '{partition_date}', '{partition_date}');")
            print(partition_date)
            pg_cursor.execute(
                f"TRUNCATE TABLE geoai_mt_partition.tb_farm_migratorybird_information_{partition_date};"
            )

    print(long_df)
    sql_engine = hook.get_sqlalchemy_engine()
    long_df.to_sql(
        name='tb_farm_migratorybird_information',
        schema='geoai_mt',
        if_exists='append',
        con=sql_engine,
        index=False
    )
    ##################### END #####################


# F
def make_car_dominant_period_data(**context):
    # 1년간의 농장별 방문 시계열 데이터 불러오기
    std_dt = context["templates_dict"]["std_dt"]
    print(f"✅ 실행 기준일: {std_dt}")
    def get_farms_info(standard_date):
        query = f"""
            with fulltime as (
                select * from geoai_mt.tb_fulltime_farm_list
                where standard_date = '{standard_date}'
                and fulltime_yn = 'Y'
                ),
                visit_count as (
                select *
                from geoai_mt.tb_car_daily_visit_count
                where standard_date <= '{standard_date}'
                and standard_date > '{standard_date}'::date - interval '1 year'
                )
                select b.*
                from fulltime a
                left join visit_count b
                on a.farm_serial_no = b.farm_serial_no
        """
        hook = PostgresHook(postgres_conn_id=postgresql_conn_id)
        with hook.get_conn() as pg_connection:
            with pg_connection.cursor() as pg_cursor:
                pg_cursor.execute(query)
                rows = pg_cursor.fetchall()
                colnames = [desc[0] for desc in pg_cursor.description]
        df = pd.DataFrame(rows, columns = colnames)
        print(df)
        return df
    # 방문주기 데이터 불러오기
    def get_smallest_period(standard_date, visit_type):
        query = f'''
            select * from geoai_mt.tb_car_visit_period
            where visit_date <= '{standard_date}'
            and visit_date <= '{standard_date}'
            and visit_date > '{standard_date}'::date - interval '1 year'
            and car_visit_purpose_code ='{visit_type}'
        '''
        hook = PostgresHook(postgres_conn_id=postgresql_conn_id)
        with hook.get_conn() as pg_connection:
            with pg_connection.cursor() as pg_cursor:
                pg_cursor.execute(query)
                rows = pg_cursor.fetchall()
                colnames = [desc[0] for desc in pg_cursor.description]
        df = pd.DataFrame(rows, columns = colnames)
        smallest_period = df.groupby('farm_serial_no')['days_between_visits'].min().reset_index()
        return smallest_period
    # 1. 농장별 일별 방문 시계열 만들기
    def prepare_time_series(df, ref_date, visit_type = '01', farm_col='farm_serial_no', date_col='standard_date', value_col='car_visit_count'):
        ref_date = pd.to_datetime(ref_date)
        start_date = ref_date - pd.Timedelta(days=365)
        end_date = ref_date
        df[date_col] = pd.to_datetime(df[date_col])
        df_type = df[df['car_visit_purpose_code'] == visit_type]
        
        # IQR 기반 이상치 처리
        q3 = df_type.groupby(farm_col)[value_col].quantile(0.75)
        q1 = df_type.groupby(farm_col)[value_col].quantile(0.25)
        iqr = q3 - q1
        upper_bound = q3 + 1.5 * iqr
        upper_bound = upper_bound.reset_index().rename(columns={value_col: 'q3_plus_1.5iqr'})
        upper_bound['q3_plus_1.5iqr'] = np.round(upper_bound['q3_plus_1.5iqr'])
        df_merged = df_type.merge(upper_bound, on=farm_col, how='left')

        df_filtered = df_merged.copy()
        df_filtered['max_count'] = df_filtered.groupby(farm_col)[value_col].transform('max')

        # 최대치와 q3_plus_1.5iqr 가 4 이하인 것은 애초에 방문이 희소하여 적은 방문도 남겨두기
        df_filtered = df_filtered[
            ((df_filtered['q3_plus_1.5iqr'] <= 4) & (df_filtered['max_count'] <= 4) & (df_filtered[value_col] <= 2)) |
            (df_filtered[value_col] > 2)
        ]
        df_filtered[value_col] = df_filtered[[value_col, 'q3_plus_1.5iqr']].min(axis=1)

        # 방문일 3일 초과 농장만 남김
        df_flagged = df_filtered.copy()
        df_flagged['visit_flag'] = df_flagged.groupby(farm_col)[date_col].transform('nunique') > 3
        df_flagged = df_flagged[df_flagged['visit_flag']]
        valid_farms = df_flagged[farm_col].unique().tolist()
        df_final = df_type[df_type[farm_col].isin(valid_farms)]

        # 일 단위 방문수 재집계 (정제된 농장만)
        daily_df = df_final.groupby([farm_col, date_col])[value_col].sum().reset_index()

        # 날짜 템플릿 생성 및 결측 0 처리
        date_range = pd.date_range(start=start_date, end=end_date)
        all_farms = daily_df[farm_col].unique()
        index = pd.MultiIndex.from_product([all_farms, date_range], names=[farm_col, date_col])
        template = pd.DataFrame(index=index).reset_index()
        daily_df = pd.merge(template, daily_df, how='left', on=[farm_col, date_col])
        daily_df[value_col] = daily_df[value_col].fillna(0)

        # 농장 × 일자 pivot
        pivot = daily_df.pivot(index=farm_col, columns=date_col, values=value_col).fillna(0)
        pivot = pivot.sort_index(axis=1).reset_index()
        return pivot

    # 250909 함수 수정 (최대 진폭 찾을 때 농장별 최소 주기 -smallest_period- 보다 작은 것들은 제외)
    def extract_dominant_periods(pivot_df, smallest_period=None):
        """
        각 농장별로 가장 강한 반복 주기와 진폭, 임계값을 추출하는 함수.
        (pivot_df: index=농장ID, columns=날짜, 값=방문횟수)

        """
        pivot_df = pivot_df.sort_index()
        farms_no = pivot_df.farm_serial_no.tolist()
        pivot_df.drop(columns=['farm_serial_no'], inplace=True)
        dominant_periods = []
        dominant_amps = []
        thresholds = []
        N = pivot_df.shape[1]
        T = 1.0

        min_period_arr = pd.Series(14, index=farms_no).to_numpy()
        try:
            sp_series = (
                smallest_period
                .set_index('farm_serial_no')['days_between_visits']
                .reindex(farms_no)
                .astype(float)
            )
            min_period_arr = np.maximum(sp_series.fillna(14).to_numpy(), 14.0)
        except Exception:
            pass

        for i, (_, row) in tqdm(enumerate(pivot_df.iterrows()), total=pivot_df.shape[0]):
            yf = fft(row.values)
            xf = fftfreq(N, T)[:N//2]
            amp = 2.0/N * np.abs(yf[0:N//2])

            mean_amp = np.mean(amp)
            std_amp = np.std(amp)
            threshold = mean_amp + 2*std_amp

            periods = 1 / xf[1:]
            mask = (periods <= 365) & (periods >= float(min_period_arr[i]))
            valid_indices = np.where(mask)[0] + 1

            if len(valid_indices) > 0:
                peak_idx = valid_indices[np.argmax(amp[valid_indices])]
                dominant_freq = xf[peak_idx]
                dominant_amp = amp[peak_idx]
                dominant_period = 1 / dominant_freq
            else:
                dominant_period = np.nan
                dominant_amp = np.nan

            dominant_periods.append(dominant_period)
            dominant_amps.append(dominant_amp)
            thresholds.append(threshold)

        result = pd.DataFrame({
            'dominant_period_days': dominant_periods,
            'dominant_amp': dominant_amps,
            'threshold': thresholds
        }, index=pivot_df.index)

        result['farm_serial_no'] = farms_no
        return result
    
    def make_dominant_period(standard_date, db_conn_str):
        # 데이터 불러오기
        df = get_farms_info(standard_date)

        main_list = ['01', '05', '07']
        dominant_period_df = pd.DataFrame()


        for i in tqdm(main_list):
            df_i = df[df['car_visit_purpose_code'] == i]
            df_pivot_i = prepare_time_series(df_i, ref_date=standard_date, visit_type=i)
            sp_df_i = get_smallest_period(standard_date,visit_type=i)
            dominant_period_df_i = extract_dominant_periods(df_pivot_i, smallest_period=sp_df_i)
            dominant_period_df_i['car_visit_purpose_code'] = i
            dominant_period_df = pd.concat([dominant_period_df, dominant_period_df_i], axis=0)
            dominant_period_df['standard_date'] = standard_date
            dominant_period_df = dominant_period_df[['standard_date', 'farm_serial_no', 'car_visit_purpose_code','dominant_period_days','dominant_amp','threshold']]
        
        with PostgresHook(postgres_conn_id=postgresql_conn_id).get_conn() as pg_connection:
            pg_curl = pg_connection.cursor()
            partition_date = standard_date.replace('-','')
            pg_curl.execute(f"call geoai_mt.generate_std_dt_partition_table('geoai_mt', 'tb_car_dominant_visit_period', 'geoai_mt_partition', '{partition_date}', '{partition_date}');")
            delete_query = f"delete from geoai_mt.tb_car_dominant_visit_period where standard_date = '{standard_date}'"
            pg_curl.execute(delete_query)
            pg_connection.commit()
            
            sql_engine = PostgresHook(postgres_conn_id=postgresql_conn_id).get_sqlalchemy_engine()
            dominant_period_df.to_sql('tb_car_dominant_visit_period', if_exists='append', con=sql_engine, schema='geoai_mt', index=False)
            
    db_conn_str = PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()
    # 메인 함수 실행
    dominant_period_df = make_dominant_period(std_dt, db_conn_str)

    return f"done {std_dt}"

def make_tb_car_visit_pattern(**context):
    standard_date = context["templates_dict"]["std_dt"]
    print(f"✅ 실행 기준일: {standard_date}")
    def get_periods_info(standard_date):
        # 농장별 방문 메인주기 불러오기
        query = f"""
        select *
        from geoai_mt.tb_car_dominant_visit_period
        where standard_date = '{standard_date}';
        """
        hook = PostgresHook(postgres_conn_id=postgresql_conn_id)
        with hook.get_conn() as conn:
            df = pd.read_sql_query(query, con=conn)

        return df

    def get_visit_info(standard_date):
        # 농장별 방문 데이터 불러오기
        query = f"""
        with fulltime as (
                select * from geoai_mt.tb_fulltime_farm_list
                where standard_date = '{standard_date}'
                and fulltime_yn = 'Y'
                ),
                car_daily_visit_count as (
                select *
                from geoai_mt.tb_car_daily_visit_count
                where standard_date <= '{standard_date}'
                and standard_date > '{standard_date}'::date - interval '1 year'
                )
                select b.* from fulltime a
                left join car_daily_visit_count b
                on a.farm_serial_no = b.farm_serial_no
        """
        hook = PostgresHook(postgres_conn_id=postgresql_conn_id)
        with hook.get_conn() as conn:
            df = pd.read_sql_query(query, con=conn)

        return df

    def get_visit_period_info(standard_date):
        # 농장별 방문 간격 불러오기
        query = f"""
        with fulltime as (
                select * from geoai_mt.tb_fulltime_farm_list
                where standard_date = '{standard_date}'
                and fulltime_yn = 'Y'
                ),
                visit_period as (
                select *
                from geoai_mt.tb_car_visit_period
                where visit_date <= '{standard_date}'
                and visit_date > '{standard_date}'::date - interval '1 year'
                )
                select b.* from fulltime a
                left join visit_period b
                on a.farm_serial_no = b.farm_serial_no
        
        """
        hook = PostgresHook(postgres_conn_id=postgresql_conn_id)
        with hook.get_conn() as conn:
            df = pd.read_sql_query(query, con=conn)

        return df

    def create_visit_pattern_features(df_filtered, standard_date, df_visit_period):
        """농장별 방문 패턴 관련 변수들을 생성하는 함수 (1년기준)"""
        # 기본 그룹화 연산 최적화
        df_filtered.rename(columns = {'standard_date':'visit_date'}, inplace = True)
        grouped_farm_visit = df_filtered.groupby(['farm_serial_no', 'car_visit_purpose_code'])

        # 방문 횟수 계산 - agg 사용하여 한번에 계산
        visit_stats = grouped_farm_visit.agg(
            visit_count=('car_visit_count', 'sum'),
            first_visit=('visit_date', 'min'),
            last_visit=('visit_date', 'max'),
            visit_date_count=('visit_date', 'nunique')  # unique 날짜 수 계산
        ).reset_index()

        # 총 기간 계산
        # visit_stats['total_period'] = (visit_stats['last_visit'] - visit_stats['first_visit']).dt.days + 1
        farm_total_period = (df_filtered.groupby('farm_serial_no')['visit_date'].max() -
                        df_filtered.groupby('farm_serial_no')['visit_date'].min()).dt.days + 1
        farm_total_period = pd.DataFrame(farm_total_period).reset_index()
        farm_total_period.columns = ['farm_serial_no', 'total_period']

        df_visit_period['days_between_visits'] = df_visit_period['days_between_visits'].clip(upper = 365)

        # 방문 간격 통계 계산 - 한번의 groupby로 모든 통계 계산
        interval_stats = df_visit_period.groupby(['farm_serial_no', 'car_visit_purpose_code'])['days_between_visits'].agg([
            ('median_interval', 'median'),
            ('mean_interval', 'mean'),
            ('std_interval', 'std'),
            ('mode_interval', lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan)
        ]).reset_index()

        # 데이터프레임 병합
        visit_stats = visit_stats.merge(interval_stats, on=['farm_serial_no', 'car_visit_purpose_code'], how='left')
        visit_stats = visit_stats.merge(farm_total_period, on='farm_serial_no', how='left')

        # 파생변수 계산 - vectorized 연산 사용
        # visit_stats['monthly_visits'] = visit_stats['visit_date_count'] / (visit_stats['total_period'] / 30)
        # visit_stats['regularity_score'] = visit_stats['std_interval'] / visit_stats['mean_interval']
        visit_stats['visit_concentration'] = visit_stats['visit_count'] / visit_stats['visit_date_count']
        visit_stats['visit_per_day_count'] = visit_stats['visit_count'] / visit_stats['total_period']
        visit_stats['daily_visit_count'] = visit_stats['visit_date_count'] / visit_stats['total_period']


        # 피벗 테이블 생성
        visit_features = pd.pivot_table(
            visit_stats,
            index='farm_serial_no',
            columns='car_visit_purpose_code',
            values=[
                'visit_count', 'visit_date_count', 'visit_concentration',
                'median_interval', 'mean_interval', 'std_interval',
                'mode_interval', 'daily_visit_count', 'visit_per_day_count'
            ]
        )

        visit_features.columns = [f'{col[0]}_{col[1]}' for col in visit_features.columns]
        visit_features = visit_features.reset_index()

        # 전체 방문 통계 계산
        total_stats = df_visit_period.groupby('farm_serial_no')['days_between_visits'].agg([
            ('total_median_interval', 'median'),
            ('total_mean_interval', 'mean'),
            ('total_std_interval', 'std'),
            ('total_visits', 'count')
        ])

        # 방문 다양성과 총 방문 횟수 계산
        visit_diversity = df_filtered.groupby('farm_serial_no')['car_visit_purpose_code'].nunique()
        visit_total_count = (df_filtered.groupby('farm_serial_no').size() /
                            visit_stats.groupby('farm_serial_no')['total_period'].max()).reset_index(name='visit_total_count')

        # 최종 데이터프레임 병합
        final_features = (visit_features
                         .merge(total_stats, on='farm_serial_no', how='left')
                         .merge(pd.DataFrame({'visit_diversity': visit_diversity}), on='farm_serial_no', how='left')
                         .merge(visit_total_count, on='farm_serial_no', how='left'))

        # visit_yn 컬럼 생성 - vectorized 연산 사용
        visit_count_cols = [col for col in final_features.columns if col.startswith('visit_count_')]
        for col in visit_count_cols:
            yn_col = col.replace('visit_count_', 'visit_yn_')
            final_features[yn_col] = (final_features[col] >= 1).astype(int)

            # visit_ratio 계산
            ratio_col = col.replace('visit_count_', 'visit_ratio_')
            final_features[ratio_col] = final_features[col] / final_features[visit_count_cols].sum(axis=1)

        # 1일 주기 비율 계산 - 효율적인 그룹화 사용
        day_1_counts = df_visit_period[df_visit_period['days_between_visits'] == 1].groupby(['farm_serial_no', 'car_visit_purpose_code']).size()
        total_counts = df_visit_period.groupby(['farm_serial_no', 'car_visit_purpose_code']).size()
        ratio_1_day = (day_1_counts / total_counts).unstack(fill_value=0)
        ratio_1_day.columns = ['percent1_ratio_' + str(x) for x in ratio_1_day.columns]

        final_features = final_features.merge(ratio_1_day, on='farm_serial_no', how='left')

        # 차량 수 계산
        # vehicle_counts = df_filtered.groupby('farm_serial_no')['regist_no'].nunique().reset_index(name='total_visit_vehicle')
        # final_features = final_features.merge(vehicle_counts, on='farm_serial_no', how='left')

        return final_features

    def weekly_features(df_filtered, standard_date):    
        standard_date = pd.to_datetime(standard_date)
        df_filtered = df_filtered.rename(columns = {'standard_date':'visit_date'})
        ## 최근 방문일자 계산
        latest_visits = df_filtered.groupby(['farm_serial_no', 'car_visit_purpose_code'])['visit_date'].max().reset_index()
        latest_visits['days_since_last_visit'] = (standard_date - pd.to_datetime(latest_visits['visit_date'])).dt.days
        latest_visits['standard_date'] = standard_date

        latest_visits_pivot = latest_visits.pivot(
        index=['farm_serial_no', 'standard_date'],
        columns='car_visit_purpose_code',
        values='days_since_last_visit'
        )

        latest_visits_pivot.columns = ['days_since_last_' + x for x in latest_visits_pivot.columns]
        latest_visits_pivot.reset_index(inplace=True)

        ## 일별 방문 대수
        df_week = df_filtered[df_filtered['visit_date'] > standard_date - pd.Timedelta(days=7)]

        df_week_count = df_week.groupby(['car_visit_purpose_code','farm_serial_no']).size().reset_index()
        df_week_count = df_week_count.pivot(index='farm_serial_no', columns='car_visit_purpose_code', values=0)
        df_week_count.fillna(0, inplace=True)
        df_week_count.columns = [f'weekly_visit_count_{purpose}' for purpose in df_week_count.columns]

        df_week_count.reset_index(inplace=True)

        ## 방문 일수
        df_week_date_count = df_week.groupby(['car_visit_purpose_code','farm_serial_no'])["visit_date"].nunique().reset_index().pivot(index='farm_serial_no', columns='car_visit_purpose_code', values='visit_date')
        df_week_date_count.fillna(0, inplace=True)
        df_week_date_count.columns = [f'weekly_visit_date_count_{purpose}' for purpose in df_week_date_count.columns]
        df_week_date_count.reset_index(inplace=True)

        merged_df = (
            pd.merge(df_week_count, df_week_date_count, on=['farm_serial_no'], how='left')
            .merge(latest_visits_pivot, on=['farm_serial_no'], how='left')
        )
        return merged_df

    def short_pattern_features(df_week_res, visit_pattern_features):

        visit_count_cols = [col for col in df_week_res.columns if col.startswith('weekly_visit_count_')]
        for col in visit_count_cols:
            yn_col = col.replace('weekly_visit_count_', 'weekly_visit_yn_')
            df_week_res[yn_col] = (df_week_res[col] >= 1).astype(int)
        df_week_res['week_yn'] = 1
        merged_df_test = visit_pattern_features.merge(df_week_res, on=['farm_serial_no'], how='left')
        col_daily_visit_count = [col for col in merged_df_test.columns if 'daily_visit_count' in col]
        merged_df_test[col_daily_visit_count] = merged_df_test[col_daily_visit_count].fillna(0)

        # 각 목적별로 7일간 방문 확률 계산
        for i in range(1, 20):
            visit_yn_col = f'weekly_visit_yn_{i:02d}'
            daily_count_col = f'daily_visit_count_{i:02d}'
            if visit_yn_col in merged_df_test.columns and daily_count_col in merged_df_test.columns:
                merged_df_test[f'possibility_gap_{i:02d}'] = merged_df_test[visit_yn_col] - (1-(1-merged_df_test[daily_count_col])**7)


        # 각 목적별로 방문율 비교
        for i in range(1, 20):
            weekly_visit_count_col = f'weekly_visit_count_{i:02d}'
            daily_count_col = f'daily_visit_count_{i:02d}'
            if weekly_visit_count_col in merged_df_test.columns and daily_count_col in merged_df_test.columns:
                merged_df_test[f'daily_count_gap_{i:02d}'] = (merged_df_test[weekly_visit_count_col]/7)/merged_df_test[daily_count_col]


        daily_count_gap_cols = [col for col in merged_df_test.columns if 'daily_count_gap_' in col]
        possibility_gap_cols = [col for col in merged_df_test.columns if 'possibility_gap_' in col]

        merged_df_week = merged_df_test[['farm_serial_no'] + daily_count_gap_cols + possibility_gap_cols]
        merged_df_week[daily_count_gap_cols] = merged_df_week[daily_count_gap_cols].fillna(1)
        merged_df_week[possibility_gap_cols] = merged_df_week[possibility_gap_cols].fillna(0)


        ## 마지막 날짜
        # 변화율 지표: (현재 방문 주기 - 평균 방문 주기) / 평균 방문 주기
        # 이 값이 양수이고 커질수록 방문 주기가 길어졌다는 의미 (이탈 위험 증가).
        # 이 값이 음수이고 작아질수록 방문 주기가 짧아졌다는 의미 (충성도 증가 또는 특정 이벤트 영향).

        days_since_last_cols = [col for col in merged_df_test.columns if 'days_since_last_' in col]
        mean_interval_cols = [col for col in merged_df_test.columns if 'mean_interval_' in col]


        merged_df_test[mean_interval_cols] = merged_df_test[mean_interval_cols].fillna(365).clip(upper = 365)
        merged_df_test[days_since_last_cols] = merged_df_test[days_since_last_cols].fillna(365).clip(upper = 365)


        for i in range(1, 20):
            mean_interval_col = f'mean_interval_{i:02d}'
            days_since_last_col = f'days_since_last_{i:02d}'
            if mean_interval_col in merged_df_test.columns and days_since_last_col in merged_df_test.columns:
                merged_df_test[f'last_visit_gap_{i:02d}'] = (merged_df_test[days_since_last_col] -  merged_df_test[mean_interval_col])/merged_df_test[mean_interval_col]


        ## 방문 집중도 비교
        ## 단기 방문 집중도 정보 생성
        for i in range(1, 20):
            count_col = f'weekly_visit_count_{i:02d}'
            date_count_col = f'weekly_visit_date_count_{i:02d}'
            if count_col in merged_df_test.columns and date_count_col in merged_df_test.columns:
                merged_df_test[f'weekly_visit_concentration_{i:02d}'] = merged_df_test[count_col]/merged_df_test[date_count_col]


        for i in range(1, 20):
            weekly_visit_concentration_col = f'weekly_visit_concentration_{i:02d}'
            visit_concentration_col = f'visit_concentration_{i:02d}'
            if weekly_visit_concentration_col in merged_df_test.columns and visit_concentration_col in merged_df_test.columns:
                merged_df_test[f'weekly_visit_concentration_ratio_{i:02d}'] = merged_df_test[weekly_visit_concentration_col]/merged_df_test[visit_concentration_col]


        col_possibility_gap = [col for col in merged_df_test.columns if col.startswith('possibility_gap_')]
        col_last_visit_gap = [col for col in merged_df_test.columns if col.startswith('last_visit_gap_')]
        col_weekly_visit_concentration_ratio = [col for col in merged_df_test.columns if col.startswith('weekly_visit_concentration_ratio_')]

        merged_df_test_selected = merged_df_test[['farm_serial_no'] + col_possibility_gap + col_last_visit_gap + col_weekly_visit_concentration_ratio + ['week_yn']]
        merged_df_test_selected.loc[merged_df_test_selected['week_yn'] != 1, col_possibility_gap + col_last_visit_gap + col_weekly_visit_concentration_ratio] = np.nan

        return merged_df_test_selected

    # 1년간의 농장별 방문 메인주기 불러오기
    dominant_period_df = get_periods_info(standard_date)

    dominant_period_df_pivot = dominant_period_df.pivot(index=['farm_serial_no','standard_date'], columns='car_visit_purpose_code', values='dominant_period_days')

    dominant_period_df_pivot.columns = ['dominant_period_days_' + x for x in dominant_period_df_pivot.columns]
    dominant_period_df_pivot.reset_index(inplace=True)

    # 1년간의 농장별 방문 데이터 불러오기
    df_filtered = get_visit_info(standard_date)
    df_filtered['standard_date'] = pd.to_datetime(df_filtered['standard_date'])
    df_filtered = df_filtered[df_filtered['car_visit_purpose_code'].notnull()]

    # 1년간의 주기 불러오기
    df_visit_period = get_visit_period_info(standard_date)
    df_visit_period['visit_date'] = pd.to_datetime(df_visit_period['visit_date'])

    # 장기 변수 생성
    df_visit_pattern_features = create_visit_pattern_features(df_filtered, standard_date, df_visit_period)
    
    df_week_res = weekly_features(df_filtered, standard_date)
    
    short_pattern_features_df = short_pattern_features(df_week_res, df_visit_pattern_features)
    # 최종 데이터프레임 병합
    df = pd.merge(df_visit_pattern_features, short_pattern_features_df, on=['farm_serial_no'], how='left')
    df = pd.merge(df, dominant_period_df_pivot, on=['farm_serial_no'], how='left')
    df['standard_date'] = standard_date
    
    selected_ctgr = ['percent1_ratio','median_interval','visit_ratio','visit_concentration','dominant_period_days','daily_visit_count','possibility_gap']
    selected_ty = ['01','03','04','05','06','07','08','10','11','13','15','17','18']
    selected_cols = []
    for x in selected_ctgr:
        selected_cols.extend([col for col in df.columns if col.startswith(f'{x}_') and col.split('_')[-1] in selected_ty])
        
    selected_cols = ['farm_serial_no','standard_date','week_yn'] + selected_cols
    df = df[selected_cols]
    
    #with create_engine(db_conn_str).connect() as conn:
    #    df.to_sql('tb_car_visit_pattern', if_exists='append', con=conn, schema='geoai_mt', index=False)
    
    hook = PostgresHook(postgres_conn_id=postgresql_conn_id)
    with hook.get_conn() as conn:
        with conn.cursor() as pg_cursor:
            partition_date = standard_date.replace('-','')
            pg_cursor.execute(f"call geoai_mt.generate_std_dt_partition_table('geoai_mt', 'tb_car_visit_pattern', 'geoai_mt_partition', '{partition_date}', '{partition_date}');")
            pg_cursor.execute(f"delete from geoai_mt.tb_car_visit_pattern where standard_date = '{standard_date}'")
            
    sql_engine = hook.get_sqlalchemy_engine()
    df.to_sql('tb_car_visit_pattern', if_exists='append', con=sql_engine, schema='geoai_mt', index=False)

local_tz = pendulum.timezone("Asia/Seoul")
with DAG(
    dag_id=DAG_ID,
    description='전업농 위험도 생성 플로우',
    default_args={"on_failure_callback": on_task_failure},
    start_date=datetime.datetime(2023, 7, 24, tzinfo=local_tz),
    #schedule='0 7 * * *',
    schedule=None,
    catchup=False,
) as dag:
    CONF_STD = "{{ dag_run.conf.get('standard_date', (data_interval_end - macros.timedelta(days=1)).in_timezone('Asia/Seoul').format('YYYY-MM-DD')) }}"
    # 전업농 기본 필요 데이터
    # 수동 + 자동 작업이 필요하므로 A, I 는 별도의 dag으로 
    # B -> tb_farm_visit_info 을 가져와서 사용하는 것으로 대체
    # C -> 
    # D -> 4. 전체적으로는 일년에 한 번 업데이트 되는 것이 맞으나, 새로운 농장이 생긴다면 따로 데이터가 생성되는 일 배치 쿼리가 필요함 이 로직 구현해서 적용해야함
    # E -> tb_car_daily_visit_count
    # F -> tb_dominant_visit_period
    # G -> tb_livestock_species_information 을 사용하여 별도 작업 X
    # H -> 1 년 한번 업데이트라 별도 작업 X
    # ㄱ -> tb_last_visit_date
    # ㄴ -> tb_visit_period
    with TaskGroup(group_id='make_fulltime_risk_data') as make_fulltime_risk_data:
        # C
        make_tb_farm_migratorybird_information = PythonOperator(task_id='make_tb_farm_migratorybird_information',
                                                provide_context=True,
                                                python_callable=make_tb_farm_migratorybird_information,
                                                templates_dict={'std_dt': CONF_STD},
                                                dag=dag)
        
        # D
        year = '2023'
        season = '24-25'
        call_geoai_mt_sp_insert_tb_fulltime_farm_land_cover = PostgresOperator(task_id='call_geoai_mt_sp_insert_tb_fulltime_farm_land_cover',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_mt.sp_insert_tb_fulltime_farm_land_cover" + f"""('{CONF_STD}','2023','24-25' );""")

        # E 
        call_geoai_mt_sp_insert_tb_car_daily_visit_count = PostgresOperator(task_id='call_geoai_mt_sp_insert_tb_car_daily_visit_count',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_mt.sp_insert_tb_car_daily_visit_count" + f"""('{CONF_STD}');""")
        
        # ㄱ
        #call_geoai_mt_sp_update_tb_car_last_visit_date = PostgresOperator(task_id='call_geoai_mt_sp_update_tb_car_last_visit_date',
        #                                                                postgres_conn_id=postgresql_conn_id,
        #                                                                sql="call geoai_mt.sp_update_tb_car_last_visit_date" + f"""('{CONF_STD}');""")

        # ㄴ
        call_geoai_mt_sp_insert_tb_car_visit_period = PostgresOperator(task_id='call_geoai_mt_sp_insert_tb_car_visit_period',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_mt.sp_insert_tb_car_visit_period" + f"""('{CONF_STD}');""")
        
        call_geoai_mt_sp_insert_tb_farm_elevation = PostgresOperator(task_id='call_geoai_mt_sp_insert_tb_farm_elevation',
                                                                        postgres_conn_id=postgresql_conn_id,
                                                                        sql="call geoai_mt.sp_insert_tb_farm_elevation" + f"""('{CONF_STD}');""")
        
        # F
        #make_car_dominant_period_data = make_car_dominant_period_data('2025-09-16')
        make_car_dominant_period_data = PythonOperator(task_id='make_car_dominant_period_data',
                                                provide_context=True,
                                                python_callable=make_car_dominant_period_data,
                                                templates_dict={'std_dt': CONF_STD},
                                                dag=dag)
        
        
        make_tb_farm_migratorybird_information >> call_geoai_mt_sp_insert_tb_fulltime_farm_land_cover >> call_geoai_mt_sp_insert_tb_car_daily_visit_count >> call_geoai_mt_sp_insert_tb_car_visit_period >> make_car_dominant_period_data >> call_geoai_mt_sp_insert_tb_farm_elevation

        
    # 각 클러스터 학습 데이터
    # 철새 -> J, O, S
    # 환경 -> K, P, T
    # 전파 -> L, Q, U
    # 축종 -> M, R, V
    with TaskGroup(group_id='make_cluster_data') as make_cluster_data:
        #철새
        def build_payload():
            return {
                # 실행일(YYYY-MM-DD)
                "standard_date": CONF_STD,
                "db_conn_str": PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri(),
                "model_paths": {
                    "wisconsin_pkl": "./FulltimeFarmRisk/run_functions/cluster/wisconsin_colmax.pkl",
                    "ae_model": "./FulltimeFarmRisk/run_functions/cluster/ae_model_non_environ_non_sigmoid",
                    "fcm_centers": "./FulltimeFarmRisk/run_functions/cluster/fcm_centers.pkl",
                },
                "threshold": 0.2,
                "upload": {
                    "enable": True,
                    "schema": "geoai_monthly_report",
                    "table": "tb_cluster_migratorybird",
                    "chunksize": 5000,
                },
            }

        cluster_migratorybird = SimpleHttpOperator(
            task_id="call_api_migratorybird",
            http_conn_id="kahis_flask_fulltime_farm_risk",
            method="POST",
            endpoint="/migratorybird",
            data=json.dumps(build_payload()),   # ← 기존방식 유지
            headers={"Content-Type": "application/json"},
        )

        # 환경
        payload = {
            "standard_date": CONF_STD,
            "db_conn_str": PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri()
        }
        cluster_environment = SimpleHttpOperator(
                    task_id = "call_api_environment",
                    http_conn_id = "kahis_flask_fulltime_farm_risk",
                    method="POST",
                    endpoint="/environment",
                    data = json.dumps(payload),
                    headers={"Content-Type": "application/json"},
                    dag=dag)
        
        #전파
        make_tb_car_visit_pattern_task = PythonOperator(task_id='make_tb_car_visit_pattern',
                                                provide_context=True,
                                                python_callable=make_tb_car_visit_pattern,
                                                templates_dict={'std_dt': CONF_STD
                                                ,'db_conn_str' : PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri() },
                                                # op_kwargs={'standard_date': CONF_STD,
                                                #           'db_conn_str' : PostgresHook(postgres_conn_id=postgresql_conn_id).get_uri() },
                                                dag=dag)
        cluster_visit_pattern = SimpleHttpOperator(
                    task_id = "call_api_visit_pattern",
                    http_conn_id = "kahis_flask_fulltime_farm_risk",
                    method="POST",
                    endpoint="/visit_pattern",
                    data = json.dumps(payload),
                    headers={"Content-Type": "application/json"},
        dag=dag)

        # 축종
        cluster_diseasecontrol = PostgresOperator(
            task_id='sp_insert_tb_cluster_diseasecontrol',
            postgres_conn_id=postgresql_conn_id,
            sql="call geoai_monthly_report.sp_insert_tb_cluster_diseasecontrol" + f"""('{CONF_STD}');""")
        
        # 추가 데이터
        cluster_addition = PostgresOperator(
            task_id='sp_insert_tb_cluster_addition',
            postgres_conn_id=postgresql_conn_id,
            sql="call geoai_monthly_report.sp_insert_tb_cluster_addition" + f"""('{CONF_STD}');""")
        
        # 종합
        make_train_set = PostgresOperator(
            task_id='sp_insert_tb_fulltime_farm_risk_nonscaled_train',
            postgres_conn_id=postgresql_conn_id,
            sql="call geoai_monthly_report.sp_insert_tb_fulltime_farm_risk_nonscaled_train" + f"""('{CONF_STD}');""")

        #make_tb_car_visit_pattern_task >> cluster_visit_pattern >> [cluster_migratorybird, cluster_environment , cluster_diseasecontrol] >> make_train_set
        cluster_migratorybird >> cluster_environment >> make_tb_car_visit_pattern_task >> cluster_visit_pattern >> cluster_diseasecontrol >> cluster_addition >> make_train_set
        
    # 학습 데이터 모델 적용
    with TaskGroup(group_id='make_train_result_data') as make_train_result_data:
        call_api_predict = SimpleHttpOperator(
                    task_id = f"call_api_predict",
                    http_conn_id = "kahis_flask_fulltime_farm_risk",
                    method="POST",
                    endpoint="/predict",
                    data = json.dumps({
                        "standard_date" : CONF_STD,
                    }),
                    headers={"Content-Type": "application/json"},
                    dag=dag)
        
    
    execute_view_report_dag = TriggerDagRunOperator(
        task_id='trigger_region_risk',
        trigger_dag_id='region_risk_process',
        execution_date="{{ data_interval_end }}",
        conf={"standard_date": CONF_STD},
        reset_dag_run=True,
        wait_for_completion=False
    )
    #execute_view_report_dag = TriggerDagRunOperator(
    #    task_id="execute_view_report_dag",
    #    trigger_dag_id="make_view_report_process",
    #    execution_date="{{ ds }}",
    #    conf={"standard_date": CONF_STD},   
    #    trigger_run_id="view_report__{{ ds_nodash }}",
    #    reset_dag_run=True,
    #    wait_for_completion=False,
    #)   
    make_fulltime_risk_data >> make_cluster_data >> make_train_result_data >> execute_view_report_dag