import numpy as np
import pandas as pd
import psycopg2
import sqlalchemy
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine


def make_calculation_diseasecontrol_status_information(std_dt, db_conn_str):
    conn = psycopg2.connect(db_conn_str)
    cur = conn.cursor()
    standard_date = std_dt
    # standard_date = '2023-08-01'
    # 테이블 불러오기

    #  농가 정보
    def get_가금농장(standard_date):
        # present_breeding_livestock_count_average로 수정
        cur.execute(f"""select b.standard_date, livestock_species_class_code, present_breeding_livestock_count_average, 
                                a.farm_serial_no, farm_latitude, farm_longitude
                        from geoai_mt.tb_farm_information a
                        inner join geoai_mt.tb_livestock_species_information b
                        on a.farm_serial_no = b.farm_serial_no
                        and a.standard_date = b.standard_date
                        where a.farm_operation_status_code in ('1','2')
                        and b.livestock_species_class_code similar to '(415|416)%'
                        and b.livestock_species_operation_code in ('1','2')
                        AND a.standard_date = '{standard_date}';""")

        result = cur.fetchall()

        ret = pd.DataFrame(result, dtype='str')
        ret.columns = [desc[0] for desc in cur.description]
        return ret

    farms = get_가금농장(standard_date)

    # 방역카드 정보
    def get_방역카드(standard_date):
        # present_breeding_livestock_count_average로 수정
        cur.execute(f"""select *
                        from geoai_mt.tb_diseasecontrol_status_information
                        where standard_date = '{standard_date}';""")

        result = cur.fetchall()

        ret = pd.DataFrame(result, dtype='str')
        ret.columns = [desc[0] for desc in cur.description]

        ret = ret.drop(columns="standard_date")
        ret = ret.drop(columns="affiliate_name")
        ret = ret.replace('Y ', '1')
        ret = ret.replace('N ', '0')
        ret = ret.replace('Y', '1')
        ret = ret.replace('N', '0')

        return ret

    card = get_방역카드(standard_date)

    def get_process_farm(farms):
        # farms.present_breeding_livestock_count_average.fillna('0')
        farms.present_breeding_livestock_count_average = farms.present_breeding_livestock_count_average.astype(int)
        farms = farms.dropna(subset = 'farm_longitude', axis = 0) # 좌표 결측치 제거
        
        replace_dict = {
            '416509':'416900',
            '415010':'415005',
            '415011':'415005',
            '415012':'415005'}
        farms['livestock_species_class_code'] = farms['livestock_species_class_code'].replace(replace_dict)         

        df = farms.pivot_table(columns="livestock_species_class_code", index=["farm_serial_no", "standard_date", "farm_latitude", "farm_longitude"],
                               values="present_breeding_livestock_count_average").reset_index()
        df = df.fillna(0)
        df["present_breeding_livestock_count_average"] = df.iloc[:, 4:].sum(axis=1)
        df.iloc[:, 4:-1] = df.iloc[:, 4:-1].clip(upper=1).replace({0: '0', 1: '1'})
        df_merged = df.merge(card, how='left', on='farm_serial_no')
        df_merged = df_merged.dropna(subset=["farm_latitude"], axis=0)  # 좌표 없는 농가 삭제
        df_merged.loc[:, 'farm_latitude': 'etc_sale_yn'] = df_merged.loc[:, 'farm_latitude': 'etc_sale_yn'].astype('float')
        # df_merged = df_merged.replace(-1, np.nan)
        df_merged = df_merged.replace(np.nan, -1)

        return df_merged

    process_farm = get_process_farm(farms)

    data = process_farm[['415002', '415003', '415005', '415006', '415008', '416100',
                         '416210', '416230', '416240', '416300', '416400', '416500', '416900',
                         '416600', '416700', '415009', 'present_breeding_livestock_count_average', 'farm_latitude',
                         'farm_longitude']]

    # 정규화 진행
    scaler = MinMaxScaler()
    data_scale = scaler.fit_transform(data)
    # scaler.fit_transform

    # 시각화 후 지역별로 5개로 분화되는 k로 결정
    k = 25

    # 그룹 수, random_state 설정
    model = KMeans(n_clusters=k, random_state=42)

    # 정규화된 데이터에 학습
    model.fit(data_scale)

    # 클러스터링 결과 각 데이터가 몇 번째 그룹에 속하는지 저장
    process_farm['cluster'] = model.fit_predict(data_scale)

    # 클러스터링 별 최빈값으로 결측치 대체
    group_col = 'cluster'  # 그룹화할 컬럼 지정

    # 수정전
    value_cols = ['windowless_shape_breeding_yn',
                  'withwindows_shape_breeding_yn',
                  'polyhouse_shape_breeding_yn',
                  'crowded_complex_yn',
                  'nearby_habitat_yn',
                  'rent_farm_yn',
                  'other_industries_management_yn',
                  'fence_installation_yn',
                  'spray_disinfector_hold_yn',
                  'birdnetting_installation_yn',
                  'footboard_disinfectiontank_installation_yn',
                  'farm_disinfection_implementation_yn',
                  'killmouse_deworming_implementation_yn',
                  'sale_shipment_yn',
                  'slaughterhouse_shipment_yn',
                  'farm_sale_yn',
                  'merchant_sale_yn',
                  'etc_sale_yn']

    # 그룹별로 각 컬럼의 결측값을 제외한 최빈값을 계산

    def mode_without_minus_one(x):
        x_filtered = x[x != -1]  # -1을 제외한 값만 선택
        return x_filtered.mode().iloc[0]

    modes = process_farm.groupby(group_col)[value_cols].apply(mode_without_minus_one)

    # 각 컬럼의 결측치를 그룹별 최빈값으로 채움
    for col in value_cols:
        for group_value, row in modes.iterrows():
            mask = (process_farm[group_col] == group_value) & (process_farm[col] == -1)
            process_farm.loc[mask, col] = row[col]

    process_farm = process_farm.fillna(0) # 클러스터에 모든 값이 결측치인 경우
    process_farm_to_db = process_farm[['standard_date',
                                       'farm_serial_no',
                                       'windowless_shape_breeding_yn',
                                       'withwindows_shape_breeding_yn',
                                       'polyhouse_shape_breeding_yn',
                                       'rent_farm_yn',
                                       'other_industries_management_yn',
                                       'crowded_complex_yn',
                                       'nearby_habitat_yn',
                                       'fence_installation_yn',
                                       'spray_disinfector_hold_yn',
                                       'birdnetting_installation_yn',
                                       'footboard_disinfectiontank_installation_yn',
                                       'farm_disinfection_implementation_yn',
                                       'killmouse_deworming_implementation_yn',
                                       'sale_shipment_yn',
                                       'slaughterhouse_shipment_yn',
                                       'farm_sale_yn',
                                       'merchant_sale_yn',
                                       'etc_sale_yn']]

    coltype = {
        'standard_date': sqlalchemy.types.Date,
        'farm_serial_no': sqlalchemy.types.CHAR(8),
        'windowless_shape_breeding_yn': sqlalchemy.types.CHAR(1),
        'withwindows_shape_breeding_yn': sqlalchemy.types.CHAR(1),
        'polyhouse_shape_breeding_yn': sqlalchemy.types.CHAR(1),
        'rent_farm_yn': sqlalchemy.types.CHAR(1),
        'other_industries_management_yn': sqlalchemy.types.CHAR(1),
        'crowded_complex_yn': sqlalchemy.types.CHAR(1),
        'nearby_habitat_yn': sqlalchemy.types.CHAR(1),
        'fence_installation_yn': sqlalchemy.types.CHAR(1),
        'spray_disinfector_hold_yn': sqlalchemy.types.CHAR(1),
        'birdnetting_installation_yn': sqlalchemy.types.CHAR(1),
        'footboard_disinfectiontank_installation_yn': sqlalchemy.types.CHAR(1),
        'farm_disinfection_implementation_yn': sqlalchemy.types.CHAR(1),
        'killmouse_deworming_implementation_yn': sqlalchemy.types.CHAR(1),
        'sale_shipment_yn': sqlalchemy.types.CHAR(1),
        'slaughterhouse_shipment_yn': sqlalchemy.types.CHAR(1),
        'farm_sale_yn': sqlalchemy.types.CHAR(1),
        'merchant_sale_yn': sqlalchemy.types.CHAR(1),
        'etc_sale_yn': sqlalchemy.types.CHAR(1)
    }

    result = pd.concat([process_farm_to_db[["farm_serial_no", "standard_date"]],
                        process_farm_to_db.loc[:, 'windowless_shape_breeding_yn': 'etc_sale_yn'].astype('int').astype('str')],
                       axis=1)
    conn.close()

    with create_engine(db_conn_str).connect() as conn:
        conn.execute(f"delete from geoai_mt.tb_calculation_diseasecontrol_status_information where standard_date = '{standard_date}'")
        result.to_sql('tb_calculation_diseasecontrol_status_information', dtype=coltype, if_exists='append', con=conn, schema='geoai_mt', index=False)



if __name__ == '__main__':
    standard_date = '2024-08-21'
    db_conn_str = 'postgresql://geoai:1234@10.10.12.202:15432/geoai'
    make_calculation_diseasecontrol_status_information(standard_date, db_conn_str)
    # make_district_danger(sys.argv[1])
