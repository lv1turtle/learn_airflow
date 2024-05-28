from airflow import DAG
# task decorator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json
from datetime import datetime
import requests
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_country_info(url):
    req = requests.get(url)

    # json으로 변환해서 dict형식으로 만듦
    json_data = req.json()
    # 대괄호가 2번 들어가는 이유
    # df[] 인덱싱에는 하나의 자료만 들어가야하기 때문에
    # 대괄호를 한번 더 사용함으로써 리스트형의 자료를 넣기 위함
    df = pd.DataFrame(json_data)[['name','area','population']]
    # NA 처리
    df = df.fillna("")
    # df['country'] = [x['official'] for x in df['name']]와 동일
    df['country'] = df['name'].apply(lambda x: x['official'])
    # ' 작은 따옴표를 SQL에서 처리할 수 있도록 \'으로 변환
    df['country'] = df['country'].str.replace('\'','\\\'')
    
    # dataframe을 list로 변환
    records = df[['country','population','area']].values.tolist()
    return records


def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        country text,
        population float,
        area float
    );""")


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        # 원본 테이블이 없으면 생성 - 테이블이 처음 한번 만들어질 때 필요한 코드
        _create_table(cur, schema, table, False)
        # 임시 테이블로 원본 테이블을 복사
        # CREATE TEMP TABLE은 AWS에서 제공하는 임시 테이블 기능
        cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};")
        for r in records:
            sql = f"INSERT INTO t VALUES ('{r[0]}', {r[1]}, {r[2]});"
            print(sql)
            cur.execute(sql)

        # 원본 테이블 생성
        _create_table(cur, schema, table, True)
        # 임시 테이블 내용을 원본 테이블로 복사
        cur.execute(f"INSERT INTO {schema}.{table} SELECT DISTINCT * FROM t;")
        cur.execute("COMMIT;") 
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;") 
        raise
    logging.info("load done")


with DAG(
    dag_id = 'UpdateCountryInfo',
    start_date = datetime(2024,5,20),
    catchup=False,
    tags=['API'],
    # 매주 토요일 오전 6시 30분에 실행
    schedule = '30 6 * * 6'
) as dag:
    url = "https://restcountries.com/v3.1/all"
    results = get_country_info(url)
    load("skqltldnjf77", "country_info", results)
