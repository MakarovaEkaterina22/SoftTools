"""Variant 17: Titanic labelled training sample. One row is one passenger."""
import csv
import hashlib
import io
import json
import logging
from decimal import Decimal, InvalidOperation
from pathlib import Path
import urllib.request
import urllib.error
import zipfile
from datetime import datetime,timedelta
from psycopg2.extras import execute_values,RealDictCursor
from airflow.decorators import dag,task
from airflow.providers.postgres.hooks.postgres import PostgresHook

ROOT=Path('/opt/airflow/dags')
DATA=ROOT/'data'
KAGGLE_URL='https://api.kaggle.com/v1/competitions/data/download/titanic/train.csv'
MIRROR_URL='https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv'
CONNECTION_ID='analytics_postgres'
REQUIRED={'PassengerId','Survived','Pclass','Sex','Age'}

def connection():
    return PostgresHook(postgres_conn_id=CONNECTION_ID).get_conn()

def parse_csv(content):
    rows=list(csv.DictReader(io.StringIO(content.decode('utf-8-sig'))))
    if not rows or not REQUIRED.issubset(rows[0]): raise ValueError('Unexpected labelled Titanic data structure')
    return rows

def source_rows():
    return parse_csv((DATA/'train.csv').read_bytes())

@dag(dag_id='titanic_analysis_variant_17',start_date=datetime(2024,1,1),schedule=None,
     catchup=False,max_active_runs=1,default_args={'retries':2,'retry_delay':timedelta(seconds=30)},
     tags=['variant_17','Titanic'])
def pipeline():
    @task
    def extract():
        DATA.mkdir(exist_ok=True)
        used_url=KAGGLE_URL; fallback_reason=None
        try:
            with urllib.request.urlopen(KAGGLE_URL,timeout=45) as response: content=response.read()
            if content[:2]==b'PK':
                with zipfile.ZipFile(io.BytesIO(content)) as archive: content=archive.read('train.csv')
            rows=parse_csv(content)
        except (urllib.error.URLError,ValueError,UnicodeError,zipfile.BadZipFile,KeyError) as error:
            fallback_reason=str(error)
            logging.warning('Kaggle source unavailable: %s. Using documented Data Science Dojo copy.',error)
            used_url=MIRROR_URL
            with urllib.request.urlopen(MIRROR_URL,timeout=90) as response: content=response.read()
            rows=parse_csv(content)
        (DATA/'train.csv').write_bytes(content)
        manifest={'source_url':used_url,'original_source':'https://www.kaggle.com/competitions/titanic/data',
                  'fallback_reason':fallback_reason,'sha256':hashlib.sha256(content).hexdigest(),'source_rows':len(rows)}
        (DATA/'source_manifest.json').write_text(json.dumps(manifest,indent=2))
        logging.info('Source manifest: %s',manifest)
        return len(rows)

    @task
    def load_raw(expected):
        ok,message=PostgresHook(postgres_conn_id=CONNECTION_ID).test_connection()
        logging.info('Connection Test: %s %s',ok,message)
        if not ok: raise ConnectionError(message)
        rows=source_rows()
        if len(rows)!=expected: raise ValueError('Source count changed')
        cols=', '.join('"'+f+'" TEXT' for f in rows[0])
        with connection() as conn,conn.cursor() as cur:
            cur.execute('DROP VIEW IF EXISTS titanic_datamart; DROP TABLE IF EXISTS stg_titanic; DROP TABLE IF EXISTS raw_titanic;')
            cur.execute('CREATE TABLE raw_titanic(row_id INTEGER PRIMARY KEY, '+cols+')')
            execute_values(cur,'INSERT INTO raw_titanic VALUES %s',[(i,*r.values()) for i,r in enumerate(rows,1)])
        return expected

    @task
    def transform(expected):
        with connection() as conn,conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('SELECT row_id,"PassengerId","Pclass","Sex","Age","Survived" FROM raw_titanic ORDER BY row_id')
            rows=cur.fetchall()
        if len(rows)!=expected: raise ValueError('RAW count mismatch')
        valid=[];rejected=[];seen=set();missing_age=0
        for r in rows:
            try:
                pid=int(r['PassengerId']);p_class=int(r['Pclass']);survived=int(r['Survived'])
                sex=' '.join(r['Sex'].split()).casefold()
                age_text=r['Age'].strip();age=Decimal(age_text) if age_text else None
                if pid<=0 or p_class not in (1,2,3) or survived not in (0,1) or sex not in ('male','female'): raise ValueError('Invalid id/class/sex/survival')
                if age is not None and (not age.is_finite() or not 0<=age<=120): raise ValueError('Invalid age')
                if pid in seen: raise ValueError('Duplicate passenger id')
                valid.append((r['row_id'],pid,p_class,sex,age,survived));seen.add(pid)
                missing_age+=age is None
            except (ValueError,InvalidOperation,KeyError) as error:
                rejected.append({'row_id':r['row_id'],'passenger_id':r.get('PassengerId'),'reason':str(error)})
        with connection() as conn,conn.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS stg_titanic CASCADE')
            cur.execute('CREATE TABLE stg_titanic(row_id INTEGER PRIMARY KEY REFERENCES raw_titanic(row_id),passenger_id INTEGER UNIQUE NOT NULL,p_class INTEGER NOT NULL,sex TEXT NOT NULL,age NUMERIC(6,2),survived INTEGER NOT NULL)')
            cur.execute('CREATE TABLE IF NOT EXISTS etl_audit(loaded_at TIMESTAMPTZ DEFAULT now(),source_count INTEGER,accepted_count INTEGER,rejected_count INTEGER,missing_age_count INTEGER,rejected_rows JSONB,source_manifest JSONB)')
            if valid: execute_values(cur,'INSERT INTO stg_titanic VALUES %s',valid)
            cur.execute('INSERT INTO etl_audit(source_count,accepted_count,rejected_count,missing_age_count,rejected_rows,source_manifest) VALUES (%s,%s,%s,%s,%s,%s)',(expected,len(valid),len(rejected),missing_age,json.dumps(rejected),(DATA/'source_manifest.json').read_text()))
        logging.info('RAW=%s STG=%s rejected=%s missing_age=%s reasons=%s',expected,len(valid),len(rejected),missing_age,rejected)
        if not valid or len(valid)+len(rejected)!=expected: raise ValueError('Reconciliation failed')
        return len(valid)

    @task
    def create_datamart(expected):
        with connection() as conn,conn.cursor() as cur:
            cur.execute((ROOT/'datamart_variant_17.sql').read_text())
            cur.execute('SELECT count(*),sum(survived),100.0*avg(survived),count(*) FILTER(WHERE age IS NULL) FROM titanic_datamart')
            values=cur.fetchone()
            if values[0]!=expected: raise ValueError('Datamart reconciliation failed')
            logging.info('Datamart: %s',values)
    create_datamart(transform(load_raw(extract())))
pipeline()
