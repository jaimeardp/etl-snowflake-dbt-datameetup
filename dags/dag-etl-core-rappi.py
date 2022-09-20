"""
A DAG that demonstrates use of the operators in this provider package.
"""

import os
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator 

from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator,
    )


#base_path = Path(__file__).parents[2]

base_path = "/usr/local/airflow/"

data_dir = os.path.join(base_path, "include", "data")

DBT_FOLDER = "dbt_config"


SNOWFLAKE_SAMPLE_TABLE = "GROUPS_TOPICS"
SNOWFLAKE_SCHEMA = "BHP_SCHEMA"
SNOWFLAKE_STAGE = "DEV"
S3_FILE_PATH = ['s3://snowflake-rappi-etl/data/groups_topics.csv']


create_table_ofstage = '''
    CREATE TABLE tmp_tstage AS 
    SELECT t.$1 from @stage_data_rappiV3
    (pattern=>'.*categories.*.csv', IGNORE_UTF8_ERRORS=>true) t
'''

INGEST_TYPE = "INCREMENTAL"

drop_table_ofstage = "drop table if exists %s"

def clean_column(column_name):
    return column_name.replace('.', '_')

def get_querysql(dir_files):

    output = []
    
    for path in os.listdir(dir_files):
        # check if current path is a file
        if os.path.isfile(os.path.join(dir_files, path)):
            files.append(path)

    for file in files:
        print(file)

        df = pd.read_csv(f"{dir_files}/{file}", nrows=10)

        columns_number = len(df.columns)

        columns_name = df.columns

        columns = [f"t.${number} as {clean_column(columns_name[number-1])}" for number in range(1, columns_number+1)]

        columns_ddl = ",".join(columns)

        file_name = file.split('.')[0]

        tquery = fcreate_table_ofstage % (file_name, columns_ddl, file_name)

        output.append( (file_name, tquery, ) )

    return output

def gen_operators_snow_drop(file):
    return SnowflakeOperator(
            task_id=f'snowflake_op_sql_str_{file}_drop',
            sql= drop_table_ofstage % (file, ),
            snowflake_conn_id="snowflake_nan",
            warehouse='BHP_DWH',
            database='BHP_DEMO',
            schema='RAPPI_SCHEMA',
            role='ACCOUNTADMIN',
        )

def gen_operators_snow_create(file, tquery):

    return SnowflakeOperator(
            task_id=f'snowflake_op_sql_str_{file}',
            sql=tquery,
            snowflake_conn_id="snowflake_nan",
            warehouse='BHP_DWH',
            database='BHP_DEMO',
            schema='RAPPI_SCHEMA',
            role='ACCOUNTADMIN',
        )

fcreate_table_ofstage = '''
    CREATE TABLE %s AS 
    SELECT %s from @stage_data_rappiV3
    (pattern=>'.*%s.*.csv') t
'''

with DAG(
        dag_id="etl-core-rappi",
        start_date=datetime(2021, 12, 15),
        catchup=False,
        schedule_interval=None
) as dag:


    files = []

    #./usr/local/airflow/dbt_config/custom_seeds/'

    dir_files = f"{base_path}{DBT_FOLDER}/custom_seeds/"

    dir_files = os.getcwdb()

    dir_files = f'dags/{DBT_FOLDER}/custom_seeds/'


    dbt_run_deltas = DbtRunOperator(
        task_id='dbt_run_deltas',
        dir=f'/usr/local/airflow/dags/{DBT_FOLDER}/',
        profiles_dir=f'/usr/local/airflow/dags/{DBT_FOLDER}/',
        select="members_deltas"

    )

    dbt_run_models = DbtRunOperator(
        task_id='dbt_run_models',
        dir=f'/usr/local/airflow/dags/{DBT_FOLDER}/',
        profiles_dir=f'/usr/local/airflow/dags/{DBT_FOLDER}/',
        select="members_usuarios_ciudad_aux1"

    )
    dbt_run_models2 = DbtRunOperator(
        task_id='dbt_run_models2',
        dir=f'/usr/local/airflow/dags/{DBT_FOLDER}/',
        profiles_dir=f'/usr/local/airflow/dags/{DBT_FOLDER}/',
        select="members_usuarios_ciudad_aux2"

    )
    dbt_run_models3 = DbtRunOperator(
        task_id='dbt_run_models3',
        dir=f'/usr/local/airflow/dags/{DBT_FOLDER}/',
        profiles_dir=f'/usr/local/airflow/dags/{DBT_FOLDER}/',
        select="members_usuarios_ciudad_aux3"

    )

    droped = DummyOperator(task_id="task1_error_handler_status_check")


    output_tasks = []

    output_tasks_create = []

    if INGEST_TYPE == 'HISTORICAL':

        for file, tquery in get_querysql(dir_files):
            #( gen_operators_snow_drop(file) >> gen_operators_snow_create(file, tquery) )

            output_tasks.append( gen_operators_snow_drop(file)  )

            output_tasks_create.append( gen_operators_snow_create(file, tquery) )

        output_tasks >> droped >> output_tasks_create >> dbt_run_models >> dbt_run_models2 >> dbt_run_models3

    elif INGEST_TYPE == 'INCREMENTAL':

        droped >> dbt_run_deltas >> dbt_run_models >> dbt_run_models2 >> dbt_run_models3