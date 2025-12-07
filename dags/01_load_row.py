from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,1,1),
    'retries': 1
}

with DAG(
    dag_id = '01_load_row',
    default_args = default_args,
    schedule_interval = None,
    catchup = False
) as dag:

#очистка
    task_trancute = SnowflakeOperator(
        task_id = 'task_trancute',
        snowflake_conn_id = 'snowflake_conn',
        sql = 'TRUNCATE TABLE AIRLINE_DWH.RAW_LAYER.STG_AIRLINE_RAW;'
    )
#загрузка
    task_load_csv = SnowflakeOperator(
        task_id = 'task_load_csv',
        snowflake_conn_id = 'snowflake_conn',
        sql = """
            COPY INTO AIRLINE_DWH.RAW_LAYER.STG_AIRLINE_RAW
            FROM @AIRLINE_DWH.RAW_LAYER.MY_CSV_STAGE
            FILE_FORMAT = (
                TYPE = 'CSV',
                FIELD_DELIMITER = ',',
                SKIP_HEADER = 1,
                FIELD_OPTIONALLY_ENCLOSED_BY = '"',
                TRIM_SPACE = TRUE
            )
            ON_ERROR = CONTINUE;
        """
    )

task_trancute >> task_load_csv