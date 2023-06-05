"""This file for orchestration The ETL
"""
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datahub_provider.entities import Dataset, Urn

# reddit_data = File(url="s3://reddit-data")
# transformed_data = File(url="s3://transformed-data")
# redshift_table = Database(conn_id="redshift_conn_id", table="my_table")

default_args = {
    'owner': 'Ayooub Qyoubi',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for data processing',
    schedule_interval='0 0 * * 0',  # Run every week at midnight (Sunday)
    catchup=False
)


extract_data = BashOperator(
    task_id='step1',
    bash_command='python /home/ay/DataEng/projects/DS_RedditEndToEnd/etl/extract/extract.py',
    dag=dag,
    inlets=[Dataset("snowflake", "mydb.schema.tableD")],
    outlets=[Dataset("snowflake", "mydb.schema.tableD")]
)

transform_data = BashOperator(
    task_id='step2',
    bash_command='python /home/ay/DataEng/projects/DS_RedditEndToEnd/etl/transform/transform.py',
    dag=dag,
    inlets=[Dataset("snowflake", "mydb.schema.tableD")],
    outlets=[Dataset("snowflake", "mydb.schema.tableD")]
)

load_data = BashOperator(
    task_id='step3',
    bash_command='python /home/ay/DataEng/projects/DS_RedditEndToEnd/etl/load/load.py',
    dag=dag,
    inlets=[Dataset("snowflake", "mydb.schema.tableD")],
    outlets=[Dataset("snowflake", "mydb.schema.tableD")]
)

extract_data >> transform_data >> load_data
