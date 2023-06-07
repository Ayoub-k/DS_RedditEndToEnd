"""This file for orchestration The ETL
"""
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datahub_provider.entities import Dataset
from src.common.utils.paths import Paths
from src.common.utils.config import Config
from src.common.utils.logger import logging
from dataclasses import dataclass


@dataclass
class ParamsPipeline:
    """For loading paths data pipeline
    """
    extract: str
    load : str
    transform : str


default_args = {
    'owner': 'Ayooub Qyoubi',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"),
    'retries': 0,
    # 'retry_delay': timedelta(minutes=30)
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for data processing',
    schedule_interval='0 0 * * 0',  # Run every week at midnight (Sunday)
    catchup=False
)

paths_files = ParamsPipeline(**Config.get_config_yml('path_etl_reddit'))
path_extract = Paths.get_file_path(paths_files.extract)

extract_data = BashOperator(
    task_id='Extract_reddit',
    bash_command=f'python {path_extract}',
    dag=dag,
    inlets=[Dataset("snowflake", "mydb.schema.tableD")],
    outlets=[Dataset("snowflake", "mydb.schema.tableD")]
)

path_transform = Paths.get_file_path(paths_files.transform)

transform_data = BashOperator(
    task_id='Transform_reddit',
    bash_command=f'python {path_transform}',
    dag=dag,
    inlets=[Dataset("snowflake", "mydb.schema.tableD")],
    outlets=[Dataset("snowflake", "mydb.schema.tableD")]
)

path_load = Paths.get_file_path(paths_files.load)

load_data = BashOperator(
    task_id='Load_reddit',
    bash_command=f'python {path_load}',
    dag=dag,
    inlets=[Dataset("snowflake", "mydb.schema.tableD")],
    outlets=[Dataset("snowflake", "mydb.schema.tableD")]
)

extract_data >> transform_data >> load_data
