"""This file for orchestration The ETL
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import sys
sys.path.append('/home/ay/DataEng/projects/DS_RedditEndToEnd/etl/extract')

default_args = {
    'owner': 'Ayooub Qyoubi',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for data processing',
    schedule_interval='0 0 * * 0',  # Run every week at midnight (Sunday)
    catchup=False
)

def generate_lineage(context):
    """for generate data lineage for each steps in ETL

    Args:
        context (_type_): _description_
    """
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    execution_date = task_instance.execution_date

    lineage_info = f'Task ID: {task_id}, Execution Date: {execution_date}'
    task_instance.xcom_push(key='lineage_info', value=lineage_info)

# Define each step in the ETL process as a BashOperator executing the respective .py file

# step0 = BashOperator(
#     task_id='step0',
#     bash_command='export PYTHONPATH=/home/ay/DataEng/projects/DS_RedditEndToEnd/',
#     dag=dag
#     # provide_context=True,
#     # on_success_callback=generate_lineage
# )


step1 = BashOperator(
    task_id='step1',
    bash_command='python /home/ay/DataEng/projects/DS_RedditEndToEnd/etl/extract/extract.py',
    dag=dag
    # provide_context=True,
    # on_success_callback=generate_lineage
)

step2 = BashOperator(
    task_id='step2',
    bash_command='python /home/ay/DataEng/projects/DS_RedditEndToEnd/etl/transform/transform.py',
    dag=dag
    # provide_context=True,
    # on_success_callback=generate_lineage
)

step3 = BashOperator(
    task_id='step3',
    bash_command='python /home/ay/DataEng/projects/DS_RedditEndToEnd/etl/load/load.py',
    dag=dag
    # provide_context=True,
    # on_success_callback=generate_lineage
)

# step4 = BashOperator(
#     task_id='step4',
#     bash_command='python /path/to/step4.py',
#     dag=dag,
#     provide_context=True,
#     on_success_callback=generate_lineage
# )

# Set task dependencies

# step0 >> 
step1 >> step2 >> step3
