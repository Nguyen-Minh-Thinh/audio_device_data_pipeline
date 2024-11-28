from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='audio_device_dag',
    schedule_interval='@daily',
    start_date=days_ago(1)     # Set the start date to one day before the DAG execution date    
):
    crawl_data = BashOperator(
        task_id='crawl_data',
        bash_command='python /opt/airflow/tasks/crawl_data/crawl_data.py'
    )

    upload_bronze_level_to_s3 = BashOperator(
        task_id='upload_bronze_level_to_s3',
        bash_command='python /opt/airflow/tasks/work_with_s3/upload_to_s3.py bronze'    # Pass 'bronze' argument
    )
    clean_bronze_level_data = BashOperator(
        task_id='clean_bronze_level_data',
        bash_command='python /opt/airflow/clean_bronze_level_data.py'
    )
    upload_silver_level_to_s3 = BashOperator(
        task_id='upload_silver_level_to_s3',
        bash_command='python /opt/airflow/tasks/work_with_s3/upload_to_s3.py silver'
    )
    upload_silver_level_to_mysql = BashOperator(
        task_id='upload_silver_level_to_mysql',
        bash_command='python /opt/airflow/tasks/work_with_mysql/load_data_to_mysql.py'
    )
    upload_silver_level_to_clickhouse = BashOperator(
        task_id='upload_silver_level_to_clickhouse',
        bash_command='python /opt/airflow/tasks/work_with_clickhouse/upload_data_to_clickhouse.py'
    )
crawl_data >> upload_bronze_level_to_s3 >> clean_bronze_level_data >> upload_silver_level_to_s3 >> [upload_silver_level_to_clickhouse, upload_silver_level_to_mysql]
