import uuid
import datetime
from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import DataprocCreatePysparkJobOperator

YC_SOURCE_BUCKET = 's3a://avs-bucket'
YC_TARGET_BUCKET = 's3a://avs-target'


with DAG(
        'DATA_INGEST',
        schedule_interval='@hourly',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        cluster_id='c9qii7enb1fkhj57ufc5',
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f'{YC_SOURCE_BUCKET}/job.py',
        args=[
            f'{YC_SOURCE_BUCKET}/2025/06/13',
            f'{YC_TARGET_BUCKET}/output_data',
        ],
    )

    poke_spark_processing
