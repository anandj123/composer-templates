import airflow
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocListBatchesOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteBatchOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

dag = DAG(
    dag_id='dataproc_submit_pyspark_batch_job_simple_dag',
    schedule_interval='@once',
    start_date=airflow.utils.dates.days_ago(0)
)


with dag:
    start = DummyOperator( task_id='start')

    list_dataproc_batches = airflow.providers.google.cloud.operators.dataproc.DataprocListBatchesOperator (
                            task_id = 'list_dataproc_batches',
                            project_id = 'devansh-365318',
                            region = 'us-central1',
                            trigger_rule='none_failed')

    delete_dataproc_batch = airflow.providers.google.cloud.operators.dataproc.DataprocDeleteBatchOperator (
                            task_id = 'delete_dataproc_batch',
                            batch_id = 'batch-composer-simple-dag-test',
                            region = 'us-central1',
                            project_id = 'devansh-365318',
                            trigger_rule='none_failed')

    dataproc_submit_pyspark_batch_job = airflow.providers.google.cloud.operators.dataproc.DataprocCreateBatchOperator (
                            task_id = 'dataproc_submit_pyspark_batch_job',
                            project_id = 'devansh-365318',
                            region = 'us-central1',
                            batch = {'pyspark_batch': {'main_python_file_uri': 'gs://composer-temp-dataproc/pyspark_scripts/demo_pyspark_script.py', 'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar']}},
                            batch_id = 'batch-composer-simple-dag-test',
                            trigger_rule='none_failed')

    start >> list_dataproc_batches >> delete_dataproc_batch >> dataproc_submit_pyspark_batch_job
