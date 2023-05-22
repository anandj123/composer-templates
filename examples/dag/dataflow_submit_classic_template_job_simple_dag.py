import airflow
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator

dag = DAG(
    dag_id='dataflow_submit_classic_template_job_simple_dag',
    schedule_interval='@once',
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:
    start = DummyOperator( task_id='start')

    dataflow_submit_classic_template_job = airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator (
                            task_id = 'dataflow_submit_classic_template_job',
                            project_id = 'project-id',
                            location = 'us-central1',
                            job_name = 'bigquery_to_cloud_storage',
                            append_job_name = True,
                            template = 'gs://dataflow-templates/latest/Cloud_BigQuery_to_GCS_TensorFlow_Records',
                            options = {'serviceAccountEmail': '1234-compute@developer.gserviceaccount.com', 'tempLocation': 'gs://bucket/temp', 'ipConfiguration': 'WORKER_IP_PRIVATE'},
                            parameters = {'readQuery': 'select * from project_id.dataset.table', 'outputDirectory': 'gs://bucket/output'},
                            poll_sleep = 200,
                            cancel_timeout = 200,
                            wait_until_finished = True,
                            trigger_rule='none_failed')

    start >> dataflow_submit_classic_template_job