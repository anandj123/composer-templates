import airflow
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id='simple_push_elt',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(0),
    cathup=False
)

with dag:
    start = DummyOperator(
        task_id='start',
        dag=dag
    )
    bash1 = BashOperator(
         task_id='bash1',
         bash_command='echo "hello 1"')
    bash2 = BashOperator(
         task_id='bash2',
         bash_command='echo "hello 2"')

    start >> bash1 >> bash2