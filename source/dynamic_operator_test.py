import airflow
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id='testing_dag',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(0),
    catchup=False,
)
 
with dag: 
    start = airflow.operators.dummy_operator.DummyOperator(
        task_id='start',
        dag=dag
    )

    start 
