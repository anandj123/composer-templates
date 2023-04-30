import airflow
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
import logging as log

dag = DAG(
    dag_id='gcs_sensor_dag',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:

    start = DummyOperator(task_id='start')

    gcs_sensor = GCSObjectsWithPrefixExistenceSensor(
                bucket='anand-bq-test-2',
                prefix='gcs-sensor/',
                mode='poke',
                task_id='gcs_sensor',
                trigger_rule='none_failed', 
            )
    bash_xcom = BashOperator(
                task_id="bash_push",
                bash_command='echo {{ ti.xcom_pull(task_ids="gcs_sensor") }} ',
                trigger_rule='none_failed', 
    )
    gcs_remove = GCSDeleteObjectsOperator(
                bucket_name='anand-bq-test-2',
                prefix='gcs-sensor/',
                task_id='gcs_remove',
                trigger_rule='none_failed', 
                
    )
    trigger_files_processor_dag_task = TriggerDagRunOperator(
        task_id='trigger_files_processor_dag',
        trigger_dag_id='gcs_sensor_dag'
    )
    start >> gcs_sensor >> bash_xcom >> gcs_remove >> trigger_files_processor_dag_task