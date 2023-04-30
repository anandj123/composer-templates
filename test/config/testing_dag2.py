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
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='testing_dag2',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:

    start_task = Variable.get('testing_dag2_start_step',default_var='1')    
    start = DummyOperator(task_id='start')

    current_task = 1
    dynamic1 = airflow.operators.dummy_operator.DummyOperator (
                                task_id = 'dynamic1',
                                trigger_rule='none_failed',
                               )


    current_task += 1

    start >> dynamic1