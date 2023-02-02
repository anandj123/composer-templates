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

dag = DAG(
    dag_id='simple_push_elt',
    schedule_interval='None',
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:

    def gcs_sensor(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            GCSObjectsWithPrefixExistenceSensor(
                bucket='anand-bq-test-2',
                prefix='HCA_TEST/',
                mode='poke',
                task_id='gcs_sensor',
                trigger_rule='none_failed', 
            )
        else:
            raise AirflowSkipException

    def call_sp(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            BigQueryInsertJobOperator(
            task_id='call_sp',
            trigger_rule='none_failed', 
            configuration={"query": {
                           "query": "call Anand_BQ_Test_1.GetJobHash('a')",
                            "useLegacySql": False,
                           }})
        else:
            raise AirflowSkipException


    start_task = Variable.get('simple_push_elt_start_step',default_var='1')    
    start = DummyOperator(task_id='start')

    current_task = 1
    gcs_sensor = PythonOperator(
                                task_id='gcs_sensor',
                                provide_context=True,
                                python_callable=gcs_sensor,
                                trigger_rule='none_failed',
                                op_kwargs={'current_task': current_task,'start_task': start_task}
                               )

    current_task += 1
    call_sp = PythonOperator(
                                task_id='call_sp',
                                provide_context=True,
                                python_callable=call_sp,
                                trigger_rule='none_failed',
                                op_kwargs={'current_task': current_task,'start_task': start_task}
                               )

    current_task += 1

    start >> gcs_sensor >> call_sp