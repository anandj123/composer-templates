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


dag = DAG(
    dag_id='restartable_dag_elt',
    schedule_interval='None',
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:

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


    start_task = Variable.get('restartable_dag_elt_start_step',default_var='1')    
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