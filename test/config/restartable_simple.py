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
    dag_id='restartable_simple',
    schedule_interval='0 0 0 * *',
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:

    def preprocess1(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            preprocess1 = BashOperator(
            task_id='preprocess1',
            trigger_rule='none_failed', 
            bash_command="echo 'Hello 1'")
            preprocess1.execute(dict())
        else:
            raise AirflowSkipException

    def preprocess2(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            preprocess2 = BashOperator(
            task_id='preprocess2',
            trigger_rule='none_failed', 
            bash_command="echo 'Hello 2'")
            preprocess2.execute(dict())
        else:
            raise AirflowSkipException


    start_task = Variable.get('restartable_simple_start_step',default_var='1')    
    start = DummyOperator(task_id='start')

    current_task = 1
    preprocess1 = PythonOperator(
                                task_id='preprocess1',
                                provide_context=True,
                                python_callable=preprocess1,
                                trigger_rule='none_failed',
                                op_kwargs={'current_task': current_task,'start_task': start_task}
                               )

    current_task += 1
    preprocess2 = PythonOperator(
                                task_id='preprocess2',
                                provide_context=True,
                                python_callable=preprocess2,
                                trigger_rule='none_failed',
                                op_kwargs={'current_task': current_task,'start_task': start_task}
                               )

    current_task += 1

    start >> preprocess1 >> preprocess2