import airflow
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

dag = DAG(
    dag_id='restartable_dag_elt',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:

    def preprocess(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            preprocess = airflow.operators.bash.BashOperator (
                            task_id = 'preprocess',
                            bash_command = 'scripts/cmd.sh ',
                            trigger_rule='none_failed')
            preprocess.execute(kwargs) 
        else:
            raise AirflowSkipException

    def load_data(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            load_data = airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator (
                            task_id = 'load_data',
                            bucket = 'anand-bq-test-2',
                            source_objects = ['HCA_TEST/HCA_TEST_HCA_Test.csv'],
                            destination_project_dataset_table = 'anand-bq-test-2.Anand_BQ_Test_1.test3',
                            write_disposition = 'WRITE_TRUNCATE',
                            trigger_rule='none_failed')
            load_data.execute(kwargs) 
        else:
            raise AirflowSkipException

    def call_sp(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            call_sp = airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator (
                            task_id = 'call_sp',
                            configuration = {'query': {'query': "call Anand_BQ_Test_1.GetJobHash('a')", 'useLegacySql': False}},
                            trigger_rule='none_failed')
            call_sp.execute(kwargs) 
        else:
            raise AirflowSkipException


    start_task = Variable.get('restartable_dag_elt_start_step',default_var='1')    
    start = DummyOperator(task_id='start')

    current_task = 1
    preprocess = PythonOperator(
                                task_id='preprocess',
                                provide_context=True,
                                python_callable=preprocess,
                                trigger_rule='none_failed',
                                op_kwargs={'current_task': current_task,'start_task': start_task}
                               )
    current_task += 1
    load_data = PythonOperator(
                                task_id='load_data',
                                provide_context=True,
                                python_callable=load_data,
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

    start >> preprocess >> load_data >> call_sp