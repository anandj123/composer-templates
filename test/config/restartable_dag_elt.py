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
    schedule_interval='0 0 0 * *',
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:

    def preprocess(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            preprocess = BashOperator(
            task_id='preprocess',
            trigger_rule='none_failed', 
            bash_command="./scripts/cmd.sh")
            preprocess.execute(dict())
        else:
            raise AirflowSkipException

    def load_data(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            load_data = GoogleCloudStorageToBigQueryOperator(
            task_id='load_data',
            trigger_rule='none_failed', 
            bucket='anand-bq-test-2',
            source_objects=['HCA_TEST/HCA_TEST_HCA_Test.csv'],
            destination_project_dataset_table='anand-bq-test-2.Anand_BQ_Test_1.test3',
            write_disposition='WRITE_TRUNCATE')
            load_data.execute(dict()) 
        else:
            raise AirflowSkipException

    def call_sp(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            call_sp = BigQueryInsertJobOperator(
            task_id='call_sp',
            trigger_rule='none_failed', 
            configuration={"query": {"query": "call Anand_BQ_Test_1.GetJobHash('a')", "useLegacySql": False}}
                            )
            call_sp.execute(dict()) 
        else:
            raise AirflowSkipException

    def call_sp2(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            call_sp2 = BigQueryInsertJobOperator(
            task_id='call_sp2',
            trigger_rule='none_failed', 
            configuration={"query": {
                           "query": "call Anand_BQ_Test_1.GetJobHash('b')",
                            "useLegacySql": False,
                           }})
            call_sp2.execute(dict()) 
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
    call_sp2 = PythonOperator(
                                task_id='call_sp2',
                                provide_context=True,
                                python_callable=call_sp2,
                                trigger_rule='none_failed',
                                op_kwargs={'current_task': current_task,'start_task': start_task}
                               )

    current_task += 1

    start >> preprocess >> load_data >> call_sp >> call_sp2