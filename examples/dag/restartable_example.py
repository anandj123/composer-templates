import airflow
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id='restartable_example',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:

    def preprocess1(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            preprocess1 = airflow.operators.bash.BashOperator (
                            task_id = 'preprocess1',
                            bash_command = 'echo "Hello 1"',
                            trigger_rule='none_failed')
            preprocess1.execute(kwargs) 
        else:
            raise AirflowSkipException
   
    def preprocess2(**kwargs):
        if int(kwargs['current_task']) >= int(kwargs['start_task']):
            preprocess2 = airflow.operators.bash.BashOperator (
                            task_id = 'preprocess2',
                            bash_command = 'echo "Hello 2"',
                            trigger_rule='none_failed')
            preprocess2.execute(kwargs) 
        else:
            raise AirflowSkipException


    start_task = Variable.get('restartable_example_start_step',default_var='1')    
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