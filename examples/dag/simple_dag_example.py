import airflow
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.bash import BashOperator
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id='simple_dag_example',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:
    start = DummyOperator( task_id='start')

    bash_task_1 = airflow.operators.bash.BashOperator (
                            task_id = 'bash_task_1',
                            bash_command = 'echo "hello 1"',
                            trigger_rule='none_failed')

    bash_task_2 = airflow.operators.bash.BashOperator (
                            task_id = 'bash_task_2',
                            bash_command = 'echo "hello 2"',
                            trigger_rule='none_failed')

    bash_task_3 = airflow.operators.bash.BashOperator (
                            task_id = 'bash_task_3',
                            bash_command = 'echo "hello 4"',
                            trigger_rule='none_failed')

    start >> bash_task_1 >> bash_task_2 >> bash_task_3