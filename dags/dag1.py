import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2021, 9, 4)
}

with DAG('airflow_tutorial_v01', default_args=default_args) as dag:
    print_hello = BashOperator(task_id='print_hello',
        bash_command='echo hello')
    print_world = BashOperator(task_id='print_world',
        bash_command='echo world')

    print_hello >> print_world

