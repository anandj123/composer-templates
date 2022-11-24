from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id='my_dag2',
    schedule_interval='@once',
    start_date=datetime(2020, 1, 1)
)

with dag:
    dummy = DummyOperator(
        task_id='test',
        dag=dag
    )
    op_1 = PythonOperator(
        task_id='python_op_1',
        dag=dag
    )

    dummy  >> op_1