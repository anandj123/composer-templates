import airflow
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

dag = DAG(
    dag_id='gcs_sensor_example',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:

    start = DummyOperator(task_id='start')
    gcs_sensor_step = airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor (
                        task_id = 'gcs_sensor_step',
                        bucket = 'anand-bq-test-2',
                        prefix = 'gcs-sensor/',
                        mode = 'poke',
                        trigger_rule='none_failed')
    print_file_names = airflow.operators.bash.BashOperator (
                        task_id = 'print_file_names',
                        bash_command = 'echo {{ ti.xcom_pull(task_ids="gcs_sensor_step") }}',
                        trigger_rule='none_failed')
    gcs_delete_archive_file = airflow.providers.google.cloud.operators.gcs.GCSDeleteObjectsOperator (
                        task_id = 'gcs_delete_archive_file',
                        bucket_name = 'anand-bq-test-2',
                        prefix = 'gcs-sensor/',
                        trigger_rule='none_failed')

    trigger_files_processor_dag_task = TriggerDagRunOperator(
        task_id='trigger_files_processor_dag',
        trigger_dag_id='gcs_sensor_example'
    )

    start >> gcs_sensor_step >> print_file_names >> gcs_delete_archive_file >> trigger_files_processor_dag_task