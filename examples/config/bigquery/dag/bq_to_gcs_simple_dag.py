import airflow
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

dag = DAG(
    dag_id='bq_to_gcs_simple_dag',
    schedule_interval='@hourly',
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:
    start = DummyOperator( task_id='start')

    bq_query_execute = airflow.contrib.operators.bigquery_operator.BigQueryOperator (
                            task_id = 'bq_query_execute',
                            sql = 'SELECT * FROM `composer-templates-shared.hmh_demo.covid` WHERE case_reported_date = "2021-08-18"',
                            use_legacy_sql = False,
                            write_disposition = 'WRITE_TRUNCATE',
                            allow_large_results = True,
                            destination_dataset_table = 'composer-templates-shared.hmh_demo.tmp_covid',
                            trigger_rule='none_failed')

    export_to_gcs = airflow.contrib.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator (
                            task_id = 'export_to_gcs',
                            source_project_dataset_table = 'composer-templates-shared.hmh_demo.tmp_covid',
                            destination_cloud_storage_uris = 'gs://hmh_demo/export_files/covid.csv',
                            export_format = 'CSV',
                            field_delimiter = ',',
                            print_header = True,
                            trigger_rule='none_failed')

    start >> bq_query_execute >> export_to_gcs