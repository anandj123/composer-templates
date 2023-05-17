import airflow
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_sftp import GCSToSFTPOperator

dag = DAG(
    dag_id='bq_to_sftp_simple_dag',
    schedule_interval='@hourly',
    start_date=airflow.utils.dates.days_ago(0)
)

with dag:
    start = DummyOperator( task_id='start')

    bq_query_execute = airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator (
                            task_id = 'bq_query_execute',
                            sql = 'SELECT * FROM `composer-templates-dev.hmh_demo.covid` WHERE case_reported_date = "2021-08-18"',
                            use_legacy_sql = False,
                            write_disposition = 'WRITE_TRUNCATE',
                            allow_large_results = True,
                            destination_dataset_table = 'composer-templates-dev.hmh_demo.tmp_covid',
                            trigger_rule='none_failed')

    export_to_gcs = airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryToGCSOperator (
                            task_id = 'export_to_gcs',
                            source_project_dataset_table = 'composer-templates-dev.hmh_demo.tmp_covid',
                            destination_cloud_storage_uris = 'gs://hmh_composer_demo/export_files/covid.csv',
                            export_format = 'CSV',
                            field_delimiter = ',',
                            print_header = True,
                            trigger_rule='none_failed')

    file_move_gsc_to_sftp = airflow.providers.google.cloud.transfers.gcs_to_sftp.GCSToSFTPOperator (
                            task_id = 'file_move_gsc_to_sftp',
                            sftp_conn_id = 'ssh_default',
                            source_bucket = 'hmh_composer_demo',
                            source_object = 'export_files/covid.csv',
                            destination_path = 'sftp_files/',
                            move_object = True,
                            trigger_rule='none_failed')

    start >> bq_query_execute >> export_to_gcs >> file_move_gsc_to_sftp