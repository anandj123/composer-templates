# DAGTemplates
Composer DAG template project

# Configuration file

```json

{
    "dag_name":"process_batch_data",
    "schedule_interval": "0 0 * * *",
    "tasks":[
        {
            "task_id": "preprocess",
            "task_type": "bas_operator",
            "bash_command":"./scripts/cmd.sh"
        },
        {
            "task_id": "load_data",
            "task_type": "gcs_to_bigquery",
            "bucket":"anand-bq-test-2",
            "source_objects":["HCA_TEST/HCA_TEST_HCA_Test.csv"],
            "destination_project_dataset_table": "anand-bq-test-2.Anand_BQ_Test_1.test3",
            "autodetect":"True",
            "write_disposition":"WRITE_TRUNCATE"
        },
        {
            "task_id": "call_sp",
            "task_type": "bigquery_stored_procedure",
            "stored_procedure_name":"Anand_BQ_Test_1.GetJobHash('a')"
        }
    ]   
}

```

# Run template

```sh
python3 generate_dag.py \
-config_json config.json \
-template_name simple_dag.template \
-generate_file_name simple_dag.py

```

# Run test

```sh
pytest -v
```