# DAGTemplates
Composer DAG template project

# Install requirements
```sh
python3 -m pip install -r requirements.txt
```

# Configuration file

```yaml
---
dag_name: simple_dag
schedule_interval: 0 0 0 * *
tasks:
- task_id: preprocess
  task_type: bash_operator
  bash_command: "./scripts/cmd.sh"
- task_id: load_data
  task_type: gcs_to_bigquery
  bucket: anand-bq-test-2
  source_objects:
  - HCA_TEST/HCA_TEST_HCA_Test.csv
  biquery_table: anand-bq-test-2.Anand_BQ_Test_1.test3
  autodetect: 'True'
  write_disposition: WRITE_TRUNCATE
- task_id: call_sp
  task_type: bigquery_stored_procedure
  stored_procedure_name: Anand_BQ_Test_1.GetJobHash('a')
- task_id: call_sp2
  task_type: bigquery_stored_procedure
  stored_procedure_name: Anand_BQ_Test_1.GetJobHash('b')


```

# Run template

```sh
python3 generate_dag.py -config_file config.yaml
```

# Run test

```sh
python3 -m pytest -v
```