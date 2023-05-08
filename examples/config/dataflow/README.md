# Cloud Composer DAG Templates for Dataflow

## Generate DAG for Dataflow Classic Templates

To generate a DAG for submitting dataflow classic template job use ***dataflow_submit_classic_template_job_simple_dag.yaml*** file as reference. File explains how configuration needs to be provided for generating DAG. Visit airflow page for information more on [DataflowTemplatedJobStartOperator](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataflow/index.html#airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator).

For this example [Google Provided Dataflow Templates](https://cloud.google.com/dataflow/docs/guides/templates/provided/bigquery-to-tfrecords) is used.

Run below command to generate the template:
```sh
python3 ~/composer-templates/source/generate_dag.py -config_file ~/composer-templates/example/config/dataflow/dataflow_submit_classic_template_job_simple_dag.yaml
```

Generated dag is stored in dag folder inside directory from where above command is run. Navigate to [dataflow dag](./dag/dataflow_submit_classic_template_job_simple_dag.py) for example output of above command.