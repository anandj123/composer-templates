# DAGTemplates
Composer DAG template project

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