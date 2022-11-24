from jinja2 import Environment, FileSystemLoader
import os
import json

f = open('dag.json')
config_data = json.load(f)

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))

template = env.get_template('simple_dag.template')

# I don't know what the configuration format but as long as you can convert to a dictionary, it can work.
values = {}

filename = os.path.join(file_dir, config_data['generated_file_name'])
with open(filename, 'w') as fh:
 fh.write(template.render(
     dag_id=config_data['dag_name'],
     num_task=config_data['number_of_tasks'],
     **values
 ))