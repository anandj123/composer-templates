# Copyright 2019 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# [START generate_dag]

from jinja2 import Environment, FileSystemLoader
import os
import json
import argparse
import yaml

config_file = ''

# Read configuration file from command line
# Please refer to the documentation (README.md) to see how to author a
# configuration (YAML) file that is used by the program to generate
# Airflow DAG python file.
def read_config():
    description = '''This application creates Composer DAGs based on the config file
        config.json and template. Current template supported are: 
        "simple_dag.template" '''
        
    parser = argparse.ArgumentParser(description= description)

    parser.add_argument('-config_file', 
                    required=True,
                    help='''Provide template configuration YAML file location
                    e.g. ./config.yaml''')
    options = parser.parse_args()
    global config_file

    config_file = options.config_file

# Generate Airflow DAG python file by reading the config (YAML) file
# that is passed to the program. This section loads a .template file
# located in the ./templates folder in the source and the template folder
# parses and dynamically generate a python file using Jinja2 template
# programming language. Please refer to Jinja documentation for Jinja 
# template authoring guidelines.
def generate_dag_file():

    with open(config_file,'r') as f:
        config_data = yaml.safe_load(f)

        config_path = os.path.abspath(config_file)
        file_dir = os.path.dirname(os.path.abspath(__file__))
        template_dir = os.path.join(file_dir,"templates")
        dag_template = config_data['dag_template']
        dag_name = config_data['dag_name']

        print("Config file: {}".format(config_path))
        print("Generating DAG for: {}".format(dag_template))

        # Uses template renderer to load and render the Jinja template
        # The template file is selected from config_data['dag_template']
        # variable from the config file that is input to the program.
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template(dag_template+".template")
        values = {}

        dag_path = os.path.join(os.path.dirname(config_path),"dag") 
        if not os.path.exists(dag_path):
            os.makedirs(dag_path)
            
        generate_file_name = os.path.join(dag_path, dag_name + '.py')
        with open(generate_file_name, 'w') as fh:
            fh.write(template.render(config_data=config_data, **values))

        print("Finished generating file: {}".format(generate_file_name))
        print("Number of tasks generated: {}".format(str(len(config_data['tasks']))))

if __name__ == '__main__':
    read_config()
    generate_dag_file()

 # [END generate_dag]


