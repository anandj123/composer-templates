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
from colorama import Fore, Back, Style, init
init(autoreset=True)

config_file = ''
template_name = 'simple_dag.template'
generate_file_name = ''
def main():
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

def process():
    print("{:<30}".format("Generating DAG for  ") + Fore.GREEN + template_name)
    print("{:<30}".format("Config file ") + Fore.GREEN + config_file)

    with open(config_file,'r') as f:
        config_data = yaml.safe_load(f)
        file_dir = os.path.dirname(os.path.abspath(__file__))

        template_dir = os.path.join(file_dir,"templates")
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template(config_data['dag_template']+".template")

        values = {}
        generate_file_name = config_data['dag_name'] +'.py'
        filename = os.path.join(os.path.dirname(os.path.abspath(config_file)),
                         generate_file_name)

        with open(filename, 'w') as fh:
            fh.write(template.render(
                config_data=config_data,
                **values
            ))

        print("{:<30}".format("Finished generating file ") + 
                    Fore.GREEN + 
                    generate_file_name)
        print("{:<30}".format("Number of tasks generated ") + 
                    Fore.GREEN + 
                    str(len(config_data['tasks'])))

if __name__ == '__main__':
    main()
    process()

 # [END generate_dag]


