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
from colorama import Fore, Back, Style, init
init(autoreset=True)

config_json = ''
template_name = ''
generate_file_name = ''
def main():
    parser = argparse.ArgumentParser(description= \
        '''This application downloads CCDA files from GCS, 
        parses them using bluebutton.js library
        and uploads the parsed content to BigQuery.''')

    parser.add_argument('-config_json', 
                    required=True,
                    help='''Provide DAG configuration json file location
                    e.g. ./config.json''')
    parser.add_argument('-template_name', 
                        required=True,
                        help='''Provide template name to use
                                e.q. simple_dag.template''')
    parser.add_argument('-generate_file_name', 
                        required=True,
                        help='''Provide file name to generate
                                e.q. simple_dag.py''')
    options = parser.parse_args()
    global config_json, template_name,generate_file_name

    config_json = options.config_json
    template_name = options.template_name
    generate_file_name = options.generate_file_name
def process():
    print("{:<30}".format("Generating DAG for  ") + Fore.GREEN + template_name)
    print("{:<30}".format("Config file ") + Fore.GREEN + config_json)

    f = open(config_json)
    config_data = json.load(f)

    file_dir = os.path.dirname(os.path.abspath(__file__))
    env = Environment(loader=FileSystemLoader(file_dir))

    template = env.get_template(template_name)

    values = {}

    filename = os.path.join(file_dir, generate_file_name)

    with open(filename, 'w') as fh:
        fh.write(template.render(
            config_data=config_data,
            **values
        ))
    print("{:<30}".format("Finished generating file ") + Fore.GREEN + generate_file_name)
    print("{:<30}".format("Number of tasks generated ") + Fore.GREEN + str(len(config_data['tasks'])))

if __name__ == '__main__':
    main()
    process()

 # [END generate_dag]


