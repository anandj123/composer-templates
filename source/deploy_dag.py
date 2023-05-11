
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
# [START deploy_dag]
 
import os
import json
import argparse
import yaml
import google.auth
import google.auth.transport.requests
from jinja2 import Environment, FileSystemLoader
from google.cloud import storage

input_folder = ''
composer_environment = ''
dag_gcs_path = ''
credentials = ''
# Read parametes: 
#   -input_folder = input folder name where python dags are preset
#   -composer-environment = project.location.environmentname 
def read_parameters():
    description = '''This application deploys Airflow python DAG 
        file to the composer environment that is supplied as parameters.'''
        
    parser = argparse.ArgumentParser(description= description)

    parser.add_argument('-input_folder', 
                    required=True,
                    help='''Provide input folder name. 
                    e.g. ./examples/config/dag  ''')
    parser.add_argument('-composer_environment', 
                    required=True,
                    help='''Provide fully qualified composer environment name 
                    e.g. <project-id>.<location-id>.<environment-id>
                        my-project.us-east1.composer-env-1 ''')
    options = parser.parse_args()
    global input_folder, composer_environment 

    input_folder = options.input_folder
    composer_environment = options.composer_environment

def get_dag_prefix(project_id, location, composer_env_id):
    global dag_gcs_path, credentials

    credentials, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/cloud-platform'])
    authed_session = google.auth.transport.requests.AuthorizedSession(
        credentials)

    environment_url = (
        'https://composer.googleapis.com/v1beta1/projects/{}/locations/{}'
        '/environments/{}').format(project_id, location, composer_env_id)
    response = authed_session.request('GET', environment_url)
    environment_data = response.json()

    print(environment_data)

# https://iam.googleapis.com/projects/424899038521/locations/global/workloadIdentityPools/github-action-pool-1/providers/github


    dag_gcs_path = environment_data['config']['dagGcsPrefix']
    print("Deploying DAG files to:", dag_gcs_path)
    dag_bucket_split = dag_gcs_path.split('/')
    dag_gcs_path = dag_bucket_split[2] 

def deploy_dag_file():
    (project_id, location, composer_env_id) = composer_environment.split('.') 
    get_dag_prefix(project_id, location, composer_env_id)
    for path in os.listdir(input_folder):
        print("Deploying file:", path)
        client = storage.Client(credentials=credentials, project=project_id)
        bucket = client.get_bucket(dag_gcs_path)
        blob = bucket.blob("dags/" + path)
        blob.upload_from_filename(os.path.join(input_folder,path))

if __name__ == '__main__':
    read_parameters()
    deploy_dag_file()

 # [END generate_dag]