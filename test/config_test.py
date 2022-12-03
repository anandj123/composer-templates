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
import yaml
f = open("config.yaml")
config_data = yaml.safe_load(f)
    
def test_tasks():
    assert(len(config_data['tasks'])>0)
def test_dag_name():
    assert(len(config_data['dag_name'])>0)
def test_scheduled_interval():
    assert(len(config_data['schedule_interval'])>0)