import json
f = open("config.json")
config_data = json.load(f)

def test_tasks():
    assert(len(config_data['tasks'])>0)
def test_dag_name():
    assert(len(config_data['dag_name'])>0)