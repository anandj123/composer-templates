import yaml
f = open("config.yaml")
config_data = yaml.safe_load(f)
    
def test_tasks():
    assert(len(config_data['tasks'])>0)
def test_dag_name():
    assert(len(config_data['dag_name'])>0)
def test_scheduled_interval():
    assert(len(config_data['schedule_interval'])>0)