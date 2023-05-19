# Gives the names of required variables
import airflow
import inspect
import funcsigs
from airflow.providers.google.cloud.operators.dataproc import DataprocListBatchesOperator

# option 1 to get the list of variable
# args_names = inspect.signature(DataprocListBatchesOperator).parameters.keys()
# print(args_names)

# Gives entire string of all the variables we can loop through this and create a custom logic
try:  # python 3.3+
    from inspect import signature
except ImportError:
    from funcsigs import signature


def get_function_parameters(operator):
    operator_name = str(operator.split("import")[-1]).strip()
    operator_args = str(signature(eval(operator_name)))
    print(operator_args)
    
    variable_name = []
    variable_type = []
    for args in operator_args.split(","):
        if ":" in args:
            variable_name.append(args.split(":")[0].strip())
            variable_type.append(args.split(":")[1].strip().replace("'",""))
        else:
            pass
        
    variable_dict = dict(zip(variable_name,variable_type))
    # print(variable_name)
    # print(variable_type)
    # print(variable_dict)
    
    return variable_dict

operator = 'from airflow.providers.google.cloud.operators.dataproc import DataprocListBatchesOperator'
variable_dict = get_function_parameters(operator=operator)
print(variable_dict)




