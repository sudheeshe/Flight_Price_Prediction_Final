import yaml
import pathlib

path = 'D:/Experimental Space/Flight Price Prediction_MLOps/params.yaml'


""" 
def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config
        
"""


def read_params(config_path):
    path = pathlib.Path(config_path)
    with open(path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config