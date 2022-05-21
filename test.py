from Src.Read_Yaml import read_params
from Src.Prediction_Final_Data_Input import DataGenerator
from Src.Prediction_Validation_Insertion import PredictionValidation

path = read_params('params.yaml')
pred_val_obj = PredictionValidation(path['test_data']['raw_test_dataset'])
pred_val_obj.prediction_validation()



