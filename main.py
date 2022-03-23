from DataType_Validation_Insertion_Training.DataTypeValidation import DBOperation
from DataTransfrom_Training.DataTransformation import DataTransform
from Training_raw_data_validation.rawValidation import RawDataValidation
import pandas as pd
from training_validation_insertion import TrainValidation
from Data_Ingestion.data_loader import DataGetter
from Data_Preprocessing.preprocessing import Preprocessor
from application_logger.logging import AppLogger


#a = DataTransform()
#c = a.converting_to_datetime()
#d = a.convert_column_to_part_of_day('Dep_Time')
#a.drops_rows_with_nan()
#d = a.drop_rows_based_on_value_count('Airline',2500)
#print(d)
#a.database_connection('flight_price_prediction')
#a.create_table_in_db("flight_price_prediction")
#a.insert_data_to_db_table("flight_price_prediction")
#a.selecting_data_from_table_into_csv('flight_price_prediction')
from Data_Preprocessing.clustering import KMeansClustering
from training_model import TrainModel


#a = TrainValidation('Training_Batch_Files/')
#a.train_validation()
#c = RawDataValidation('Training_Batch_Files')
#n,col_names,p = c.values_from_schema()
#print(col_names)

#a = DBOperation()
#a.create_table_in_db(col_names)
#a.insert_data_to_db_table('flight_price_prediction',col_names)
#a.selecting_data_from_table_into_csv('flight_price_prediction')
#print('finished')


#file = 'Training_Logs/GeneralLog.txt'
#logger = AppLogger()
#c = Preprocessor(file, logger)
#d = c.onehot_encoder('Airline')
#print(d)

#a = KMeansClustering(file, logger)
#a.elbow_plot(d)

#a = TrainModel()

#a.training_model()




##### Prediction ####

from Prediction_Raw_Rata_Validation.predictionDataValidation import PredictionDataValidation
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import DBOperation

a = PredictionDataValidation('Prediction_Batch_Files/')
LengthOfYearStampInFile, column_names, NumberOfColumns = a.values_from_schema()
#regex = a.manual_regex_creation()
#a.create_directory_for_GoodBadRaw_data()
#a.validation_file_name_raw(regex,LengthOfYearStampInFile)
#a.validate_number_of_columns(NumberOfColumns)
#a.validate_missing_values_in_whole_columns()
#a.move_BadFiles_to_Archive()

x = DBOperation()
x.database_connection('flight_price_prediction')
x.create_table_in_db(column_names)
x.insert_data_to_db_table('flight_price_prediction', column_names)



