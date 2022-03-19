from DataType_Validation_Insertion_Training.DataTypeValidation import DBOperation
from DataTransfrom_Training.DataTransformation import DataTransform
from Training_raw_data_validation.rawValidation import RawDataValidation
import pandas as pd
from training_validation_insertion import TrainValidation


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


#a = TrainValidation('Training_Batch_Files/')
#a.train_validation()
c = RawDataValidation('Training_Batch_Files')
n,col_names,p = c.values_from_schema()
print(col_names)

a = DBOperation()
a.create_table_in_db('flight_price_prediction',col_names)
