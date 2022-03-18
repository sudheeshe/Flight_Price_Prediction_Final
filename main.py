from DataType_Validation_Insertion_Training.DataTypeValidation import DBOperation
from DataTransfrom_Training.DataTransformation import DataTransform
import pandas as pd


a = DataTransform()
#c = a.converting_to_datetime()
d = a.convert_column_to_part_of_day('Dep_Time')
print(d)






#a.database_connection('flight_price_prediction')
#a.create_table_in_db("flight_price_prediction")
#a.insert_data_to_db_table("flight_price_prediction")
#a.selecting_data_from_table_into_csv('flight_price_prediction')


