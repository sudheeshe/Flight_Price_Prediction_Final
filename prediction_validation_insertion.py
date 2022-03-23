from datetime import datetime
from Prediction_Raw_Rata_Validation.predictionDataValidation import PredictionDataValidation
from DataType_Validation_Insertion_Prediction.DataTypeValidationPrediction import DBOperation
from application_logger.logging import AppLogger


class PredictionValidation:
    """
    This method validates and transform the training data
    """

    def __init__(self, path):
        self.raw_data = PredictionDataValidation(path)
        self.db_operation = DBOperation()
        self.file = open("Prediction_Logs/Prediction_Main_Log.txt", 'a+')
        self.logger = AppLogger()
        self.database_name = 'flight_price_prediction'

    def prediction_validation(self):

        try:
            self.logger.log(self.file, 'Start of Validation on files for prediction!!')
            # extracting values from prediction schema
            LengthOfYearStampInFile, column_names, NumberOfColumns = self.raw_data.values_from_schema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manual_regex_creation()
            # validating filename of prediction files
            self.raw_data.validation_file_name_raw(regex, LengthOfYearStampInFile)
            # validating column length in the file
            self.raw_data.validate_number_of_columns(NumberOfColumns)
            # validating if any column has all values missing
            self.raw_data.validate_missing_values_in_whole_columns()
            self.logger.log(self.file, "Raw Data Validation Complete!!")

            self.logger.log(self.file, "Creating Prediction_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema file
            self.db_operation.create_table_in_db(self.database_name)
            self.logger.log(self.file, f"Table created successfully on Cassandra DB ")
            self.logger.log(self.file, "Starting Insertion of Data into Table !!!!")
            # insert csv files in the table
            self.db_operation.insert_data_to_db_table(self.database_name, column_names)
            self.logger.log(self.file, "Data has been inserted successfully on Casandra!!!")
            self.logger.log(self.file, "Deleting Good Data Folder")
            # Delete the good data folder after loading files in table
            self.raw_data.delete_existing_Good_data_training_folder()
            self.logger.log(self.file, "Good_Data folder deleted!!!")
            self.logger.log(self.file, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.move_BadFiles_to_Archive()
            self.logger.log(self.file, "Bad files moved to archive!! Bad folder Deleted!!")
            self.logger.log(self.file, "Validation Operation completed!!")
            self.logger.log(self.file, "Extracting csv file from table")
            # export data in table to csvfile
            self.db_operation.selecting_data_from_table_into_csv(self.database_name)
            self.file.close()





        except Exception as e:
            raise e
