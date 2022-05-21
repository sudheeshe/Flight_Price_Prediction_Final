from datetime import datetime
from Src.Prediction_Data_Validation import PredictionDataValidation
from Src.Logging import AppLogger
from Src.Read_Yaml import read_params


class PredictionValidation:
    """
    This method validates and transform the training data
    """

    def __init__(self, path):
        self.schema = read_params('params.yaml')
        self.raw_data = PredictionDataValidation(path)
        self.file = open(self.schema['logs']['log_dir_prediction'] + "Prediction_Main_Log.txt", 'a+')
        self.logger = AppLogger()

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
            self.file.close()


        except Exception as e:
            raise e
