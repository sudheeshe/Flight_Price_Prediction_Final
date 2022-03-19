from datetime import datetime
from Training_raw_data_validation.rawValidation import RawDataValidation
from DataTransfrom_Training.DataTransformation import DataTransform
from DataType_Validation_Insertion_Training.DataTypeValidation import DBOperation
from application_logger.logging import AppLogger


class TrainValidation:

    """
    This method validates and transform the training data
    """
    def __init__(self, path):
        self.raw_data = RawDataValidation(path)
        self.data_transform = DataTransform()
        self.db_operation = DBOperation()
        self.file = open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.logger = AppLogger()

    def train_validation(self):

        try:
            self.logger.log(self.file, 'Start of Validation on files for prediction!!')
            #extracting values from prediction schema
            LengthOfYearStampInFile, column_names, NumberOfColumns = self.raw_data.values_from_schema()
            #getting the regex defined to validate filename
            regex = self.raw_data.manual_regex_creation()
            # validating filename of prediction files
            self.raw_data.validation_file_name_raw(regex, LengthOfYearStampInFile)
            # validating column length in the file
            self.raw_data.validate_number_of_columns(NumberOfColumns)
            # validating if any column has all values missing
            self.raw_data.validate_missing_values_in_whole_columns()
            self.logger.log(self.file, "Raw Data Validation Complete!!")

        except Exception as e:
            print(e)