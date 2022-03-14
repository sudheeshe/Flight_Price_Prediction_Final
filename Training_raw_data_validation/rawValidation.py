from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logger.logging import AppLogger


class rawDataValidation:
    """
                 This class shall be used for handling all the validation done on the Raw Training Data!!.
    """

    def __init__(self, path):
        self.Batch_directory = path
        self.schema_path = 'schema_training.json'
        self.logger = AppLogger()

    def values_from_schema(self):
        """
                                Method Name: values_from_schema
                                Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                                Output: LengthOfYearStampInFile, column_names, NumberOfColumns
                                On Failure: Raise ValueError,KeyError,Exception
        """

        try:
            with open(self.schema_path) as f:
                dic = json.load(f)

            pattern = dic["SampleFileName"]
            LengthOfYearStampInFile = dic['LengthOfYearStampInFile']
            column_names = dic['ColName']
            NumberOfColumns = dic['NumberOfColumns']

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message = f"LengthOfYearStampInFile : {LengthOfYearStampInFile}" + "\t" + "NumberOfColumns: {NumberOfColumns}" + "\n"
            self.logger.log(file, message)
            file.close()

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfYearStampInFile, column_names, NumberOfColumns

    def manual_regex_creation(self):
        """
                                        Method Name: manual_regex_creation
                                        Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                                    This Regex is used to validate the filename of the training data.
                                        Output: Regex pattern
                                        On Failure: None
        """

        regex = "['data']+['\_'']+[\d]+\.xlsx"
        return regex

    def create_directory_for_GoodBadRaw_data(self):

        """
                                              Method Name: create_directory_for_GoodBadRaw_data
                                              Description: This method creates directories to store the Good Data and Bad Data
                                                            after validating the training data.

                                              Output: None
                                              On Failure: OSError
        """

        try:
            path = os.path.join("Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

            path = os.path.join("Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, f"Error while creating Directory: {e}")
            file.close()
            raise e

    def delete_existing_Good_data_training_folder(self):
        """
                      Method Name: delete_existing_Good_data_training_folder
                      Description: This method deletes the directory made  to store the Good Data
                                after loading the data in the table. Once the good files are
                                loaded in the DB,deleting the directory ensures space optimization.
                      Output: None
                      On Failure: OSError

        """
        try:
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path+ 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, f"Error while Deleting Directory : {e}")
            file.close()
            raise e

    def delete_existing_Bad_data_training_folder(self):

        """
                                            Method Name: delete_existing_Bad_data_training_folder
                                            Description: This method deletes the directory made to store the bad Data.
                                            Output: None
                                            On Failure: OSError
        """

        try:
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "BadRaw directory deleted before starting validation!!!")
                file.close()
        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, f"Error while Deleting Directory : {e}")
            file.close()
            raise e


    def move_BadFiles_to_Archive(self):

        """
                Method Name: move_BadFiles_to_Archive
                Description: This method deletes the directory made  to store the Bad Data
                             after moving the data in an archive folder. We archive the bad
                             files to send them back to the client for invalid data issue.
                Output: None
                On Failure: OSError
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:

            source = 'Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_' + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
                file.close()

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, f"Error while moving bad files to archive: {e}")
            file.close()
            raise e


    def validation_file_name_raw(self, regex, LengthOfYearStampInFile):

        """
            Method Name: validation_file_name_raw
            Description: This function validates the name of the training csv files as per given name in the schema!
                         Regex pattern is used to do the validation.If name format do not match the file is moved
                         to Bad Raw Data folder else in Good raw data.
            Output: None
            On Failure: Exception

        """

        self.delete_existing_Bad_data_training_folder()
        self.delete_existing_Good_data_training_folder()

        onlyfiles = [f for f in listdir(self.Batch_directory)]

        try:
            self.create_directory_for_GoodBadRaw_data()
            f = open("Training_Logs/nameValidationLog.txt", 'a+')

            for filename in onlyfiles:
                if (re.match(regex,filename)):
                    splitAtDot = re.split('.xlsx', filename)
                    splitAt_ = re.split('_', splitAtDot[0])

                    if (splitAt_[0]) == 'data':
                        if len(splitAt_[1]) == LengthOfYearStampInFile:
                            shutil.copy(f"Training_Batch_Files/{filename}", "Training_Raw_files_validated/Good_Raw")
                            self.logger.log(f, "Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()


        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, f"Error occured while validating FileName {e}" )
            f.close()
            raise e
                


    def validate_number_of_columns(self, number_of_columns):
        """
                Method Name: validate_number_of_columns
                Description: This function validates the number of columns in the csv files.
                            It is should be same as given in the schema file.
                            If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                            If the column number matches, file is kept in Good Raw Data for processing.
                Output: None
                On Failure: Exception
        """

        try:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Column Length Validation Started!!")

            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                data = pd.read_excel("Training_Raw_files_validated/Good_Raw/" + file)
                if data.shape[1] == number_of_columns:
                    pass
                else:
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)

            self.logger.log(f, "Column Length Validation Completed!!")

        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()


    def validate_missing_values_in_whole_columns(self):
        """
                 Method Name: validate_missing_values_in_whole_columns
                 Description: This function validates if any column in the csv file has all values missing.
                               If all the values are missing, the file is not suitable for processing.
                               SUch files are moved to bad raw data.

                 Output: None
                 On Failure: Exception

        """

        try:
            file = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(file,"Missing Values Validation Started!!")

            path = 'Training_Raw_Files_Validated/Good_Raw/'
            for f in os.listdir(path):
                data = pd.read_excel(path + f)
                for col in data.columns:
                    if data[col].isnull().all():
                        shutil.move(path + f, 'Training_Raw_Files_Validated/Bad_Raw')
                        self.logger.log(file,"File is invalid, an entire column have null values, File moved to Bad_Raw")

        except Exception as e:
            file = open('Training_Logs/MissinValuesInWholeColumnLog.txt', "a+")
            self.logger.log(file, f"Error while validating missing values in entire column {e}")
            file.close()



