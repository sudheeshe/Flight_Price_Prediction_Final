from os import listdir
import os
import csv
import pandas as pd
from Src.Read_Yaml import read_params
from Src.Logging import AppLogger


class DataGenerator:
    """
          This class shall be used for handling all the Cassandra DB operations.
    """

    def __init__(self):
        self.schema = read_params('params.yaml')
        self.badFilePath = self.schema['test_data']['validated_raw_test_data'] + "Bad_Raw"
        self.goodFilePath = self.schema['test_data']['validated_raw_test_data'] + "Good_Raw"
        self.logger = AppLogger()



    def final_input_data_generator(self):

        """
               Method Name: insertIntoTableGoodData
               Description: This method inserts the Good data files from the Good_Raw folder into the above created table.
               Output: None
               On Failure: Raise Exception
        """

        goodFilePath = self.goodFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        file = open(self.schema['logs']['log_dir_prediction'] + '/DbInsertLog.txt', 'a+')

        try:
            col_names = ['Airline', 'Date_of_Journey', 'Source', 'Destination', 'Route', 'Dep_Time',
                         'Arrival_Time', 'Duration', 'Total_Stops', 'Additional_Info', 'Price', 'ID']
            df_total = pd.DataFrame(columns=col_names)
            for files in onlyfiles:
                data = pd.read_excel(self.goodFilePath + "/" + files, engine='openpyxl')
                # creating a new column 'ID' to the dataframe
                df_total = df_total.append(data)

            df_total['ID'] = pd.Series(range(0, len(df_total)))

            self.logger.log(file, "CSV Loaded successfully")
            file.close()
            return df_total.to_csv(self.schema['test_data']['final_test_data'], index=False)


        except Exception as e:
            self.logger.log(file, f"Error while inserting data to DB {e}")
            file.close()

