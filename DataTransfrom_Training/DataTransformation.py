from datetime import datetime
from os import listdir
from application_logger.logging import AppLogger
import pandas as pd


class DataTransform:
    """
          This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.
    """

    def __init__(self):
        self.goodDataPath = "Training_Raw_files_validated/Good_Raw"
        self.logger = AppLogger()

    def converting_to_datetime(self):
        """
              Method Name: converting_to_datetime
              Description: This method converts Date_of_Journey to datetime format.
                           And this method creates 'Month_of_Journey' , 'Day_of_Journey', 'Year_of_Journey' separate columns
                           from 'Date_of_Journey' column. This Method drops 'Date_of_Journey' column once the output get created

              Output: A Dataframe, by after creating  'Month_of_Journey' , 'Day_of_Journey', 'Year_of_Journey' columns and
                        Dropping 'Date_of_Journey'
        """

        file = open("Training_Logs/dataTransformLog.txt", 'a+')
        try:
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for files in onlyfiles:
                df = pd.read_excel(self.goodDataPath + "/" + files, engine='openpyxl')
                #Converting 'Date_of_Journey' to datetime format
                df.Date_of_Journey = pd.to_datetime(df['Date_of_Journey'], format = '%d/%m/%Y')

                # Creating 'Month_of_Journey' , 'Day_of_Journey', 'Year_of_Journey' columns
                df['Month_of_Journey'] = df['Date_of_Journey'].dt.month
                df['Day_of_Journey'] = df['Date_of_Journey'].dt.day
                df['Weekday_of_Journey'] = df['Date_of_Journey'].dt.weekday

                # Dropping 'Date_of_Journey' column
                df.drop('Date_of_Journey', axis='columns', inplace=True)
                self.logger.log(file, "Conversion to datetime format of 'Date_of_Journey' column is successful......!!")
                file.close()
                return df


        except Exception as e:
            self.logger.log(file, f"Error occurred during conversion {e}")
            file.close()


    def parts_of_the_day(self, x):

        """
              Method Name: parts_of_the_day
              Description: This method converts Pandas Series given in 'HH:MM:SS DD:MM:YYYY' format to the Morning, Afternoon, Evening,Night
              Output: Pandas Series
        """

        file = open("Training_Logs/dataTransformLog.txt", 'a+')
        try:
            x = x.strip()
            t = (int)(x.split(':')[0])

            if (t >= 5 and t < 11):
                x = 'morning'
            elif (t >= 11 and t < 16):
                x = 'afternoon'
            elif (t >= 16 and t < 21):
                x = 'evening'
            elif (t >= 21 or t < 5):
                x = 'night'

            return x

        except Exception as e:
            self.logger.log(file, f"Error occurred {e}")



    def convert_column_to_part_of_day(self, column_name):
        """
              Method Name: convert_column_to_part_of_day
              Description: This method converts the mentioned column to parts of the day like (Morning, Afternoon, Evening,Night)
              Output: A Dataframe
                """


        file = open("Training_Logs/dataTransformLog.txt", 'a+')
        try:
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for files in onlyfiles:
                df = pd.read_excel(self.goodDataPath + "/" + files, engine='openpyxl')
                df[column_name] = df[column_name].apply(self.parts_of_the_day)


            self.logger.log(file, f"Conversion to parts_of_day of '{column_name}' column is successful......!!")
            file.close()
            return df




        except Exception as e:
            self.logger.log(file, f"Conversion to parts_of_day of '{column_name}' column failed {e}")
            file.close()















