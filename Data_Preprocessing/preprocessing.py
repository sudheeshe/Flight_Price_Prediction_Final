import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler, LabelEncoder
from datetime import datetime
from os import listdir
from application_logger.logging import AppLogger


class Preprocessor:

    """
          This class shall  be used to clean and transform the data before training..
    """

    def __init__(self, file, logger):
        self.input_file = 'Training_FileFromDB/'
        self.logger = logger
        self.file = file

    def converting_to_datetime(self):
        """
              Method Name: converting_to_datetime
              Description: This method converts Date_of_Journey to datetime format.
                           And this method creates 'Month_of_Journey' , 'Day_of_Journey', 'Year_of_Journey' separate columns
                           from 'Date_of_Journey' column. This Method drops 'Date_of_Journey' column once the output get created

              Output: A Dataframe, by after creating  'Month_of_Journey' , 'Day_of_Journey', 'Year_of_Journey' columns and
                        Dropping 'Date_of_Journey'
        """


        try:
            onlyfiles = [f for f in listdir(self.input_file)]
            for files in onlyfiles:
                df = pd.read_csv(self.input_file + "/" + files)
                #Converting 'Date_of_Journey' to datetime format
                df.Date_of_Journey = pd.to_datetime(df['Date_of_Journey'], format = '%d/%m/%Y')

                # Creating 'Month_of_Journey' , 'Day_of_Journey', 'Year_of_Journey' columns
                df['Month_of_Journey'] = df['Date_of_Journey'].dt.month
                df['Day_of_Journey'] = df['Date_of_Journey'].dt.day
                df['Weekday_of_Journey'] = df['Date_of_Journey'].dt.weekday

                # Dropping 'Date_of_Journey' column
                df.drop('Date_of_Journey', axis='columns', inplace=True)
                self.logger.log(self.file, "Conversion to datetime format of 'Date_of_Journey' column is successful......!!")
                self.file.close()
                return df


        except Exception as e:
            self.logger.log(self.file, f"Error occurred during conversion {e}")
            self.file.close()


    def parts_of_the_day(self, x):

        """
              Method Name: parts_of_the_day
              Description: This method converts Pandas Series given in 'HH:MM:SS DD:MM:YYYY' format to the Morning, Afternoon, Evening,Night
              Output: Pandas Series
        """


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
            self.logger.log(self.file, f"Error occurred {e}")



    def convert_column_to_part_of_day(self, column_name):
        """
              Method Name: convert_column_to_part_of_day
              Description: This method converts the mentioned column to parts of the day like (Morning, Afternoon, Evening,Night)
              Output: A Dataframe
        """

        try:
            onlyfiles = [f for f in listdir(self.input_file)]
            for files in onlyfiles:
                df = pd.read_csv(self.input_file + "/" + files)
                df[column_name] = df[column_name].apply(self.parts_of_the_day)

            self.logger.log(self.file, f"Conversion to parts_of_day of '{column_name}' column is successful......!!")
            self.file.close()
            return df

        except Exception as e:
            self.logger.log(self.file, f"Conversion to parts_of_day of '{column_name}' column failed {e}")
            self.file.close()


    def drop_column(self, column_name):

        """
             Method Name: drop_column
             Description: This method drops the specified column from the dataframe
             Output: Nil
        """


        try:
            onlyfiles = [f for f in listdir(self.input_file)]
            for files in onlyfiles:
                df = pd.read_csv(self.input_file + "/" + files)
                df.drop(column_name, axis='columns', inplace= True,)
            self.logger.log(self.file, f'{column_name} column has been removed successfully...!!')
            self.file.close()
            return df

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while removing {column_name} column, Error is {e}")
            self.file.close()



    def drops_rows_with_nan(self):
        """
              Method Name: drops_rows_with_nan
              Description: This method drops the rows which have NaN
              Output: Nil
        """

        try:
            onlyfiles = [f for f in listdir(self.input_file)]
            for files in onlyfiles:
                df = pd.read_csv(self.input_file + "/" + files)
                df.dropna(inplace = True)
            self.logger.log(self.file, "Dropping Nan rows are successfull....!!")
            self.file.close()
            return df

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while dropping rows with NaN values {e}")
            self.file.close()


    def mapping(self, column_name):
        """
              Method Name: mapping
              Description: This method does the mapping of given column_values into below specified integer values
              Output: Nil
        """

        try:
            onlyfiles = [f for f in listdir(self.input_file)]
            for files in onlyfiles:
                df = pd.read_csv(self.input_file + "/" + files)
                df[column_name] = df[column_name].map({'non-stop': 0, '1 stop': 1, '2 stops': 2, '3 stops': 3, '4 stops': 4})
                df[column_name] = df[column_name].astype('int')
            self.logger.log(self.file, "Mapping of the values are successful....!!")
            self.file.close()
            return df

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while mapping values {e}")
            self.file.close()


    def merging_values(self, column_name, value_to_be_replaced, value_to_replace):
        """
               Method Name: merging_values
               Description: This method does the merging of the values of given column
               Output: Nil
        """

        try:
            onlyfiles = [f for f in listdir(self.input_file)]
            for files in onlyfiles:
                df = pd.read_csv(self.input_file + "/" + files)
                df[column_name] = df[column_name].replace(value_to_be_replaced, value_to_replace)
            self.logger.log(self.file, "Merging of the values are successful....!!")
            self.file.close()
            return df

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while merging values {e}")
            self.file.close()


    def remove_row(self, value):

        """
             Method Name: remove_row
             Description: This method does the removes the row which have value given as input on 'Duration' column
             Output: Nil
        """

        try:
            onlyfiles = [f for f in listdir(self.input_file)]
            for files in onlyfiles:
                df = pd.read_csv(self.input_file + "/" + files)
                df.drop(df.loc[df['Duration'] == value].index, inplace=True)
            self.logger.log(self.file, f"removal of the rows with {value} on 'Duration' column are successful....!!")
            self.file.close()
            return df

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while merging values {e}")
            self.file.close()

    def convert_to_minutes(self, data):

        """
             Method Name: convert_to_minutes
             Description: This method does the conversion of time into minutes
             Output: Nil
        """
        try:
            data = data.strip()
            total_time = data.split(' ')
            to = total_time[0]
            hrs = (int)(to[:-1]) * 60
            if ((len(total_time)) == 2):
                mint = (int)(total_time[1][:-1])
                hrs = hrs + mint
            output = int(hrs)
            return output

        except Exception as e:

            self.logger.log(self.file, f"Error occurred while removing values {e}")
            self.file.close()




    def column_value_into_minutes(self, column_name):
        """
             Method Name: column_value_into_minutes
             Description: This method does the conversion of time into minutes of specified column
             Output: Nil
        """

        try:
            onlyfiles = [f for f in listdir(self.input_file)]
            for files in onlyfiles:
                df = pd.read_csv(self.input_file + "/" + files)
                df[column_name] = df[column_name].apply(self.convert_to_minutes)
            self.logger.log(self.file, "Conversion of column values into minutes are successful....!!")
            self.file.close()
            return df

        except Exception as e:
            self.logger.log(self.file, f"Error while converting column values into minutes {e}")
            self.file.close()



    def label_encoder(self, column_name):
        """
             Method Name: column_value_into_minutes
             Description: This method does the conversion of time into minutes of specified column
             Output: Nil
        """

        try:
            encoder = LabelEncoder()
            onlyfiles = [f for f in listdir(self.input_file)]
            for files in onlyfiles:
                df = pd.read_csv(self.input_file + "/" + files)
                df[column_name] = encoder.fit_transform(df[column_name])
            self.logger.log(self.file, "Label Encoding of the values are successful....!!")
            self.file.close()
            return df

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while label encoding {e}")
            self.file.close()


    def drop_rows_based_on_value_count(self, column_name, value):
        """
             Method Name: column_value_into_minutes
             Description: This method does the conversion of time into minutes of specified column
             Output: Nil
        """

        try:
            onlyfiles = [f for f in listdir(self.input_file)]
            for files in onlyfiles:
                df = pd.read_csv(self.input_file + "/" + files)
                count = df[column_name].value_counts()
                df_new = df[~df[column_name].isin(count[count < int(value)].index)]
            self.logger.log(self.file, "Dropping of the row based on value_counts are successful....!!")
            self.file.close()
            return df_new

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while dropping records based on value_counts {e}")
            self.file.close()











