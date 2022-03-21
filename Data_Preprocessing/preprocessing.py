import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
from sklearn.compose import ColumnTransformer
from datetime import datetime
from os import listdir
import os
from application_logger.logging import AppLogger


class Preprocessor:

    """
          This class shall  be used to clean and transform the data before training..
    """

    def __init__(self, file, logger):
        self.input_file = 'Training_FileFromDB/'
        self.logger = logger
        self.file = file

    def converting_to_datetime(self, data, column_name):
        """
              Method Name: converting_to_datetime
              Description: This method converts Date_of_Journey to datetime format.
                           And this method creates 'Month_of_Journey' , 'Day_of_Journey', 'Year_of_Journey' separate columns
                           from 'Date_of_Journey' column. This Method drops 'Date_of_Journey' column once the output get created

              Output: A Dataframe, by after creating  'Month_of_Journey' , 'Day_of_Journey', 'Year_of_Journey' columns and
                        Dropping 'Date_of_Journey'
        """


        try:
            #Converting 'Date_of_Journey' to datetime format
            data[column_name] = pd.to_datetime(data[column_name], format = '%d-%m-%Y')

            # Creating 'Month_of_Journey' , 'Day_of_Journey', 'Year_of_Journey' columns
            data['Month_of_Journey'] = data[column_name].dt.month
            data['Day_of_Journey'] = data[column_name].dt.day
            data['Weekday_of_Journey'] = data[column_name].dt.weekday

            # Dropping 'Date_of_Journey' column
            data = data.drop(column_name, axis='columns')
            self.logger.log(self.file, f"Conversion to datetime format of '{column_name}' column is successful......!!")

            return data


        except Exception as e:
            self.logger.log(self.file, f"Error occurred during conversion {e}")



    def parts_of_the_day(self, x):

        """
              Method Name: parts_of_the_day
              Description: This method converts Pandas Series given in 'HH:MM:SS DD:MM:YYYY' format to the Morning, Afternoon, Evening,Night
              Output: Pandas Series
        """


        try:
            x = x.strip()
            t = (x.split(':')[0])

            if len(t) > 2:
                t = (x.split(' ')[1])
                t = (t.split(':')[0])
                if (int(t) >= 5 and int(t)  < 11):
                    string = 'morning'
                elif (int(t)  >= 11 and int(t)  < 16):
                    string = 'afternoon'
                elif (int(t)  >= 16 and int(t)  < 21):
                    string = 'evening'
                elif (int(t)  >= 21 or int(t)  < 5):
                    string = 'night'



            else:
                if (int(t)  >= 5 and int(t)  < 11):
                    string = 'morning'
                elif (int(t)  >= 11 and int(t)  < 16):
                    string = 'afternoon'
                elif (int(t)  >= 16 and int(t)  < 21):
                    string = 'evening'
                elif (int(t)  >= 21 or int(t)  < 5):
                    string = 'night'

            return string



        except Exception as e:
            self.logger.log(self.file, f"Error occurred {e}")



    def convert_column_to_part_of_day(self, data, column_name):
        """
              Method Name: convert_column_to_part_of_day
              Description: This method converts the mentioned column to parts of the day like (Morning, Afternoon, Evening,Night)
              Output: A Dataframe
        """

        try:
            data[column_name] = data[column_name].apply(self.parts_of_the_day)
            self.logger.log(self.file, f"Conversion to parts_of_day of '{column_name}' column is successful......!!")
            return data

        except Exception as e:
            self.logger.log(self.file, f"Conversion to parts_of_day of '{column_name}' column failed {e}")



    def drop_column(self, data, column_name):

        """
             Method Name: drop_column
             Description: This method drops the specified column from the dataframe
             Output: Nil
        """

        try:
            data.drop(column_name, axis='columns', inplace= True,)
            self.logger.log(self.file, f'{column_name} column has been removed successfully...!!')

            return data

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while removing {column_name} column, Error is {e}")




    def drops_rows_with_nan(self, data):
        """
              Method Name: drops_rows_with_nan
              Description: This method drops the rows which have NaN
              Output: Nil
        """

        try:
            #df = pd.read_csv(data)
            data.dropna(inplace = True)
            self.logger.log(self.file, "Dropping Nan rows are successfull....!!")

            return data

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while dropping rows with NaN values {e}")



    def mapping(self, data, column_name):
        """
              Method Name: mapping
              Description: This method does the mapping of given column_values into below specified integer values
              Output: Nil
        """

        try:
            data[column_name] = data[column_name].map({'non-stop': 0, '1 stop': 1, '2 stops': 2, '3 stops': 3, '4 stops': 4})
            data[column_name] = data[column_name].astype('int')
            self.logger.log(self.file, "Mapping of the values are successful....!!")


            return data

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while mapping values {e}")



    def merging_values(self, data, column_name, value_to_be_replaced, value_to_replace):
        """
               Method Name: merging_values
               Description: This method does the merging of the values of given column
               Output: Nil
        """

        try:
            data[column_name] = data[column_name].replace(value_to_be_replaced, value_to_replace)
            self.logger.log(self.file, "Merging of the values are successful....!!")

            return data

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while merging values {e}")



    def remove_row(self, data, value):

        """
             Method Name: remove_row
             Description: This method does the removes the row which have value given as input on 'Duration' column
             Output: Nil
        """

        try:

            df = data.drop(data.loc[data['Duration'] == value].index)
            self.logger.log(self.file, f"removal of the rows with 'Duration' column are successful....!!")
            return df

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while merging values {e}")


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





    def column_value_into_minutes(self, data, column_name):
        """
             Method Name: column_value_into_minutes
             Description: This method does the conversion of time into minutes of specified column
             Output: Nil
        """

        try:
            data[column_name] = data[column_name].apply(self.convert_to_minutes)
            data[column_name] = data[column_name].astype('int')
            self.logger.log(self.file, "Conversion of column values into minutes are successful....!!")

            return data

        except Exception as e:
            self.logger.log(self.file, f"Error while converting column values into minutes {e}")




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

            return df

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while label encoding {e}")



    def drop_rows_based_on_value_count(self, data, column_name, value):
        """
             Method Name: column_value_into_minutes
             Description: This method does the conversion of time into minutes of specified column
             Output: Nil
        """

        try:

            count = data[column_name].value_counts()
            df = data[~data[column_name].isin(count[count < int(value)].index)]
            #self.logger.log(self.file, "Dropping of the row based on value_counts are successful....!!")

            return df

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while dropping records based on value_counts {e}")


    def separate_label_feature(self, data, label_column):
        """
               Method Name: separate_label_feature
               Description: This method separates the independent features and dependent feature.
               Output: Returns two separate Dataframes, one containing independent features and the other containing dependent feature .
               On Failure: Raise Exception
        """
        self.logger.log(self.file, 'Entered the separate_label_feature method of the Preprocessor class')

        try:
            self.X =data.drop(labels = label_column, axis = 'columns')
            self.Y = data[label_column]
            self.logger.log(self.file,'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return self.X, self.Y

        except Exception as e:
            self.logger.log(self.file,f'Exception occured in separate_label_feature method of the Preprocessor class. Exception message: {e}')
            self.logger.log(self.file, 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise Exception()



    def is_null_present(self,data):
        """
              Method Name: is_null_present
              Description: This method checks whether there are null values present in the pandas Dataframe or not.
              Output: Returns True if null values are present in the DataFrame, False if they are not present and
                        returns the list of columns for which null values are present.
              On Failure: Raise Exception
        """
        #self.logger.log(self.file, 'Entered the is_null_present method of the Preprocessor class')
        self.null_present = False
        self.columns_with_missing_values = []
        self.cols = data.columns
        self.folder = 'Null_Values_In_Dataframe'

        try:
            self.null_counts = data.isna().sum()
            for i in range(len(self.null_counts)):
                if self.null_counts[i]>0:
                    self.null_present = True
                    self.columns_with_missing_values.append(self.cols[i])
            if (self.null_present):  # write the logs to see which columns have null values
                self.dataframe_with_null = pd.DataFrame()
                self.dataframe_with_null['columns'] = data.columns
                self.dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())

                if not os.path.isdir(self.folder):
                    os.makedirs(self.folder)
                    self.dataframe_with_null.to_csv(self.folder + '/' + 'null_values.csv', index=False)  # storing the null column information to file
                else:
                    self.dataframe_with_null.to_csv(self.folder + '/' + 'null_values.csv', index=False) # storing the null column information to file

            #self.logger.log(self.file,'Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return self.null_present, self.columns_with_missing_values


        except Exception as e:
            self.logger.log(self.file,f'Exception occured in is_null_present method of the Preprocessor class. Exception message: {e}')
            self.logger.log(self.file,'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise Exception()



    def onehot_encoder(self, data, column_name):
        """
             Method Name: column_value_into_minutes
             Description: This method does the conversion of time into minutes of specified column
             Output: Nil
        """

        try:
            encoder = OneHotEncoder(dtype=int, handle_unknown='ignore')
            col_transformer = ColumnTransformer([('OHE', encoder, column_name)], remainder='passthrough')
            data = col_transformer.fit_transform(data)
            #self.logger.log(self.file, "Label Encoding of the values are successful....!!")

            return data

        except Exception as e:
            self.logger.log(self.file, f"Error occurred while label encoding {e}")



    def impute_missing_values(self, data):
        """
                Method Name: impute_missing_values
                Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
                Output: A Dataframe which has all the missing values imputed.
                On Failure: Raise Exception
        """

        self.logger.log(self.file, 'Entered the impute_missing_values method of the Preprocessor class')
        self.data= data
        try:
            imputer=KNNImputer(n_neighbors=3, weights='uniform',missing_values=np.nan)
            self.new_array=imputer.fit_transform(self.data) # impute the missing values
            # convert the nd-array returned in the step above to a Dataframe
            self.new_data=pd.DataFrame(data=(self.new_array), columns=self.data.columns)
            self.logger.log(self.file, 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
            return self.new_data
        except Exception as e:
            self.logger.log(self.file,'Exception occured in impute_missing_values method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger.log(self.file,'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
            raise Exception()



    def get_columns_with_zero_std_deviation(self,data):
        """
                                                Method Name: get_columns_with_zero_std_deviation
                                                Description: This method finds out the columns which have a standard deviation of zero.
                                                Output: List of the columns with standard deviation of zero
                                                On Failure: Raise Exception

                                                Written By: iNeuron Intelligence
                                                Version: 1.0
                                                Revisions: None
                             """
        self.logger.log(self.file, 'Entered the get_columns_with_zero_std_deviation method of the Preprocessor class')
        self.columns=data.columns
        self.data_n = data.describe()
        self.col_to_drop=[]
        try:
            for x in self.col_to_drop:
                if (self.data_n[x]['std'] == 0): # check if standard deviation is zero
                    self.col_to_drop.append(x)  # prepare the list of columns with standard deviation zero
            self.logger.log(self.file, 'Column search for Standard Deviation of Zero Successful. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            return self.col_to_drop

        except Exception as e:
            self.logger.log(self.file,'Exception occured in get_columns_with_zero_std_deviation method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger.log(self.file, 'Column search for Standard Deviation of Zero Failed. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            raise Exception()









