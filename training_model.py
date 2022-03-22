"""
This is the Entry point for Training the Machine Learning Model.
"""

from sklearn.model_selection import train_test_split
from Data_Ingestion.data_loader import DataGetter
from Data_Preprocessing.preprocessing import Preprocessor
from File_Operation.file_methods import File_Operation
from Data_Preprocessing.clustering import KMeansClustering
from Best_Model_Finder.tuner import ModelFinder
from application_logger.logging import AppLogger
import pandas as pd


class TrainModel:
    """
    This class performs the training of the model
    """

    def __init__(self):
        self.logger = AppLogger()
        self.file = open("Training_Logs/ModelTrainingLog.txt", 'a+')


    def training_model(self):

        """
             Method Name: training_model
             Description: This method does Finds and Save the model file to directory
             Outcome: Nil
             On Failure: Raise Exception
        """
        self.logger.log(self.file, 'Start of Training......!!!')

        try:
            # Getting the data from the source
            data_getter = DataGetter(self.file, self.logger)
            data =  data_getter.get_data()


            ################################# PREPROCESSING AND FEATURE ENGINEERING ####################################

            preprocessor = Preprocessor(self.file, self.logger)

            # check if missing values are present in the dataset
            is_null_values_present, cols_with_missing_values = preprocessor.is_null_present(data)

            # Get the columns with constant values
            columns_to_drop_with_constant_values = preprocessor.get_columns_with_zero_std_deviation(data)

            if bool(columns_to_drop_with_constant_values):
                data = data.drop(columns_to_drop_with_constant_values, axis = 'columns')

            #Dropping the rows which have NaN
            data = preprocessor.drops_rows_with_nan(data)

            #removing a row with Duration as '5min'
            data = preprocessor.remove_row(data, '5m')

            # removing rows with have minimum occurrence
            data = preprocessor.drop_rows_based_on_value_count(data, 'Airline', '15')
            data = preprocessor.drop_rows_based_on_value_count(data, 'Additional_Info', '20')
            data = preprocessor.drop_rows_based_on_value_count(data, 'Total_Stops', '50')

            # Merging values in Additional_Info & Destination columns
            data = preprocessor.merging_values(data, 'Additional_Info', 'No Info', 'No info')
            data = preprocessor.merging_values(data, 'Destination', 'Delhi', 'New Delhi')

            # Mapping Values in Total_Stops column
            data = preprocessor.mapping(data, 'Total_Stops')

            # Converting 'Date_of_Journey' column to datetime
            data = preprocessor.converting_to_datetime(data,'Date_of_Journey')

            # Converting Dep_Time & Arrival_Time columns to parts of day
            data = preprocessor.convert_column_to_part_of_day(data, 'Dep_Time')
            data = preprocessor.convert_column_to_part_of_day(data, 'Arrival_Time')

            # Converting 'Duration' column into minutes
            data = preprocessor.column_value_into_minutes(data, 'Duration')

            # Dropping Route and ID columns
            data = preprocessor.drop_column(data, ['Route', 'ID'])
            data = data.reset_index(drop = True)

            # Separating Features and label
            train_x, train_y = preprocessor.separate_label_feature(data, 'Price')

            # Encoding categorical variables using Onehot Encoding Technique
            train_x = preprocessor.onehot_encoder(train_x, ['Airline', 'Source', 'Destination', 'Arrival_Time', 'Dep_Time', 'Additional_Info'])



            # if missing values are there, replace them appropriately.
            # kNNImputer works only for numeric variables, So if any columns is in categorical format need to be mapped to numeric
            #if (is_null_values_present) :
                #data = preprocessor.impute_missing_values(data)



            ###################################### APPLYING CLUSTERING ###########################################

            kmeans = KMeansClustering(self.file, self.logger)
            number_of_clusters = kmeans.elbow_plot(train_x)

            # Dividing the data into clusters
            train_x = kmeans.create_clusters(train_x, number_of_clusters)
            preprocessed_training_data = pd.DataFrame(train_x)
            preprocessed_training_data = preprocessed_training_data.rename(columns={34: "Cluster"})

            preprocessed_training_data['Price'] = train_y

            list_of_cluster = preprocessed_training_data['Cluster'].unique()


            ####### parsing all the clusters and looking for the best ML algorithm to fit on individual cluster ########

            for cluster in list_of_cluster:

                cluster_data = preprocessed_training_data[preprocessed_training_data['Cluster'] == cluster]
                #cluster_data = cluster_data.reset_index(drop= True)

                # Prepare the feature and Label columns
                cluster_features = cluster_data.drop(['Price', 'Cluster'], axis=1)
                cluster_label = cluster_data['Price']

                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size= .30, random_state=369)

                model_finder = ModelFinder(self.file, self.logger)  # object initialization

                # getting the best model for each of the clusters
                best_model_name, best_model, error_value = model_finder.get_best_model(x_train, y_train, x_test, y_test)

                print(best_model_name)
                print(best_model)
                print(error_value)

                # saving the best model to the directory.
                file_op = File_Operation(self.file, self.logger)
                save_model = file_op.save_model(best_model, best_model_name + str(cluster))

                # logging the successful Training
            self.logger.log(self.file, 'Successful End of Training')
            self.file.close()


        except Exception as e:
            # logging the unsuccessful Training
            self.logger.log(self.file, 'Unsuccessful End of Training')
            self.logger.log(self.file, f'Error while training the model {e}')
            self.file.close()
            raise Exception






