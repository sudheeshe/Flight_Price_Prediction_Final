import pandas as pd
from File_Operation.file_methods import File_Operation
from Data_Preprocessing.preprocessing import Preprocessor
from Data_Preprocessing.clustering import KMeansClustering
from Data_Ingestion.data_loader_prediction import DataGetter_Prediction
from application_logger.logging import AppLogger
from Prediction_Raw_Rata_Validation.predictionDataValidation import PredictionDataValidation
import os
from pickle import load



class Prediction:

    def __init__(self, path):
        self.path = path
        self.file = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.logger = AppLogger()
        if path is not None:
            self.prediction_data_val = PredictionDataValidation(path)


    def prediction_from_model(self):

        try:
            self.prediction_data_val.delete_prediction_file() #deletes the existing prediction file from last run!
            self.logger.log(self.file, "Starting prediction...!!")
            data_getter = DataGetter_Prediction(self.file, self.logger)
            data = data_getter.get_data()


            prepocessor = Preprocessor(self.file, self.logger)
            is_null_present = prepocessor.is_null_present(data)


            # if missing values are there, replace them appropriately.
            # kNNImputer works only for numeric variables, So if any columns is in categorical format need to be mapped to numeric
            # if (is_null_values_present) :
                      # data = preprocessor.impute_missing_values(data)


            ################################# PREPROCESSING AND FEATURE ENGINEERING ####################################

            preprocessor = Preprocessor(self.file, self.logger)

            # check if missing values are present in the dataset
            is_null_values_present, cols_with_missing_values = preprocessor.is_null_present(data)

            # Get the columns with constant values
            columns_to_drop_with_constant_values = preprocessor.get_columns_with_zero_std_deviation(data)

            if bool(columns_to_drop_with_constant_values):
                data = data.drop(columns_to_drop_with_constant_values, axis='columns')

            # Dropping the rows which have NaN
            data = preprocessor.drops_rows_with_nan(data)

            # removing a row with Duration as '5min'
            data = preprocessor.remove_row(data, '5m')

            # removing rows with have minimum occurrence
            data = preprocessor.rows_to_delete_in_prediction_file(data)

            # Merging values in Additional_Info & Destination columns
            data = preprocessor.merging_values(data, 'Additional_Info', 'No Info', 'No info')
            data = preprocessor.merging_values(data, 'Destination', 'Delhi', 'New Delhi')

            # Mapping Values in Total_Stops column
            data = preprocessor.mapping(data, 'Total_Stops')

            # Converting 'Date_of_Journey' column to datetime
            data = preprocessor.converting_to_datetime(data, 'Date_of_Journey')


            # Converting Dep_Time & Arrival_Time columns to parts of day
            data = preprocessor.convert_column_to_part_of_day(data, 'Dep_Time')
            data = preprocessor.convert_column_to_part_of_day(data, 'Arrival_Time')

            # Converting 'Duration' column into minutes
            data = preprocessor.column_value_into_minutes(data, 'Duration')

            # Dropping Route and ID columns
            data = preprocessor.drop_column(data, ['Route', 'ID'])
            data = data.reset_index(drop=True)


            # Encoding categorical variables using Onehot Encoding Technique
            scaler = load(open('Pickle_Files/onehot_encoder.pkl', 'rb'))
            test_x = scaler.transform(data)

            ###################################### APPLYING CLUSTERING ###########################################

            kmeans = KMeansClustering(self.file, self.logger)
            number_of_clusters = kmeans.elbow_plot(test_x)

            # Dividing the data into clusters
            test_x = kmeans.create_clusters(test_x, number_of_clusters, 'KMeans_TPrediction')
            preprocessed_testing_data = pd.DataFrame(test_x)
            preprocessed_testing_data = preprocessed_testing_data.rename(columns={34: "Cluster"})

            list_of_cluster = preprocessed_testing_data['Cluster'].unique()

            ####### parsing all the clusters and looking for the best ML algorithm to fit on individual cluster ########

            for cluster in list_of_cluster:
                cluster_data = preprocessed_testing_data[preprocessed_testing_data['Cluster'] == cluster]
                # cluster_data = cluster_data.reset_index(drop= True)

                # Prepare the feature and Label columns
                cluster_data = cluster_data.drop('Cluster', axis=1)

                file_ops = File_Operation(self.file, self.logger)

                model_name = file_ops.find_correct_model(cluster_number= cluster)

                model = file_ops.load_model(model_name)

                result = list(model.predict(cluster_data))
                result = pd.DataFrame(result)

                path = "Prediction_Output_File/Predictions.csv"
                result.to_csv("Prediction_Output_File/Predictions.csv",
                              header=False)  # appends result to prediction file
                self.logger.log(self.file, 'End of Prediction')




        except Exception as ex:
            self.logger.log(self.logger, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex


