import pandas as pd
from Src.Prediction_Data_Validation import PredictionDataValidation
from Src.Prediction_Data_Preprocessing import PredDataPreprocessor
from Src.File_Methods import File_Operation
from Src.Clustering import KMeansClustering
from Src.Read_Yaml import read_params
from Src.Logging import AppLogger
import os
from pickle import load



class Prediction:

    def __init__(self, path):
        self.schema = read_params('params.yaml')
        self.path = path
        self.file = open(self.schema['logs']['log_dir_prediction'] + "/Prediction_Log.txt", 'a+')

        self.logger = AppLogger()
        if path is not None:
            self.prediction_data_val = PredictionDataValidation(path)


    def prediction_from_model(self):

        try:
            self.prediction_data_val.delete_prediction_file() #deletes the existing prediction file from last run!
            self.logger.log(self.file, "Starting prediction...!!")
            data = pd.read_csv(self.schema['test_data']['final_test_data'])


            prepocessor = PredDataPreprocessor(self.file, self.logger)
            is_null_present = prepocessor.is_null_present(data)


            # if missing values are there, replace them appropriately.
            # kNNImputer works only for numeric variables, So if any columns is in categorical format need to be mapped to numeric
            # if (is_null_values_present) :
                      # data = preprocessor.impute_missing_values(data)


            ################################# PREPROCESSING AND FEATURE ENGINEERING ####################################

            preprocessor = PredDataPreprocessor(self.file, self.logger)

            # check if missing values are present in the dataset
            is_null_values_present, cols_with_missing_values = preprocessor.is_null_present(data)


            # Get the columns with constant values
            columns_to_drop_with_constant_values = preprocessor.get_columns_with_zero_std_deviation(data)

            if bool(columns_to_drop_with_constant_values):
                data = data.drop(columns_to_drop_with_constant_values, axis='columns')

            # Dropping the rows which have NaN
            data = preprocessor.drops_rows_with_nan(data)


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
            scaler = load(open(self.schema['transformation_pkl']['one_hot_encoder'], 'rb'))
            test_x = scaler.transform(data)

            ###################################### APPLYING CLUSTERING ###########################################

            file_loader = File_Operation(self.file, self.logger)
            kmeans = file_loader.load_cluster_model('KMeans')

            clusters = kmeans.predict(test_x)


            preprocessed_testing_data = pd.DataFrame(test_x)
            preprocessed_testing_data['cluster'] = clusters
            list_of_cluster = preprocessed_testing_data['cluster'].unique()

            ####### parsing all the clusters and looking for the best ML algorithm to fit on individual cluster ########
            pred_dataframe = pd.DataFrame()
            for cluster in list_of_cluster:
                cluster_data = preprocessed_testing_data[preprocessed_testing_data['cluster'] == cluster]
                # cluster_data = cluster_data.reset_index(drop= True)

                # Prepare the feature and Label columns
                cluster_data = cluster_data.drop('cluster', axis=1)

                file_ops = File_Operation(self.file, self.logger)

                model_name = file_ops.find_correct_model(cluster_number= cluster)

                model = file_ops.load_model(model_name)

                result = list(model.predict(cluster_data))
                pred_dataframe = pred_dataframe.append(result)


                self.logger.log(self.file, 'End of Prediction')

            return pred_dataframe.to_csv(self.schema['test_data']['prediction_output'], index=False)




        except Exception as ex:
            self.logger.log(self.logger, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex


