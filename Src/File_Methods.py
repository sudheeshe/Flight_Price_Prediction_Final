import pickle
import os
import shutil

class File_Operation:
    """
          This class shall be used to save the model after training and load the saved model for prediction.
    """

    def __init__(self, file, logger):
        self.logger = logger
        self.file = file
        self.model_directory = 'Available_Trained_Models/'
        self.model_directory_cluster = 'Available_Cluster_Models'


    def save_model(self, model, filename):
        """
             Method Name: save_model
             Description: Save the model file to directory
             Outcome: File gets saved
             On Failure: Raise Exception
        """
        self.logger.log(self.file, 'Entered the save_model method of the File_Operation class')
        try:
            path = os.path.join(self.model_directory, filename) #create seperate directory for each cluster
            if os.path.isdir(path): #remove the previousely existing models for each cluster
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path)
            pickle.dump(model, open(path + '/' + filename + '.sav', 'wb'))
            self.logger.log(self.file, f'Model File {filename} saved. Exited the save_model method of the Model_Finder class')
            return 'Sucessfully model saved....!!'

        except Exception as e:
            self.logger.log(self.file,f'Exception occured in save_model method of the Model_Finder class. Exception message: {e}')
            self.logger.log(self.file,f'Model File {filename} could not be saved. Exited the save_model method of the Model_Finder class')
            raise Exception()


    def save_cluster_model(self, model, filename):
        """
             Method Name: save_model
             Description: Save the model file to directory
             Outcome: File gets saved
             On Failure: Raise Exception
        """
        self.logger.log(self.file, 'Entered the save_model method of the File_Operation class')
        try:
            path = os.path.join(self.model_directory_cluster,filename) #create seperate directory for each cluster
            if os.path.isdir(path): #remove the previousely existing models for each cluster
                shutil.rmtree(self.model_directory_cluster)
                os.makedirs(path)
            else:
                os.makedirs(path)
            pickle.dump(model, open(path + '/' + filename + '.sav', 'wb'))
            self.logger.log(self.file, f'Model File {filename} saved. Exited the save_model method of the Model_Finder class')
            return 'Sucessfully model saved....!!'

        except Exception as e:
            self.logger.log(self.file,f'Exception occured in save_model method of the Model_Finder class. Exception message: {e}')
            self.logger.log(self.file,f'Model File {filename} could not be saved. Exited the save_model method of the Model_Finder class')
            raise Exception()


    def load_model(self, filename):

        """
               Method Name: load_model
               Description: Load the model file to memory
               Output: The Model file loaded in memory
               On Failure: Raise Exception
        """
        self.logger.log(self.file, 'Entered the load_model method of the File_Operation class')
        try:
            model = pickle.load(open(self.model_directory + filename + "/" + filename + '.sav', 'rb'))
            self.logger.log(self.file,f'Model File {filename} loaded. Exited the load_model method of the Model_Finder class')
            return model

        except Exception as e:
            self.logger.log(self.file, f'Exception occured in load_model method of the Model_Finder class. Exception message: {e}')
            self.logger.log(self.file, f'Model File {filename} could not be saved. Exited the load_model method of the Model_Finder class')
            raise Exception()

    def load_cluster_model(self, filename):

        """
               Method Name: load_model
               Description: Load the model file to memory
               Output: The Model file loaded in memory
               On Failure: Raise Exception
        """
        self.logger.log(self.file, 'Entered the load_model method of the File_Operation class')
        try:
            model = pickle.load(open('Available_Cluster_Models/KMeans_Training/KMeans_Training.sav', 'rb'))
            self.logger.log(self.file,f'Model File {filename} loaded. Exited the load_model method of the Model_Finder class')
            return model

        except Exception as e:
            self.logger.log(self.file, f'Exception occured in load_model method of the Model_Finder class. Exception message: {e}')
            self.logger.log(self.file, f'Model File {filename} could not be saved. Exited the load_model method of the Model_Finder class')
            raise Exception()


    def find_correct_model(self, cluster_number):
        """
              Method Name: find_correct_model_file
              Description: Select the correct model based on cluster number
              Output: The Model file
              On Failure: Raise Exception
        """
        self.logger.log(self.file, 'Entered the find_correct_model_file method of the File_Operation class')
        try:
            self.cluster_number = cluster_number
            self.folder_name = self.model_directory
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)
            for self.f in self.list_of_files:
                try:
                    if (self.f.index(str(self.cluster_number)) != -1):
                        self.model_name = self.f
                except:
                    continue
            self.model_name = self.model_name.split('.')[0]
            self.logger.log(self.file,'Exited the find_correct_model_file method of the Model_Finder class.')
            return self.model_name

        except Exception as e:
            self.logger.log(self.file,f'Exception occured in find_correct_model_file method of the Model_Finder class. Exception message:{e}')
            self.logger.log(self.file,'Exited the find_correct_model_file method of the Model_Finder class with Failure')
            raise Exception()
