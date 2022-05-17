import pandas as pd
from Src.Logging import AppLogger
from Src.Read_Yaml import read_params

class DataGetter:
    """
        This class shall  be used for obtaining the data from the source for training.
    """

    def __init__(self, file, logger):
        self.schema = read_params('params.yaml')
        self.training_file = self.schema['load_data']['data_from_db'] + '/InputFile.csv'
        self.file = file
        self.logger = logger


    def get_data(self):

        """
              Method Name: get_data
              Description: This method reads the data from source.
              Output: A pandas DataFrame.
              On Failure: Raise Exception
        """
        #self.logger.log(self.file, 'Entered the get_data method of the DataGetter class')

        try:
            data = pd.read_csv(self.training_file) # reading the data file
            self.logger.log(self.file,'Data Load Successful.Exited the get_data method of the Data_Getter class')
            return data

        except Exception as e:
            self.logger.log(self.file,'Exception occured in get_data method of the Data_Getter class. Exception message: '+str(e))
            self.logger.log(self.file,'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise Exception()

