import pandas as pd
from application_logger.logging import AppLogger

class DataGetter_Prediction:
    """
        This class shall  be used for obtaining the data from the source for training.
    """

    def __init__(self, file, logger):
        self.prediction_file = 'Prediction_FileFromDB\InputFile.csv' ## here added path directly to get_data method
        self.file = file
        self.logger = logger


    def get_data(self):
        """
              Method Name: get_data
              Description: This method reads the data from source.
              Output: A pandas DataFrame.
              On Failure: Raise Exception
        """
        self.logger.log(self.file, 'Entered the get_data method of the DataGetter class')

        try:
            self.data= pd.read_csv(self.prediction_file) # reading the data file
            self.logger.log(self.file,'Data Load Successful.Exited the get_data method of the DataGetter_Prediction class')
            return self.data

        except Exception as e:
            self.logger.log(self.file,'Exception occured in get_data method of the DataGetter_Prediction class. Exception message: '+str(e))
            self.logger.log(self.file,'Data Load Unsuccessful.Exited the get_data method of the DataGetter_Prediction class')
            raise Exception()

