import shutil
from datetime import datetime
from os import listdir
import os
import csv
from application_logger.logging import AppLogger
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class dbOperation:
    """
          This class shall be used for handling all the Cassandra DB operations.
    """

    def __init__(self):
        self.path ='Training_Database/'
        self.badFilePath = "Training_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_files_validated/Good_Raw"
        self.logger = AppLogger()


    def database_connection(self, database_name):
        """
              Method Name: database_connection
              Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
              Output: Connection to the DB
              On Failure: Raise ConnectionError
        """

        try:
            cloud_config = {
                'secure_connect_bundle': r'C:\Users\Sudheesh E\Dropbox\PC\Downloads\secure-connect-flight-price-prediction.zip'
            }
            auth_provider = PlainTextAuthProvider('caqhALHQBBIUIZWptJlnouhX',
                                                  '9M86beObXwhCR+_L,FSbYX4ZeF_nJ39.pjmDtn8Za-N3L9P+kJwrDxhP4MPZ_a4hRy3Z2FEnxRCI_5SNIJZPm6eJhAvDFG-7F-ZeqPGiqk-BCy+xOr1mZf043J4boBhS')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

        except Exception as e:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, f"Error while connecting to database: {e}")
            file.close()
            raise ConnectionError
        return session


    def create_table_in_db(self, database_name, column_names):
        """
                Method Name: createTableDb
                Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
                Output: None
                On Failure: Raise Exception
        """

        session = self.database_connection(database_name)
