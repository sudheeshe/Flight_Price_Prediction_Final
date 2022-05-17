import shutil
from datetime import datetime
from os import listdir
import os
import csv
import pandas as pd
from Src.Logging import AppLogger
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from Src.Read_Yaml import read_params


class DBOperation:
    """
          This class shall be used for handling all the Cassandra DB operations.
    """

    def __init__(self):
        self.schema = read_params('params.yaml')
        #self.path = 'Training_Database/'
        self.badFilePath = self.schema['load_data']['validated_raw_data'] + "Bad_Raw"
        self.goodFilePath = self.schema['load_data']['validated_raw_data'] + "Good_Raw"
        #self.database_name = self.schema['data_base_info']['database_name']
        self.keyspace = self.schema['data_base_info']['keyspace_name']
        self.table_name = self.schema['data_base_info']['table_name']
        self.logger = AppLogger()

    def database_connection(self):
        """
              Method Name: database_connection
              Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
              Output: Connection to the DB
              On Failure: Raise ConnectionError
        """

        try:
            cloud_config = {
                'secure_connect_bundle': self.schema['data_base_info']['secure_connect_bundle']
            }
            #auth_provider = PlainTextAuthProvider('caqhALHQBBIUIZWptJlnouhX', '9M86beObXwhCR+_L,FSbYX4ZeF_nJ39.pjmDtn8Za-N3L9P+kJwrDxhP4MPZ_a4hRy3Z2FEnxRCI_5SNIJZPm6eJhAvDFG-7F-ZeqPGiqk-BCy+xOr1mZf043J4boBhS')
            auth_provider = PlainTextAuthProvider(self.schema['data_base_info']['client_id'], self.schema['data_base_info']['auth_provider'])
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            row = session.execute("select release_version from system.local")

            file = open(self.schema['logs']['log_dir_training'] + "/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, f"Connection Established successfully, release_version  is {row[0]}")
            file.close()

        except Exception as e:
            file = open(self.schema['logs']['log_dir_training'] + "/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, f"Error while connecting to database: {e}")
            file.close()
            raise ConnectionError

        return session

    def create_table_in_db(self):
        """
                Method Name: createTableDb
                Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
                Output: None
                On Failure: Raise Exception
        """
        try:

            session = self.database_connection()
            row = session.execute(f"USE {self.keyspace};")
            row = session.execute(f"DROP TABLE IF EXISTS {self.table_name};")
            # creating and additional column to make it as PRIMARY KEY
            row = session.execute(f"CREATE TABLE {self.table_name}(Airline VARCHAR, Date_of_Journey VARCHAR, Source VARCHAR, Destination VARCHAR, Route VARCHAR, Dep_Time VARCHAR, Arrival_Time VARCHAR, Duration VARCHAR, Total_Stops VARCHAR, Additional_Info VARCHAR, Price INT, ID INT, PRIMARY KEY(Airline, Date_of_Journey, Source, Destination, Route, Dep_Time, Arrival_Time, Duration, Total_Stops, Additional_Info, Price, ID));")

            # row = session.execute(f"CREATE TABLE {self.table_name} (ID VARCHAR PRIMARY KEY);")

            # for key in column_names.keys():
            # d_type = column_names[key]
            # if key == 'Price':
            # row = session.execute(f"ALTER TABLE {self.table_name} ADD ({key} VARCHAR);")
            # else:
            # row = session.execute(f"ALTER TABLE {self.table_name} ADD ({key} {d_type});")

            file = open(self.schema['logs']['log_dir_training'] + "/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Tables created successfully!!")
            file.close()

        except Exception as e:
            file = open(self.schema['logs']['log_dir_training'] + "/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, f"Error while creating table:{e}")
            file.close()
            raise e

    def insert_data_to_db_table(self):

        """
               Method Name: insertIntoTableGoodData
               Description: This method inserts the Good data files from the Good_Raw folder into the above created table.
               Output: None
               On Failure: Raise Exception
        """
        session = self.database_connection()

        goodFilePath = self.goodFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]

        file = open(self.schema['logs']['log_dir_training'] + "/DbInsertLog.txt", 'a+')

        try:
            for files in onlyfiles:
                data = pd.read_excel(self.goodFilePath + "/" + files, engine='openpyxl')
                # creating a new column 'ID' to the dataframe
                data['ID'] = pd.Series(range(0, len(data)))

                for h, i in data.iterrows():
                    try:
                        session.execute(f"INSERT INTO {self.keyspace}.{self.table_name}(Airline, Date_of_Journey, Source, Destination, Route, Dep_Time, Arrival_Time, Duration, Total_Stops, Additional_Info, Price, ID) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",[i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], int(i[10]), int(i[11])])

                    except Exception as e:
                        self.logger.log(file, f'Error occurred {e}')

            self.logger.log(file, "CSV Loaded successfully")
            file.close()

        except Exception as e:
            self.logger.log(file, f"Error while inserting data to DB {e}")
            file.close()

    def selecting_data_from_table_into_csv(self):
        """
              Method Name: selectingDatafromtableintocsv
              Description: This method exports the data in GoodData table as a CSV file. in a given location.
                            above created .
              Output: None
              On Failure: Raise Exception
        """

        self.filefrom_db = self.schema['load_data']['data_from_db']
        self.file_name = 'InputFile.csv'

        file = open(self.schema['logs']['log_dir_training'] + "/ExportToCsv.txt", 'a+')

        try:
            session = self.database_connection()
            results = session.execute(f'SELECT * FROM {self.keyspace}.{self.table_name}')

            col_names = ['Airline', 'Date_of_Journey', 'Source', 'Destination', 'Route', 'Dep_Time',
                         'Arrival_Time', 'Duration', 'Total_Stops', 'Additional_Info', 'Price', 'ID']



            # Make the CSV ouput directory
            if not os.path.isdir(self.filefrom_db):
                os.makedirs(self.filefrom_db)
            else:
                pass

            csv = pd.DataFrame(results, columns = col_names)
            csv.to_csv(self.filefrom_db + self.file_name, index= False)



            # Open CSV file for writing
            #csv_file = csv.writer(open(self.filefrom_db + self.fileName, 'w', newline=''), delimiter=',', lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the CSV file.
            #csv_file.writerow(col_names)
            #csv_file.writerow(results)

            self.logger.log(file, "File Exporting completed 'InputFile.csv created successfully")
            file.close()

        except Exception as e:
            self.logger.log(file, f"File exporting failed. Error :{e}")
            file.close()
