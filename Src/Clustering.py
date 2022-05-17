import matplotlib.pyplot as plt
import numpy as np
from sklearn.cluster import KMeans
from kneed import KneeLocator
from Src.Read_Yaml import read_params
from Src.File_Methods import File_Operation
from pickle import dump

class KMeansClustering:
    """
          This class shall  be used to divide the data into clusters before training.
    """

    def __init__(self, file, logger):
        self.file = file
        self.logger = logger
        self.schema = read_params('params.yaml')




    def elbow_plot(self, data):
        """
               Method Name: elbow_plot
               Description: This method saves the plot to decide the optimum number of clusters to the file.
               Output: A picture saved to the directory
               On Failure: Raise Exception
        """
        self.logger.log(self.file, 'Entered the elbow_plot method of the KMeansClustering class')
        wcss = []

        try:
            for i in range(1,11):
                kmeans = KMeans(n_clusters=i, init='k-means++', random_state=42)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)
            plt.plot(range(1, 11), wcss)  # creating the graph between WCSS and the number of clusters
            plt.title('The Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            # plt.show()
            plt.savefig(self.schema['logs']['KMeans_elbowplot'])
            # finding the value of the optimum cluster programmatically
            self.kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            self.logger.log(self.file, f'The optimum number of clusters is: {self.kn.knee}. Exited the elbow_plot method of the KMeansClustering class')
            return self.kn.knee

        except Exception as e:
            self.logger.log(self.file,f'Exception occured in elbow_plot method of the KMeansClustering class. Exception message:{e}')
            self.logger.log(self.file,'Finding the number of clusters failed. Exited the elbow_plot method of the KMeansClustering class')
            raise Exception()



    def create_clusters(self, data, number_of_clusters, name_to_the_model):
        """
                Method Name: create_clusters
                Description: Create a new dataframe consisting of the cluster information.
                Output: A dataframe with cluster column
                On Failure: Raise Exception
        """

        self.logger.log(self.file, 'Entered the create_clusters method of the KMeansClustering class')
        self.data = data

        try:
            self.kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', random_state=42)
            transformer = self.kmeans.fit(data)
            dump(self.kmeans, open(self.schema['transformation_pkl']['cluster'], 'wb'))
            self.y_kmeans = transformer.fit_predict(data)  # divide data into clusters

            self.file_oper = File_Operation(self.file, self.logger)
            self.saving_model = self.file_oper.save_cluster_model(self.kmeans, name_to_the_model)

            p = self.y_kmeans.reshape(self.data.shape[0],1)
            self.data = np.column_stack((self.data, p))
            self.logger.log(self.file, f'succesfully created {self.kn.knee} clusters. Exited the create_clusters method of the KMeansClustering class')

            return self.data

        except Exception as e:
            self.logger.log(self.file, f'Exception occured in create_clusters method of the KMeansClustering class. Exception message: {e}')
            self.logger.log(self.file, 'Fitting the data to clusters failed. Exited the create_clusters method of the KMeansClustering class')
            raise Exception()

