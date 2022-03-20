"""
This is the Entry point for Training the Machine Learning Model.
"""

from sklearn.model_selection import train_test_split
from Data_Ingestion.data_loader import DataGetter
from Data_Preprocessing.preprocessing import Preprocessor
from File_Operation.file_methods import File_Opeartion
