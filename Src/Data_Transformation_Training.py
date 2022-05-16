from datetime import datetime
from os import listdir
from Src.Logging import AppLogger


class DataTransform:
    """
          This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.
    """

    def __init__(self):
        self.goodDataPath = "Training_Raw_files_validated/Good_Raw"
        self.logger = AppLogger()