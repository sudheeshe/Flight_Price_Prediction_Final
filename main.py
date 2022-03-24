from training_validation_insertion import TrainValidation
from training_model import TrainModel
from prediction_validation_insertion import PredictionValidation
from predictFromModel import Prediction

### Training ####
#training_file_val = TrainValidation('Training_Batch_Files/')
#training_file_val.train_validation()
create_model = TrainModel()
create_model.training_model()

##### Prediction ####
#a = PredictionValidation('Prediction_Batch_Files/')
#a.prediction_validation()
#pred = Prediction('Prediction_FileFromDB/')
#pred.prediction_from_model()




