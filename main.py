from wsgiref import simple_server
from flask import Flask
from flask import Response
import os
from Src.Read_Yaml import read_params
from flask_cors import CORS, cross_origin
from Src.Prediction_Validation_Insertion import PredictionValidation
from Src.Training_Model import TrainModel
from Src.Train_model_MLFlow import TrainModel_MLFlow
from Src.Training_Validation_Insertion import TrainValidation
import flask_monitoringdashboard as dashboard
from Src.Predict_From_Model import Prediction



os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route("/", methods = ['GET'])
@cross_origin()
def home():
    return "Welcome to Flight Price Prediction home page"


@app.route("/train", methods = ['GET'])
@cross_origin()
def train_route_client():

    try:
        path = read_params('params.yaml')
        train_val_obj = TrainValidation(path['load_data']['raw_dataset_csv'])
        train_val_obj.train_validation()

        #train_model_obj = TrainModel()
        train_model_obj = TrainModel_MLFlow()
        train_model_obj.training_model()

        return Response("Training Successful")

    except Exception as e:
        return f"Error occurred while training: {e}"

@app.route("/predict", methods = ['GET', 'POST'])
@cross_origin()
def predict_route_client():

    try:
        path = read_params('params.yaml')
        print('Inside Predict method')
        pred_val_obj = PredictionValidation(path['test_data']['raw_test_dataset'])
        pred_val_obj.prediction_validation()

        predict = Prediction(path['test_data']['final_test_data'])
        predict.prediction_from_model()

        return Response("Prediction File created at Prediction_Output_File folder!!!")

    except Exception as e:
        return f"Error occurred while training: {e}"





port = int(os.getenv("PORT", 5000))

if __name__ == "__main__":
    host = '0.0.0.0'
    httpd = simple_server.make_server(host=host, port=port, app=app)
    httpd.serve_forever()
