from wsgiref import simple_server
from flask import Flask, request, render_template
from flask import Response
import os
from flask_cors import CORS, cross_origin
from prediction_validation_insertion import PredictionValidation
from training_model import TrainModel
from training_validation_insertion import TrainValidation
import flask_monitoringdashboard as dashboard
from predictFromModel import Prediction
import json


os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route("/", methods = ['GET'])
@cross_origin()
def home():
    return "Welcome to home page"


@app.route("/train", methods = ['GET', 'POST'])
@cross_origin()
def train_route_client():

    try:
        train_val_obj = TrainValidation('Training_Batch_Files/')
        train_val_obj.train_validation()

        train_model_obj = TrainModel()
        train_model_obj.training_model()

        return Response("Training Successful")

    except Exception as e:
        return f"Error occurred while training: {e}"

@app.route("/predict", methods = ['GET', 'POST'])
@cross_origin()
def predict_route_client():

    try:
        print('Inside Predict method')
        pred_val_obj = PredictionValidation('Prediction_Batch_Files/')
        pred_val_obj.prediction_validation()

        predict = Prediction('Prediction_FileFromDB/')
        predict.prediction_from_model()

        return Response("Prediction File created at Prediction_Output_File folder!!!")

    except Exception as e:
        return f"Error occurred while training: {e}"





if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    httpd = simple_server.make_server(host= '0.0.0.0', app = app, port = port)
    httpd.serve_forever()
    app.run()















