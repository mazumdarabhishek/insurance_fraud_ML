
import Validate_Train_Insert
from TrainingModel import TrainModels
from wsgiref import simple_server

from flask import Flask, render_template,request,send_file
from flask import Response
import os
from flask_cors import CORS, cross_origin

from Validate_Predict_Insert import pred_validation
import flask_monitoringdashboard as dashboard
from PredictingModel.MakePredictions import prediction
from  App_logging.logger import App_logger


app = Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route('/')
@cross_origin()
def index():
   return render_template('index.html')


def TrainRoute():
    try:
        path = "Training_Batch_Files/"
        valid_object = Validate_Train_Insert.trainValidation(path)
        valid_object.validationOfTrainingData()
        train_the_models = TrainModels.TrainModel()
        train_the_models.trainingModel()

    except Exception as e:
        print("Error occured in app.py :: %s"%e)


@app.route('/predict',methods=['POST'])
@cross_origin()
def PredictRoute():
    path = "Prediction_Batch_files"

    pred_val = pred_validation(path)  # object initialization

    pred_val.prediction_validation()  # calling the prediction_validation function

    pred = prediction(path)  # object initialization

    # predicting for dataset present in database
    pred.predictionFromModel()
    if os.listdir('Prediction_Output_File/') == 0:
        return Response("Prediction Failed Due to invalid Upload. Check Prediction Schema Documentation")
    else:
        return render_template('results.html')



@app.route("/upload",methods=['POST'])
def uploadFiles():
    uploaded_file = request.files['file']
    if uploaded_file.filename != '':
        file_path = os.path.join("Prediction_Batch_files",uploaded_file.filename)
        uploaded_file.save(file_path)
    return render_template('predict.html')




path = "Prediction_Output_File/Predictions.csv"

@app.route("/download")
@cross_origin()
def downloadcsv():
    try:
        path = "Prediction_Output_File/Predictions.csv"
        return send_file(path,as_attachment=True)
    except Exception as e:
        raise e



if __name__ == "__main__":
    #AWS hosts app on port 8080 by default
    #app.run(host="0.0.0.0",port=8080)
    app.run(port=5000)
    #PredictRoute()











