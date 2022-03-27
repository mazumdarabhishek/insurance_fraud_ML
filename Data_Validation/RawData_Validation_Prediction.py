import sqlite3
from datetime import datetime
from os import  listdir
import os
import shutil
import re
import json
import pandas as pd
from App_logging.logger import App_logger


class Prediction_Data_Validation:
    """
        Description: This class Validated the schema and Prediction Data
        Written By: Abhishek Mazumdar

    """
    def __init__(self,path):
        self.Batch_Dir = path
        self.schema_Path = "schema/schema_prediction.json"
        self.logger = App_logger()

    def getValuesFromSchema(self):

        try:
            with open(self.schema_Path,"r") as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open("Prediction_Logs/getValuesFromSchema_logs.txt","a+")
            self.logger.log(file,"Schema fetched successfully")
            file.close()
            return LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,NumberofColumns

        except ValueError:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

    def manualRegexCreation(self):

        regex = "['fraudDetection']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        try:
            path = os.path.join("Validated_Prediction_Raw_File/","Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Validated_Prediction_Raw_File/","Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Good and Bad Data directory successfully created")
            file.close()

        except OSError as ex:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while creating Directory %s:" % ex)
            file.close()
            raise OSError

    def deleteExistingGoodDataTrainingFolder(self):

        path = "Validated_Prediction_Raw_File"
        try:
            if os.path.isdir(path + "Good_Raw/"):
                shutil.rmtree(path+ "Good_Raw/")
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as ex:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % s)
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):

        try:
            path = 'Validated_Prediction_Raw_File/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % s)
            file.close()
            raise OSError

    def moveBadFilesToArchiveBad(self):

        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            path = "PredictionArchivedBadData"
            if not os.path.isdir(path):
                os.makedirs(path)
            source = 'Validated_Prediction_Raw_File/Bad_Raw/'
            dest = 'PredictionArchivedBadData/BadData_' + str(date) + "_" + str(time)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files = os.listdir(source)
            for f in files:
                if f not in os.listdir(dest):
                    shutil.move(source + f, dest)
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Bad files moved to archive")
            path = 'Validated_Prediction_Raw_File/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
            self.logger.log(file, "Bad Raw Data Folder Deleted successfully!!")
            file.close()
        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise OSError

    def ValidationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):

        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        self.createDirectoryForGoodBadRawData()
        onlyfiles = [f for f in listdir(self.Batch_Dir)]
        try:
            f = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Prediction_Batch_files/" + filename, "Validated_Prediction_Raw_File/Good_Raw")
                            self.logger.log(f, "Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("Prediction_Batch_files/" + filename, "Validated_Prediction_Raw_File/Bad_Raw")
                            self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("Prediction_Batch_files/" + filename, "Validated_Prediction_Raw_File/Bad_Raw")
                        self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("Prediction_Batch_files/" + filename, "Validated_Prediction_Raw_File/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e

    def validateColumnLength(self,NumberofColumns):

        try:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Column Length Validation Started!!")
            for file in listdir('Validated_Prediction_Raw_File/Good_Raw/'):
                csv = pd.read_csv("Validated_Prediction_Raw_File/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns:
                    csv.to_csv("Validated_Prediction_Raw_File/Good_Raw/" + file, index=None, header=True)
                else:
                    shutil.move("Validated_Prediction_Raw_File/Good_Raw/" + file,
                                "Validated_Prediction_Raw_File/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)

            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e

        f.close()

    def deletePredictionFile(self):

        if os.path.exists('Prediction_Output_File/Predictions.csv'):
            os.remove('Prediction_Output_File/Predictions.csv')

    def validateMissingValuesInWholeColumn(self):

        try:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started!!")

            for file in listdir('Validated_Prediction_Raw_File/Good_Raw/'):
                csv = pd.read_csv("Validated_Prediction_Raw_File/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count += 1
                        shutil.move("Validated_Prediction_Raw_File/Good_Raw/" + file,
                                    "Validated_Prediction_Raw_File/Bad_Raw")
                        self.logger.log(f,
                                        "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count == 0:
                    csv.to_csv("Validated_Prediction_Raw_File/Good_Raw/" + file, index=None, header=True)
        except OSError:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()
