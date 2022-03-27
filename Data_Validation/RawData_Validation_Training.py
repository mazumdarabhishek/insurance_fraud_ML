"""Description: This module will be used to validate the Raw Training_Batch_Files coming from the client side in different steps.
    This is in accordance to the Data Sharing Agreement and its Master Data Management shcema confirmed by the client. 
    It is important to check the Incoming file name schema and its containing columns In order to avoid apllication crash
    due to bad Training_Batch_Files injestion to the AI pipelines. It is a serious event incase of productionised model.
"""
    
    

import json
import os
from datetime import datetime
import re
import shutil
from App_logging.logger import App_logger
import pandas as pd


class Raw_Data_Validation:

    def __init__(self,path):
        self.logger = App_logger() #creating a logger object
        self.schema_path = 'schema/schema_train.json' #getting the schema path
        self.Training_Batch_Dir = path #geting the raw Training_Batch_Files path
        
        
    
    def getValuesFromSchema(self):
        """Description: Gets values from TrainingData_Schema.json and returns them 
            Error: Error Handling and Logging
            Returns: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
            
            Written By: Abhishek Mazumdar
            Version: 1.0
            Revision: None
        """
        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColNames']
            NumberofColumns = dic['NumberofColumns']

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file,message)

            file.close()



        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns

    def manualRegexCreateion(self):
        """
            Description: Returns a regex syntax to check tge filename schema
            Output: Regex String
            Errors: None
            Written By: Abhishek Mazumdar
            Version: 1.0
            Revision: None
        """
        regex = "['fraudDetection']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def validateFileName(self,regex, LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
            Description:Validates file name schema using the reger from manualRegexCreation
            Errors: Exception Handling
            Written By: Abhisek Mazumdar
            Version: 1.0
            Revision: None
        """

        self.daletExistingGoodBadData()#sanity check before working on new batch.
        #create new directories for good and bad Training_Batch_Files
        self.createDirForGoodBadRadData()
        available_files = [f for f in os.listdir(self.Training_Batch_Dir)]
        try:
            log_file = open("Training_logs/validateFileName_logs.txt","a+")
            for fname in available_files:
                if re.match(regex,fname):
                    #separate the sub units of the file name to check
                    splitAtDot = re.split(".csv",fname)
                    splitatUnderScore = re.split("_",splitAtDot[0])
                    if len(splitatUnderScore[1])== LengthOfDateStampInFile:
                        if len(splitatUnderScore[2])==LengthOfTimeStampInFile:
                            #copy the good file to the Good_Raw/
                            shutil.copy("Training_Batch_Files/" + fname,"Validated_Raw_Files_Training/Good_Raw")
                            self.logger.log(log_file,"Valid file name, moved to Good_Raw:: %s"%fname)
                        else:
                            shutil.copy("Training_Batch_Files/"+fname,"Validated_Raw_Files_Training/Bad_Raw")
                            self.logger.log(log_file,"Invalid File Name, moved to Bad_Raw::%s"%fname)
                    else:
                        shutil.copy("Training_Batch_Files/" + fname, "Validated_Raw_Files_Training/Bad_Raw")
                        self.logger.log(log_file, "Invalid File Name, moved to Bad_Raw::%s" % fname)
                else:
                    shutil.copy("Training_Batch_Files/" + fname, "Validated_Raw_Files_Training/Bad_Raw")
                    self.logger.log(log_file, "Invalid File Name, moved to Bad_Raw::%s" % fname)
                log_file.close()
        except Exception as e:
            log_file = open("Training_logs/validateFileName_logs.txt","a+")
            self.logger.log(log_file,"Error occured while validatating file schema and moving to required folders::\t%s"%e)
            log_file.close()

    def validateColumnCount(self,NumberOfColumns):
        """
            Description: This will validate for appropriate number of columns present in each file or not. If
            not, then it will mode the file to BAD_RAW.

        """
        #logger instance createion
        try:
            source = "Validated_Raw_Files_Training/Good_Raw"
            log_file = open("Training_logs/columnValidation_logs.txt","a+")
            self.logger.log(log_file,"Column Validation Initiated !")
            for file in os.listdir(source):
                df = pd.read_csv(source+"/"+file)
                if df.shape[1] == NumberOfColumns:
                    pass
                else:
                    shutil.move(src=source+file,dst="Validated_Raw_Files_Training/Bad_Raw")
                    self.logger.log(log_file,"Bad Column count detected,moved to Bad_Raw")
            self.logger.log(log_file,"Column Validation complete!")
            log_file.close()
        except Exception as e:
            log_file = open("Training_logs/columnValidation_logs.txt", "a+")
            self.logger.log(log_file,"Error occured while Validating Column count ::%s"%e)
            log_file.close()

    def checkEmptyColumn(self):
        """
            Description: Checks for any empty row. IF there is any, then moves the file to Bad_Raw
        """
        try:
            log_file = open("Training_logs/checkEmptyColumn_logs.txt","a+")
            self.logger.log(log_file,"Empty Rwo check initiated !")
            source = "Validated_Raw_Files_Training/Good_Raw"
            for file in os.listdir(source):
                df = pd.read_csv(source+file)
                count = 0
                for col in df.columns:
                    if (len(df[col]) - df[col].count()) == len(df[col]):
                        count += 1
                        shutil.move(src=source+file,dst="Validated_Raw_Files_Training/Bad_Raw")
                        self.logger.log(log_file,"Empty Column Detected,moved to Bad_Raw ::%s"%file)
                        break
            self.logger.log(log_file,"Empty Column check complete !")
            log_file.close()


        except OSError:
            f = open("Training_logs/checkEmptyColumn_logs.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError

        except Exception as e:
            f = open("Training_logs/checkEmptyColumn_logs.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e 

    def createDirForGoodBadRadData(self):
        """
        write description
        """
        try:
            path = os.path.join("Validated_Raw_Files_Training/","Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Validated_Raw_Files_Training/","Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            file = open("Training_logs/General_logs.txt", 'a+')
            self.logger.log(file,"God_raw & Bad_Raw directories are created successfully!")
            file.close()
        except OSError as er_os:
            file = open("Training_logs/General_logs.txt",'a+')
            self.logger.log(file,"Error occured during creating Good,Bad Directories: %s"%er_os)
            file.close()
            raise OSError

    def daletExistingGoodBadData(self):

        path = "Validated_Raw_Files_Training/"
        #checking for avaiable dir called Good_Raw, if so deleting all content in it
        try:
            if os.path.isdir(path+"Good_Raw/"):
                shutil.rmtree(path+"Good_Raw/")
                file = open("Training_logs/General_logs.txt",'a+')
                self.logger.log(file,"Deleted existing files in Good_Raw Successfully!")
                file.close()
            #Similarly cleaning Bad_Raw dir
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Training_logs/General_logs.txt", 'a+')
                self.logger.log(file, "Deleted existing files in Bad_Raw Successfully!")
                file.close()
        except Exception as e:
            file = open("Training_logs/General_logs.txt", 'a+')
            self.logger.log(file, "Error occured while deleting Training_Batch_Files from good,bad dir: %s"%e)
            file.close()

    def archiveBadFiles(self):
        now = datetime.now()
        time = now.strftime("%H:%M:%S")
        date = now.date()

        source = "Validated_Raw_Files_Training/"
        try:
            if os.path.isdir(source):
                path = "ArchivedBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = "ArchivedBadData/BadData_"+str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)

                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(src=source+f,dst=dest)
                file = open("Training_logs/archiveBadFiles_logs.txt",'a+')
                self.logger.log(file,"Bar_Raw files successfully Archived!")
                file.close()
            else:
                for file in os.listdir(source + "Bad_Raw/"):
                    shutil.move(src=source + "Bad_Raw/"+file,dst=source +"ArchivedBadData/")
                file = open("Training_logs/archiveBadFiles_logs.txt", 'a+')
                self.logger.log(file, "Bar_Raw files successfully Archived!")
                file.close()
        except Exception as e:
            file = open("Training_logs/archiveBadFiles_logs.txt", 'a+')
            self.logger.log(file,"Error occured while archiving Bad_Raw :%s"%e)
            file.close()

    
    
        


    
        
