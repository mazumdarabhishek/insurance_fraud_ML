from Data_Preprocessing import DataTransformation,DataPreprocessing,DataClustering
from App_logging.logger import App_logger
from Best_Model_Finder import tunner
from Data_Validation import RawData_Validation_Training
from Data_Db_Insertsion import DBOps



class trainValidation:
    def __init__(self,path):
        self.rawData = RawData_Validation_Training.Raw_Data_Validation(path)
        self.dataTransform = DataTransformation.dataTransformer()
        self.dBOps = DBOps.DBOperations()
        self.logFile = open("Training_logs/Training_Main_Logs.txt","a+")
        self.logger = App_logger()


    def validationOfTrainingData(self):

        try:
            self.logger.log(self.logFile,"Validation of Training Data initiated")

            # extract information from schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, ColumnCount = self.rawData.getValuesFromSchema()

            #get the REGEX for file name
            regex = self.rawData.manualRegexCreateion()

            #validate file name
            self.rawData.validateFileName(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)

            # Validate Column Count
            self.rawData.validateColumnCount(ColumnCount)

            self.logger.log(self.logFile,"Raw Data validation Complete. Starting Training_Batch_Files Transformation")

            #transform Training_Batch_Files imperfetections
            self.dataTransform.corectStringFormat()

            self.logger.log(self.logFile,"Data Trabsformation Complete")

            self.dBOps.createTableinDB("Training",column_names)
            self.dBOps.insertGoodDataToTable("Training")
            self.logger.log(self.logFile,"Table Created and Data Insertion is Complete")


            self.rawData.daletExistingGoodBadData()

            self.rawData.archiveBadFiles()

            self.dBOps.exportDataFromDBtoCSV("Training")
            self.logFile.close()
        except Exception as e:

            self.logFile = open("Training_logs/Training_Main_Logs.txt","a+")
            self.logger.log(self.logFile,"Error Occured in Main Traing Validation steps, check sub-logs for info")
            self.logFile.close()
            raise Exception




