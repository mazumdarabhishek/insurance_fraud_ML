import pandas as pd


class dataGetter:


    def __init__(self,file_object,logger_object):
        self.training_file = "Training_Data_to_csv/InputFile.csv"
        self.logger = logger_object
        self.file = file_object


    def getData(self):


        self.logger.log(self.file,"Entered the getData method of dataGetter class")
        try:
            self.data = pd.read_csv(self.training_file)
            self.logger.log(self.file,"Data Loading successful")
            return self.data
        except Exception as e:
            self.logger.log(self.file,"Error occured during loading Training_Batch_Files at getData():: %s"%e)
            raise Exception
