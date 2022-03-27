"""
    This module carries out required transformation operation for the incoming raw Training_Batch_Files
"""

from datetime import datetime
from App_logging.logger import App_logger
from os import listdir
import pandas as pd

class dataTransformer:
    """
        Description: This class adds single quotes to the string type columns,  wherever its missing
    """

    def __init__(self):
        self.logger = App_logger()
        self.goodPath = "Validated_Raw_Files_Training/Good_Raw"

    def corectStringFormat(self):
        lg_file = open("Training_logs/DataTransformation_logs.txt", "a+")
        try:

            self.logger.log(lg_file,"Data Transformation Initiated !")
            for file in listdir(self.goodPath):
                data = pd.read_csv(self.goodPath+"/"+file)
                temp_columns = data.columns
                new_cols = []
                for i in temp_columns:
                    try:
                        new_cols.append(i.replace("-","_"))

                    except:
                        pass
                data.columns = new_cols
                columns = ["policy_bind_date", "policy_state", "policy_csl", "insured_sex", "insured_education_level",
                           "insured_occupation", "insured_hobbies", "insured_relationship", "incident_state",
                           "incident_date", "incident_type", "collision_type", "incident_severity",
                           "authorities_contacted", "incident_city", "incident_location", "property_damage",
                           "police_report_available", "auto_make", "auto_model", "fraud_reported"]
                for col in columns:
                    data[col] = data[col].apply(lambda x: "'" + str(x) + "'")
                data.to_csv(self.goodPath + "/" + file, index=None, header=True)
                self.logger.log(lg_file, " %s: Quotes added successfully!!" % file)
        except Exception as e:
            lg = open("Training_logs/DataTransformation_logs.txt", "a+")
            self.logger.log(lg,"Error occured during Data Transformation::%s"%e)
            lg.close()
            raise e
        lg_file.close()

