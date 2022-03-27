import shutil
import sqlite3
import os
import csv
from os import listdir
from App_logging.logger import App_logger


class DBOperations:

    def __init__(self):

        self.path = 'Training_Database/'
        self.goodPath = "Validated_Raw_Files_Training/Good_Raw"
        self.badPath = "Validated_Raw_Files_Training/Bad_Raw"
        self.logger = App_logger()


    def DBConnection(self,DBName):

        try:
            conn = sqlite3.connect(self.path+DBName+".db")

            file = open("Training_logs/DBConnection_logs.txt","a+")
            self.logger.log(file,"DB Connection Established")
            file.close()
        except ConnectionError :
            file = open("Training_logs/DBConnection_logs.txt", "a+")
            self.logger.log(file,"Error while connecting to DataBase ::%s"%ConnectionError)
            file.close()
            raise ConnectionError
        return conn


    def createTableinDB(self,DBName, column_names):

        try:
            conn = self.DBConnection(DBName)
            cursor = conn.cursor()
            cursor.execute("SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = 'Good_Raw_Data'")
            if cursor.fetchone()[0] == 1:
                conn.close()
                file = open("Training_logs/DBConnection_logs.txt", "a+")
                self.logger.log(file,"Table Successfully fetched (already avaialble in DataBase)")
                file.close()

            else:
                for key in column_names.keys():
                    type = column_names[key].lower()
                    try:

                        conn.execute(f'ALTER TABLE Good_Raw_Data ADD COLUMN {key} {type}')
                    except:
                        conn.execute(f'CREATE TABLE  Good_Raw_Data ({key} {type})')

                conn.close()

                file = open("Training_logs/DBConnection_logs.txt", "a+")
                self.logger.log(file,"Table Successfully Created ")
                self.logger.log(file,"Connection Closed")
                file.close()

        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DBName)
            file.close()
            raise e


    def insertGoodDataToTable(self,DBName):


        conn = self.DBConnection(DBName)
        goodFilePath = self.goodPath
        badFilePath = self.badPath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Training_Logs/insertGoodDataToTable.txt", 'a+')

        for file in onlyfiles:
            try:
                with open(goodFilePath + '/' + file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_)))
                                self.logger.log(log_file, " %s: File loaded successfully!!" % file)
                                conn.commit()
                            except Exception as e:
                                raise e

            except Exception as e:

                conn.rollback()
                self.logger.log(log_file, "Error while creating table: %s " % e)
                shutil.move(goodFilePath + '/' + file, badFilePath)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()
                conn.close()

        conn.close()
        log_file.close()

    def exportDataFromDBtoCSV(self,DBName):

        self.fileFromDb = 'Training_Data_to_csv/'
        self.fileName = 'InputFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
        try:
            conn = self.DBConnection(DBName)
            sqlSelect = "SELECT *  FROM Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sqlSelect)

            results = cursor.fetchall()
            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            # Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing.
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''), delimiter=',',
                                 lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and Training_Batch_Files to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" % e)
            log_file.close()





