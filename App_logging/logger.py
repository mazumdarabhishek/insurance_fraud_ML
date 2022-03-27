"""This class Takes a file Object and a Message. The message is written into the sepecifed file with a 
        format. 
"""

from datetime import datetime
import os


class App_logger:
    """Description: This Class writes logs to file via file_object
        Written By: Abhishek Mazumdar
        Error: None
        Version: 1.0
        Revision: None
        
    """
    def __init__(self):
        pass
    
    def log(self, file_object, log_message):
        """Description: Logger logs messages into file via file_object
        

        Args:
            file_object (Object): file_object of file to be written into
            message (str): Message to be logged
        """
        
        
        
        now = datetime.now()
        self.time = now.strftime("%H:%M:%S")
        self.date = now.date()
        file_object.write(
            str(self.date) + "/" + str(self.time) + "\t\t" + log_message +"\n")
        


#ob = App_logger()
#print(os.getcwd())
#file = open("Training_logs/logg_trail.txt", 'a+')

#ob.log(file,"This is working")