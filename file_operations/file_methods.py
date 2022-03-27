import pickle
import os
import shutil

class File_Operations:
    """
        Description: This class is responsible for saving and loading models for predictions
    """
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.model_dir = 'model/'

    def saveModel(self,model,fileName):
        """
            Description: Saves the model with fileName in the models directory

        """
        self.logger_object.log(self.file_object,"Entred saveModel method of file_Operations class !")
        try:
            path = os.path.join(self.model_dir,fileName)
            if os.path.isdir(path):
                shutil.rmtree(self.model_dir)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path + '/'+fileName+'.sav','wb') as f:
                pickle.dump(model,f)

            self.logger_object.log(self.file_object,"Model Saved successfully::%s"%fileName)
            return 'success'
        except Exception as e:
            self.logger_object.log(self.file_object,"Model "+fileName+" could not be saved for the error:: %s"%e)


    def load_model(self, filename):
        """
                    Method Name: load_model
                    Description: load the model file to memory
                    Output: The Model file loaded in memory
                    On Failure: Raise Exception

                    Written By: iNeuron Intelligence
                    Version: 1.0
                    Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the load_model method of the File_Operation class')
        try:
            with open(self.model_dir + filename + '/' + filename + '.sav',
                      'rb') as f:
                self.logger_object.log(self.file_object,
                                       'Model File ' + filename + ' loaded. Exited the load_model method of the Model_Finder class')
                return pickle.load(f)
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in load_model method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Model File ' + filename + ' could not be saved. Exited the load_model method of the Model_Finder class')
            raise Exception()

    def find_correct_model_file(self, cluster_number):
        """
                            Method Name: find_correct_model_file
                            Description: Select the correct model based on cluster number
                            Output: The Model file
                            On Failure: Raise Exception

                            Written By: iNeuron Intelligence
                            Version: 1.0
                            Revisions: None
                """
        self.logger_object.log(self.file_object,
                               'Entered the find_correct_model_file method of the File_Operation class')
        try:
            self.cluster_number = cluster_number
            self.folder_name = self.model_dir
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str(self.cluster_number)) != -1):
                        self.model_name = self.file
                except:
                    continue
            self.model_name = self.model_name.split('.')[0]
            self.logger_object.log(self.file_object,
                                   'Exited the find_correct_model_file method of the Model_Finder class.')
            return self.model_name
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in find_correct_model_file method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Exited the find_correct_model_file method of the Model_Finder class with Failure')
            raise Exception()