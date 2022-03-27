from App_logging.logger import App_logger
from Data_Preprocessing import DataPreprocessing,DataClustering
from Data_Ingestion import Data_Loder
from Data_Preprocessing import DataClustering
from Best_Model_Finder import tunner
from file_operations import file_methods
import numpy as np
from sklearn.model_selection import train_test_split




class TrainModel:

    def __init__(self):
        self.logger = App_logger()
        self.file = open("Training_logs/TrainModel_logs.txt","a+")

    def trainingModel(self):

        self.logger.log(self.file,"TrainingModel method initiated !")

        try:

            data_getter = Data_Loder.dataGetter(self.file,self.logger)
            data = data_getter.getData()

            #preprocessing

            pre_processor = DataPreprocessing.Preprocessor(self.file,self.logger)
            data = pre_processor.remove_columns(data,columns=['policy_number','policy_bind_date','policy_state','insured_zip','incident_location','incident_date','incident_state','incident_city','insured_hobbies','auto_make','auto_model','auto_year','age','total_claim_amount'])

            data.replace('?',np.nan,inplace=True)
            is_null_present, cols_with_missing_values = pre_processor.is_null_present(data)
            if (is_null_present):
                data = pre_processor.impute_missing_values(data, cols_with_missing_values)

            data = pre_processor.encode_categorical_columns(data)
            #data.dropna()

            X, Y = pre_processor.separate_label_feature(data,label_column_name='fraud_reported')


            #clustering

            kmeans = DataClustering.KMeansClustering(self.file,self.logger)
            number_of_clusters = kmeans.elbow_plot(X)

            X = kmeans.create_clusters(X,number_of_clusters)

            X['Labels'] = Y

            list_of_clusters = X['Cluster'].unique()

            # getting best models for individual clusters

            for i in list_of_clusters:
                cluster_data = X[X['Cluster']==i]

                cluster_features = cluster_data.drop(columns = ['Cluster','Labels'])
                cluster_label = cluster_data['Labels']

                train_x,test_x,train_y,test_y = train_test_split(cluster_features,cluster_label,test_size=.10,random_state=353)

                train_x = pre_processor.scale_numerical_columns(train_x)
                test_x = pre_processor.scale_numerical_columns(test_x)


                model_finder = tunner.Model_Finder(self.file,self.logger)

                best_model_name,best_model = model_finder.getBestModel(train_x,train_y,test_x,test_y)

                file_op = file_methods.File_Operations(self.file,self.logger)

                save_model = file_op.saveModel(best_model,best_model_name+str(i))

            self.logger.log(self.file,"Training Completed Successfully, models are saved in Best_model_for_cluster")
            self.file.close()

        except Exception as e:

            self.file = open("Training_logs/TrainModel_logs.txt","a+")
            self.logger.log(self.file,"Error occured while training :: %s"%e)
            self.file.close()
            raise Exception()









