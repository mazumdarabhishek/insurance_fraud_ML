from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score,accuracy_score

class Model_Finder:


    def __init__(self,file_object,logger_object):
        self.logger = logger_object
        self.file = file_object
        self.sv_clf = SVC()
        self.xgb_clf = XGBClassifier(objective="binary:logistic",n_jobs=-1)

    def getBestParamForSVM(self,train_x,train_y):


        self.logger.log(self.file,'getBestParamForSVC method initiated')
        try:
            self.param = {
                "kernel":['rbf','sigmoid'],
                "C":[0.1,0.5,1.0],
                "random_state": [0,100,200,300]
            }

            self.grid = GridSearchCV(estimator=self.sv_clf,param_grid=self.param,cv=5, verbose=3)
            self.grid.fit(train_x,train_y)

            #getting the best parameters from Grid Search
            self.kernel = self.grid.best_params_['kernel']
            self.C = self.grid.best_params_['C']
            self.random_state = self.grid.best_params_['random_state']

            #fitting with new model

            self.svc_CLF = SVC(C=self.C,kernel=self.kernel,random_state=self.random_state)
            self.svc_CLF.fit(train_x,train_y)
            self.logger.log(self.file,'Best paramters for SVC are: '+str(self.grid.best_params_)+ "Exiting from SVC")

            return self.svc_CLF
        except Exception as e:
            self.logger.log(self.file,'Error occured at getBestParamForSVM:: %s '%e)
            raise Exception()


    def getBestParamForXGBoost(self,train_x,train_y):

        self.logger.log(self.file,'getBestParamForXGBoost method initiated')

        try:
            self.param_xgb = {
                "n_estimators": [100,130, 150],
                "criterion": ['gini','entropy'],
                "max_depth": range(8,10,1)
            }

            self.grid = GridSearchCV(estimator=self.xgb_clf,param_grid=self.param_xgb,cv=5,verbose=3)

            self.grid.fit(train_x,train_y)

            self.n_estimators = self.grid.best_params_['n_estimators']
            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']

            self.xgb_CLF = XGBClassifier(criterion=self.criterion,n_estimators=self.n_estimators,max_depth=self.max_depth,n_jobs=-1)
            self.xgb_CLF.fit(train_x,train_y)
            self.logger.log(self.file,'Best paramters for XGBClassifier :'+str(self.grid.best_params_))
            return self.xgb_CLF
        except Exception as e:
            self.logger.log(self.file,'Error occured at getBestParamForXGBoost :: %s'%e)
            raise Exception()

    def getBestModel(self,train_x,train_y,test_x,test_y):

        self.logger.log(self.file,"getBestModel method executing")
        try:
            self.xgboost = self.getBestParamForXGBoost(train_x,train_y)
            self.xgboost_predictions = self.xgboost.predict(test_x)

            if len(test_y.unique()) == 1:
                self.xgboost_score = accuracy_score(test_y,self.xgboost_predictions)
                self.logger.log(self.file,'Accuracy Score for xgboost--> '+str(self.xgboost_score))
            else:
                self.xgboost_score = roc_auc_score(test_y,self.xgboost_predictions)
                self.logger.log(self.file,'ROC_AUC Score for xgboost--> '+str(self.xgboost_score))

            self.svc  =self.getBestParamForSVM(train_x,train_y)
            self.svc_predictions = self.svc.predict(test_x)

            if len(test_y.unique()) == 1:
                self.svc_score = accuracy_score(test_y,self.svc_predictions)
                self.logger.log(self.file,'Accuracy Score for SVC -->'+str(self.svc_score))
            else:
                self.svc_score = roc_auc_score(test_y,self.svc_predictions)
                self.logger.log(self.file,'ROC_AUC_SCORE for SVC -->'+str(self.svc_score))

            if self.svc_score > self.xgboost_score:
                return 'SVC',self.svc
            else:
                return 'XGBOOST', self.xgboost

        except Exception as e:
            self.logger.log(self.file,'Error occured at getBestModel :: %s'%e)
            raise Exception()









