from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import mean_squared_error
from sklearn.linear_model import LinearRegression


class ModelFinder:
    """
         This class shall  be used to find the model with the best accuracy and MSE score.
    """

    def __init__(self, file, logger):
        self.logger = logger
        self.file = file
        self.LR = LinearRegression()
        self.RF = RandomForestRegressor()

    def get_best_params_of_randomforest_regressor(self, train_x, train_y):
        """
             Method Name: get_best_params_of_randomforest_regressor
             Description: get the parameters for Random_Forest_Regressor Algorithm which give the best accuracy.Use Hyper Parameter Tuning.
             Output: The model with the best parameters
             On Failure: Raise Exception
        """

        self.logger.log(self.file,'Entered the get_best_params_of_randomforest_regressor method of the ModelFinder class')

        try:
            self.param_grid_RF = {
                "n_estimators": [10, 20, 30],
                "max_features": ["auto", "sqrt", "log2"],
                "min_samples_split": [2, 4, 8],
                "bootstrap": [True, False]
            }

            #Creating an object fro GridsearcCV
            self.grid_search = GridSearchCV(self.RF, self.param_grid_RF, verbose=3, cv=5)
            self.grid_search.fit(train_x, train_y)

            # extracting the best parameters
            self.n_estimators = self.grid_search.best_params_['n_estimators']
            self.max_features = self.grid_search.best_params_['max_features']
            self.min_samples_split = self.grid_search.best_params_['min_samples_split']
            self.bootstrap = self.grid_search.best_params_['bootstrap']

            #Creating a Randomforest Model using best parameters
            self.best_rf_model = RandomForestRegressor(n_estimators= self.n_estimators, max_features= self.max_features, min_samples_split= self.min_samples_split, bootstrap= self.bootstrap)
            self.best_rf_model.fit(train_x, train_y)
            self.logger.log(self.file,f"RandomForestRegressor's best params are : {self.grid_search.best_params_}, Exited the RandomForestReg method of the Model_Finder class")
            return self.best_rf_model

        except Exception as e:
            self.logger.log(self.file, 'Exception occured in RandomForestReg method of the Model_Finder class. Exception message: {e} ')
            self.logger.log(self.file, 'RandomForestReg Parameter tuning  failed. Exited the get_best_params_of_randomforest_regressor method of the ModelFinder class')
            raise Exception()



    def get_best_params_of_linear_regressor(self, train_x, train_y):
        """
               Method Name: get_best_params_of_linear_regressor
               Description: get the parameters for LinearReg Algorithm which give the best accuracy.Use Hyper Parameter Tuning.
               Output: The model with the best parameters
               On Failure: Raise Exception
        """
        self.logger.log(self.file,'Entered the get_best_params_of_linear_regressor method of the ModelFinder class')

        try:
            self.param_grid_LR = {
                'fit_intercept': [True, False],
                'normalize': [True, False],
                'copy_X': [True, False]
            }

            # Creating an object of the Grid Search class
            self.grid_search = GridSearchCV(self.LR, self.param_grid_LR, verbose=3, cv=5)

            # finding the best parameters
            self.grid_search.fit(train_x, train_y)

            # extracting the best parameters
            self.fit_intercept = self.grid_search.best_params_['fit_intercept']
            self.normalize = self.grid_search.best_params_['normalize']
            self.copy_X = self.grid_search.best_params_['copy_X']

            # creating a new LR model with the best parameters
            self.best_rf_model = LinearRegression(fit_intercept=self.fit_intercept, normalize=self.normalize,copy_X=self.copy_X)
            # training the mew model
            self.best_rf_model.fit(train_x, train_y)

            self.logger.log(self.file, f'LinearRegression best params: {self.grid_search.best_params_} Exited the get_best_params_for_linearReg method of the Model_Finder class')
            return self.best_rf_model

        except Exception as e:
            self.logger.log(self.file, f'Exception occured in get_best_params_for_linearReg method of the Model_Finder class. Exception message: {e}')
            self.logger.log(self.file, 'LinearReg Parameter tuning  failed. Exited the get_best_params_of_linear_regressor method of the ModelFinder class')
            raise Exception()



    def get_best_model(self, train_x, train_y, test_x, test_y):

        """
              Method Name: get_best_model
              Description: Find out the Model which has the best AUC score.
              Output: The best model name and the model object
              On Failure: Raise Exception
        """

        self.logger.log(self.file,'Entered the get_best_model method of the ModelFinder class')
        try:
            # creating Linear Regression with best params
            self.Linear_Reg = self.get_best_params_of_linear_regressor(train_x, train_y)
            self.prediction_LR = self.Linear_Reg.predict(test_x)
            self.error_LR = mean_squared_error(test_y, self.prediction_LR)

            # creating RandomForest Regressor with best params
            self.RF_Reg = self.get_best_params_of_randomforest_regressor(train_x, train_y)
            self.prediction_RF = self.RF_Reg.predict(test_x)
            self.error_RF = mean_squared_error(test_y, self.prediction_RF)

            # comparing the two models
            if (self.error_LR < self.error_RF):
                self.logger.log(self.file, f"The Best Model is 'LinearRegression', {self.Linear_Reg} , Error of LR is {self.error_LR} and RF is {self.error_RF}")
                return 'LinearRegression', self.Linear_Reg , f"Error of LR is {self.error_LR } and RF is {self.error_RF}"

            elif (self.error_LR == self.error_RF):
                self.logger.log(self.file, f"Both Model have same error So choose either among them as final model , Error of LR is {self.error_LR} and RF is {self.error_RF}")
                return 'both have same error'

            else:
                self.logger.log(self.file, f"The Best Model is 'RF Regressor', {self.RF_Reg} , Error of LR is {self.error_LR} and RF is {self.error_RF}")
                return 'RandomForest Regressor', self.RF_Reg, f"Error of LR is {self.error_LR } and RF is {self.error_RF}"

        except Exception as e:
            self.logger.log(self.file, f'Exception occured in get_best_model method of the ModelFinder class. Exception message: {e}')
            self.logger.log(self.file,  'Model Selection Failed. Exited the get_best_model method of the ModelFinder class')
            raise Exception()