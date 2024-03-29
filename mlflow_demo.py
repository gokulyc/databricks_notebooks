# Databricks notebook source
# import mlflow.lightgbm
# mlflow.lightgbm.autolog()

# COMMAND ----------

mlflow.set_tracking_uri('databricks')

# COMMAND ----------

import mlflow
mlflow.log_metric("accuracy", 0.9)

# COMMAND ----------

mlflow.log_param("learning_rate", 0.001)

# COMMAND ----------

mlflow.end_run()

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import matplotlib.pyplot as plt
 
from numpy import savetxt
 
from sklearn.model_selection import train_test_split
from sklearn.datasets import load_diabetes
 
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error


# COMMAND ----------

db = load_diabetes()
X = db.data
y = db.target
X_train, X_test, y_train, y_test = train_test_split(X, y)

# COMMAND ----------

# Enable autolog()
# mlflow.sklearn.autolog() requires mlflow 1.11.0 or above.
mlflow.sklearn.autolog()
 
# With autolog() enabled, all model parameters, a model score, and the fitted model are automatically logged.  
with mlflow.start_run():
  
  # Set the model parameters. 
  n_estimators = 100
  max_depth = 6
  max_features = 3
  
  # Create and train model.
  rf = RandomForestRegressor(n_estimators = n_estimators, max_depth = max_depth, max_features = max_features)
  rf.fit(X_train, y_train)
  
  # Use the model to make predictions on the test dataset.
  predictions = rf.predict(X_test)

# COMMAND ----------

e1 = mlflow.search_experiments(order_by=["lastUpdateTime"])[0]

# COMMAND ----------

mlflow.search_experiments()

# COMMAND ----------

e1.artifact_location, e1.DEFAULT_EXPERIMENT_NAME, e1.experiment_id, e1.lifecycle_stage, e1.name, e1.tags

# COMMAND ----------

mlflow.list_experiments()

# COMMAND ----------

mlflow.list_run_infos("7e139a4a7853452eaec47cab92df8735")

# COMMAND ----------

mlflow_runs = mlflow.list_run_infos("7e139a4a7853452eaec47cab92df8735")

# COMMAND ----------

mlflow_runs[0]

# COMMAND ----------

mlflow.get_run("6674aa3907044eb2b622682a2fe8a36f")

# COMMAND ----------


