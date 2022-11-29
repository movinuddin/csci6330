#datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG
from airflow.models import Variable

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from joblib import Parallel, delayed
import joblib
import os


data_path = Variable.get("credit_card_data_path")
model_path = Variable.get("credit_card_model_path")

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

# initializing the default arguments
default_args = {
		'owner': 'CreditCard',
		'start_date': datetime(2022, 11, 28),
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
model_pred_dag = DAG('model_pred_dag',
		default_args=default_args,
		description='Credit Card Fraud Detection DAG',
		schedule_interval='* * * * *', 
		catchup=False,
		tags=['credit card, fraud']
)

# python callable function
def print_hello():
		return 'Hello World!'
        

def predict_fraud_transactions():
    # Read and store content of a CSV file 
    df = pd.read_csv(f"{CUR_DIR}/data/creditcard.csv")

    # Normalizing data
    scaler = StandardScaler()
    df["NormalizedAmount"] = scaler.fit_transform(df["Amount"].values.reshape(-1, 1))

    # Drop unwanted columns
    df.drop(["Amount", "Time"], inplace= True, axis= 1)

    # Fetching the independent features
    X = df.drop(["Class"], axis= 1)

    # Load the model from the file
    dt_from_joblib = joblib.load(f"{CUR_DIR}/model/credit_card_fraud_model.pkl")
      
    # Use the loaded model to make predictions
    pred = dt_from_joblib.predict(X)

    count = 0
    for i in range(len(pred)):
        if pred[i] == True:
            count+=1
            
    print(f"{count} fraud transactions found")
    

# Creating first task
start_task = DummyOperator(task_id='start_task', dag=model_pred_dag)

# Creating second task
fraud_predict_task = PythonOperator(task_id='fraud_predict_task', python_callable=predict_fraud_transactions, dag=model_pred_dag)

# Creating third task
end_task = DummyOperator(task_id='end_task', dag=model_pred_dag)

# Set the order of execution of tasks. 
start_task >> fraud_predict_task >> end_task

