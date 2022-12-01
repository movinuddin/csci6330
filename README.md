Batch Processing for Banking Systems

Team:
1. Movinuddin
2. Sanjana Gutta
3. Revanth Kumar

Things to install: Jupyter Notebook, we need to pip install pyspark.

CCFraudDetectionML.ipynb:
This file has the code for the creating a ML model used for predicting if a credit card transaction is fraud or not.

Dataset Link: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

Steps followed to create the model:

1. Initially we initialize the spark session.
2. Read all the data from the dataset into a pyspark dataframe.
3. Convert the pyspark dataframe to pandas dataframe for ease of use.
4. As the dataset is highly imbalanced with the number of fraud and non-fraud records, we try to reduce the non-fraud record according with the fraud records to get a good balance for creating an effective model.
5. After all the manipulations to the dataset are done, we convert the pandas dataframe back to the pyspark dataframe.
6. Next, we create the training and testing datasets with 80-20 ratio.
7. We are using RandomForestClassifier to build the model as this is a classification problem.
8. Once the model is created, we use the test data to test the accuracy of the model.
9. Save the model to a file 'credit_card_fraud_model.pkl' under models folder

For scheduling the jobs, we need to install airflow. Follow link guides how to install airflow on windows.
https://www.youtube.com/watch?v=SYOUbiGtGiU

All the dependencies should be installed for installing airflow. All the dependencies needed will be shown in the logs.

After airflow is installed in your system, you can see airflow folder. Inside the airflow create the following folders:
1. dags: inside this folder, paste the dag file ('model_pred_dag.py')
2. model: inside this folder, paste the model create before ('credit_card_fraud_model.pkl')
3. data: inside this folder, paste the credit card fraud detection dataset, which can be downloaded from kaggle.

After you setup all the required folders inside the airflow folder, open the terminal where your root is airflow folder. Follow the below steps:
1. airflow db init : this command initializes the database required for the airflow. Check for any errors and resolve accordingly.
2. airflow dags list: this command lists all the dag files picked up in the initialization. There will be a lot of pre-defined dag files. You must see 'model_pred_dag.py' dag in the list.
3. airflow scheduler: this command will get the scheduler up and running
4. In the new terminal, airflow webserver -p 8080: this command will run the server
5. Once all the servers are up and running with no errors, go to chrome and run localhost:8080
6. You can see the airflow UI interface when you run localhost:8080. You should be able to see the list of all the dags. There should be a dag named 'model_pred_dag'. Enable that dag with the toggle button beside it.
7. Once the dag is enabled, click on the active tab above. Now here you can see only the active dags. As we have enabled 'model_pred_dag' dag, we should see this dag in the active tab.
8. Now, in the active tab, click on 'model_pred_dag'.
9. All the information related to 'model_pred_dag' will be displayed. All the stats are displayed here.
10. Click on graph tab to see the dag graph. In the graph view you can see 3 tasks, start_task, fraud_predict_task, end_task, respectively.
11. Click on fraud_predit_task. A dialog opens and click on logs tab.
12. Once the logs are displayed, if the task is a success, we can see the output in the logs. The output will be '513 fraud transactions found'
13. 'model_pred_dag' dag is set to run every minute. For each dag run, we get the same output as we are using a static dataset to predict the fraud transactions.









